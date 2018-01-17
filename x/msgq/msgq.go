/*
Copyright (C) 2017 Verizon. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package msgq

import (
	"container/list"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/msgq/config"
	"github.com/verizonlabs/northstar/pkg/servdisc"
	"github.com/verizonlabs/northstar/pkg/stats"
	"github.com/verizonlabs/northstar/pkg/zklib"
)

type PartitionerType int

const (
	RoundRobinPartitioner PartitionerType = iota //The messages are sent to the partitions in round robin fashion
	HashPartitioner                              //32bit hash sum % num_Partitions is used to compute the partition number to which the message must be sent to
	RandomPartitioner                            //A random generated value % num_partitions is used to compute the partition index
	ManualPartitioner                            //With manual partitioner, the application must use Producer.SendToPartition API. The message will be sent to the partition that the application specifies.
)

// struct to hold the message offsets
// The unordered map(OffsetMap) has the message offset (type: int64) as key
// and a pointer to a linked list element (type:*list.Element) as value
// The linked list(OffsetList) holds the message offsets in the order they were received
type offsetData struct {
	OffsetMap  map[int64]*list.Element
	OffsetList *list.List
	sync.Mutex
}

type MsgQProducer interface {
	Send(msg []byte) error
	SendMsg(msg *sarama.ProducerMessage) error
	SendKeyMsg(key, msg []byte) error
	SendToPartition(Partition int32, msg []byte) error
	ReceiveErrors() chan *ProducerEvent
	Close() error
}

type MsgQConsumer interface {
	Receive() chan *ConsumerEvent
	SetAckOffset(ofs int64) error
	GetPartitionNum() int32
	Close() error
}

type MessageQueue interface {
	NewProducer(prodConf *ProducerConfig) (MsgQProducer, error)
	NewProducerWithRetry(prodConf *ProducerConfig) (MsgQProducer, error)
	NewConsumer(consConf *ConsumerConfig) (MsgQConsumer, error)
	NewConsumerWithRetry(consConf *ConsumerConfig) (MsgQConsumer, error)
	Partitions(topicName string) (int, error)
	ReceiveTopicAvailMonEvt() chan *TopicAvailEvent
}

// MsgQ holds pointers to the kafka client and producer, consumer
type MsgQ struct {
	ServiceName      string
	Client           sarama.Client
	Zk               *zklib.ZK
	Producers        []*Producer
	Consumers        []Consumer
	KafkaBroker      string
	KafkaBrokerMutex sync.Mutex
	ZkBroker         string
	ZkBrokerMutex    sync.Mutex
	TopicAvailChan   chan *TopicAvailEvent
}

type Producer struct {
	SaramaProducer              sarama.AsyncProducer
	TopicName                   string
	Partitioner                 PartitionerType
	ReceiveChan                 chan *ProducerEvent
	NotifyError                 bool
	ZkCon                       *zklib.ZK
	NumPartitionsZkPath         string
	ExcludeListZkPath           string
	ExcludeListPartitions       []int
	ExcludeListPartitionChannel <-chan zk.Event
	PartitionCounters           []*stats.Counter // Counter that store actual value of partition of a topic
	NumMsgSentCounter           *stats.Counter   // Counter that store number of messages that are sent from application
	NumMsgErrCounter            *stats.Counter   // Counter that store number of messages that got error from application
	AvgLatency                  *stats.Set       // value that store average latency of message
	AvgBatchNwLatency           *stats.Set       // value that store average latency of network round trip transmission delay of batch
	NonceCounter                uint64
	Encryption                  bool
	partStatusLock              sync.Mutex // ← this mutex protects the status cache below
	PartitionSt                 []PartitionStatus
	msgqRef                     *MsgQ
}

type Consumer struct {
	SaramaConsumer         sarama.PartitionConsumer
	BaseConsumer           sarama.Consumer
	TopicName              string
	PartitionNum           int32
	OffsetData             offsetData
	CommitOffset           int64
	ReceiveChan            chan *ConsumerEvent
	ZkCon                  *zklib.ZK
	PartitionZkPath        string
	OffsetInfoZkPath       string
	LockZkPath             string
	ExcludeListZkPath      string
	NumMsgsReceivedCounter *stats.Counter // Counter that store actual value of partition of a topic
	NumMsgsCommitedCounter *stats.Counter // Counter that store actual number of messages that are ACKed/processed
	NumRecvErrCounter      *stats.Counter // Counter that store number of messages that got error
	ErrOffsetNotFound      *stats.Counter // Counter for offset not found in the offset List/Map
	OffsetKafka            *stats.Set
	OffsetZkp              *stats.Set
	NumZkCommitFailCounter *stats.Counter // Counter that store number of messages that got error while commiting ack to zookeeper
	Encryption             bool
	ReqForClose            int32
	msgqRef                *MsgQ
}

type ProducerConfig struct {
	TopicName   string
	Partitioner PartitionerType
	NotifyError bool
	Encryption  bool
}

type ConsumerConfig struct {
	TopicName  string
	Encryption bool
}

// Event that will be returned to the consumer
type ConsumerEvent struct {
	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
	Err        error
}

// Event that will be returned to the producer
type ProducerEvent struct {
	Msg *sarama.ProducerMessage
	Err error
}

// Event that will be returned to the producer
type TopicAvailEvent struct {
	TopicName  string
	EventType  AppEventT // Event type is UP or DOWN
	PartStatus []PartitionStatus
}

var (
	ErrAllPartitionUsed     = errors.New("msgq: All partitions used by all the msgq instances ")
	ErrNoConfigSpecfied     = errors.New("msgq: No message library configuration is provided")
	ErrNoMetadata           = errors.New("No Metadata")
	ErrInvalidPartitioner   = errors.New("Invalid partition handler")
	ErrInvalidPartition     = errors.New("Invalid value for a partition number")
	ErrExcludeListPartition = errors.New("Writing to a partition that is blacklisted")
	ErrMalformedMessage     = errors.New("Producer object not found in message")
	ErrNullKey              = errors.New("Null Key for a HashPartitioner")

	MaxBufferTime    = 100 * time.Millisecond
	MaxBufferedBytes = 10485760 //10MB

	//Zk path created by each consumer to indicate that the partition is in use
	//This path is in the form "/msgq/<service_name>/<topic>/consumer/partitions/in_use/<index>"
	ConsumerPartitionZkPath = "/msgq/%s/%s/consumer/partitions/inuse"

	//Zk path created by each consumer to indicate the offset until which the messages
	//have been processed. This is also referred as CommitOffset
	//This path is in the form "/msgq/<service_name>/<topic>/consumer/partitions/offsetinfo/<index>"
	ConsumerOffsetInfoZkPath = "/msgq/%s/%s/consumer/partitions/offsetinfo/%d"

	//Zk path for the lock used by consumers while getting a partition
	//This path is in the form "/msgq/<service_name>/<topic>/consumer/partitions/lock"
	ConsumerLockZkPath = "/msgq/%s/%s/consumer/partitions/lock"

	//Zk path where the children indicate the list of partitions which should
	//not be used by a consumer.
	//This path is in the form "/msgq/<service_name>/<topic>/consumer/partitions/blacklist"
	ConsumerPartitionsExcludeListZkPath = "/msgq/%s/%s/consumer/partitions/blacklist"

	//Zk path where the children indicate the list of patition numbers that
	//have been 'black listed' - i.e. these are the partitions to which the
	//producer will not write-to
	//This path is in the form "/msgq/<topic>/producer/partitions/blacklist"
	ProducerPartitionsExcludeListZkPath = "/msgq/%s/producer/partitions/blacklist"
)

//A global variable used to assist the test programs to disable
//the wait for Kafka and Zookeeper servers in the Init function.
var WaitForKafkaAndZkServers = config.WaitForKafkaAndZkServers
var statsObj *stats.Stats

func initStats(svcName string, disableStats bool) {
	if statsObj == nil {
		statsObj = stats.New(svcName)
		if disableStats {
			statsObj.Disable()
		}
	}
}

func getSaramaConfig(msgQ *MsgQ, cacertfiles []string, bSkipVerify bool) *sarama.Config {
	//Create config with default values
	saramaConfig := sarama.NewConfig()

	// Default config information for producer
	saramaConfig.Net.DialTimeout = config.DialTimeout
	saramaConfig.Net.ReadTimeout = config.NetReadTimeout
	saramaConfig.Net.WriteTimeout = config.NetWriteTimeout
	saramaConfig.ChannelBufferSize = config.ChannelBufferSize
	saramaConfig.Producer.Retry.Max = config.ProducerRetryMax
	saramaConfig.Producer.Flush.Frequency = config.MaxBufferTime
	saramaConfig.Producer.Flush.Bytes = config.MaxBufferedBytes
	saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	saramaConfig.Producer.Partitioner = newMsgQPartitioner
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Return.Successes = true

	//Background refresh frequency. Sarama default is 10mins.
	//Reducing it to 1 mins
	saramaConfig.Metadata.RefreshFrequency = 1 * time.Minute
	// Set the maximum size of the message that can be fetch from kafka cluster
	saramaConfig.Consumer.Fetch.Default = int32(config.MaxMsgFetchSize)

	//Provide a root CA pool for server certificate validation
	if len(cacertfiles) > 0 {
		bTLSEnabled := false
		certs := x509.NewCertPool()
		for _, cacf := range cacertfiles {
			pemdata, err := ioutil.ReadFile(cacf)
			if err != nil {
				mlog.Error("Error reading CA cert file: %s, err = %v", cacf, err)
				continue
			}
			if certs.AppendCertsFromPEM(pemdata) {
				bTLSEnabled = true
			} else {
				mlog.Error("Could not append CA cert from file: %s", cacf)
			}
		}
		if bTLSEnabled {
			saramaConfig.Net.TLS.Enable = true
			saramaConfig.Net.TLS.Config = &tls.Config{RootCAs: certs, InsecureSkipVerify: bSkipVerify}
		}
	}
	return saramaConfig
}

func NewMsgQWithStatsOptional(svcName string, kafkaBrokers []string, zkBrokers []string, disableStats bool, cacertfiles []string, bSkipVerify bool) (MessageQueue, error) {
	return newMsgQ(svcName, kafkaBrokers, zkBrokers, disableStats, cacertfiles, bSkipVerify)
}

func NewMsgQ(svcName string, kafkaBrokers []string, zkBrokers []string) (MessageQueue, error) {
	return newMsgQ(svcName, kafkaBrokers, zkBrokers, false, []string{}, true)
}

func newMsgQ(svcName string, kafkaBrokers []string, zkBrokers []string, disableStats bool, cacertfiles []string, bSkipVerify bool) (MessageQueue, error) {
	var err error
	msgQ := &MsgQ{
		ServiceName: svcName,
	}

	//Init stats Obj
	initStats(svcName, disableStats)

	if len(kafkaBrokers) == 0 {
		kafkaBrokers, err = servdisc.GetHostPortStrings(servdisc.KAFKA_SERVICE)
		if err != nil {
			mlog.Error("Could not get kafka broker list from service discovery. err %v", err)
			return nil, err
		} else {
			mlog.Info("Received kafka broker list %v from service discovery", kafkaBrokers)
		}
	} else {
		mlog.Info("Using the kafka broker list %v from parameters. Not using service discovery", kafkaBrokers)
	}

	if len(zkBrokers) == 0 {
		zkBrokers, err = servdisc.GetHostPortStrings(servdisc.ZOOKEEPER_SERVICE)
		if err != nil {
			mlog.Error("Could not get zookeeper nodes list from service discovery. err %v", err)
			return nil, err
		} else {
			mlog.Info("Received zookeeper nodes list %v from service discovery", zkBrokers)
		}
	} else {
		mlog.Info("Using the Zookeeper node list %v from parameters. Not using service discovery", zkBrokers)
	}

	// Config used with sarama
	saramaConfig := getSaramaConfig(msgQ, cacertfiles, bSkipVerify)

	numRetries := 0
	retryInterval := config.RetryStartInterval
	for {
		// Setup a msgq client
		msgQ.Client, err = sarama.NewClient(kafkaBrokers, saramaConfig)
		if err != sarama.ErrOutOfBrokers {
			break
		} else {
			if !WaitForKafkaAndZkServers {
				break
			}
			mlog.Info("Unable to connect to kafka brokers %v, waiting for one of the brokers to be reachable", kafkaBrokers, err)
			waitRetryBackoff(&numRetries, &retryInterval, "Error while connecting to Kafka...retrying", err)
		}
	}

	if err != nil {
		mlog.Error("Failed to create new sarama client. err %v", err)
		return nil, err
	}

	msgQ.Zk, err = zklib.NewZK(zkBrokers, 6*time.Second)
	if err != nil {
		return nil, err
	}

	if IsTopicAvailMonFeatureEnabled() {
		msgQ.TopicAvailChan = make(chan *TopicAvailEvent, 64)
		availMon := &kafkaAvailMonitor{
			msgQRef: msgQ,
		}

		go availMon.KafkaAvailAnalyzer()
		mlog.Event("Started Kafka Availabilty monitor sucessfully")
	}

	mlog.Event("Connected to Zookeeper and Kafka cluster successfully")

	return msgQ, nil
}

// NewProducerWithRetry creates a msgq producer.
// In case of failures, it performs infinite
// retries with an exponential back-off mechanism for the retry interval
// till the operation succeeds
func (msgQ *MsgQ) NewProducerWithRetry(prodConf *ProducerConfig) (MsgQProducer, error) {
	mlog.Debug("NewProducerWithRetry")
	var retryInterval = config.RetryStartInterval
	var numRetries = 0

	for {
		prod, err := msgQ.NewProducer(prodConf)
		if err != nil {
			errMsg := "Failed to create Msgq Producer. Retrying again..."
			waitRetryBackoff(&numRetries, &retryInterval, errMsg, err)
		} else {
			return prod, nil
		}
	}
}

func (msgQ *MsgQ) NewProducer(prodConf *ProducerConfig) (MsgQProducer, error) {
	var err error
	prod := &Producer{
		TopicName:   prodConf.TopicName,
		Partitioner: prodConf.Partitioner,
		ZkCon:       msgQ.Zk,
		NotifyError: prodConf.NotifyError,
		Encryption:  prodConf.Encryption,
	}

	mlog.Info("Creating producer for topic %v", prodConf.TopicName)

	prod.SaramaProducer, err = sarama.NewAsyncProducerFromClient(msgQ.Client)

	if err != nil {
		return nil, err
	}

	//Create Zookeeper paths used by this producer
	prod.ExcludeListZkPath = fmt.Sprintf(ProducerPartitionsExcludeListZkPath, prodConf.TopicName)

	var excludeList []string
	excludeList, prod.ExcludeListPartitionChannel, err = msgQ.Zk.GetChildrenWithWatchNCreate(prod.ExcludeListZkPath)
	prod.ExcludeListPartitions = make([]int, len(excludeList))
	for i := range excludeList {
		val, err := strconv.Atoi(excludeList[i])
		if err != nil {
			mlog.Error("Failed to convert partition number to integer %v", err)
			return nil, err
		}

		prod.ExcludeListPartitions[i] = val
	}

	mlog.Info("Blacklist Partitions: %v", prod.ExcludeListPartitions)

	if prod.NotifyError {
		prod.ReceiveChan = make(chan *ProducerEvent, 1024)
	}

	//Get the total number of partitions from Kafka
	kafkaPartitions, err := msgQ.Client.Partitions(prod.TopicName)
	if err != nil || (0 == len(kafkaPartitions)) {
		mlog.Error("Failed to get partitions:%v from Kafka. %s", kafkaPartitions, err)
		return nil, err
	}

	prod.PartitionCounters = make([]*stats.Counter, len(kafkaPartitions))
	prod.PartitionSt = make([]PartitionStatus, len(kafkaPartitions))

	mlog.Debug("List of partitions from Kafka on Topic %v: %v", prodConf.TopicName, kafkaPartitions)
	for i := range kafkaPartitions {
		mlog.Debug("adding partition number to stats counter %v", i)
		prod.PartitionCounters[i] = statsObj.NewCounter("NumWrites_" + prod.TopicName + "_" + strconv.Itoa(i))
		/* Inittially mark all the topic status up */
		prod.PartitionSt[i] = UP
	}
	// Initialize stats counter for number of messages send/err
	prod.NumMsgSentCounter = statsObj.NewCounter(fmt.Sprintf("NumMsgSent_%s", prodConf.TopicName))
	prod.NumMsgErrCounter = statsObj.NewCounter(fmt.Sprintf("NumErrs_%s", prodConf.TopicName))
	prod.AvgLatency = statsObj.NewSet(fmt.Sprintf("AvgLatency_%s", prodConf.TopicName))
	prod.AvgBatchNwLatency = statsObj.NewSet(fmt.Sprintf("AvgBatchNwLatency_%s", prodConf.TopicName))
	/* Give sets some default value to avoid problem of null values if no message is sent */
	prod.AvgLatency.Set(0)
	prod.AvgBatchNwLatency.Set(0)

	// Link Producer with the message Q and vice versa
	msgQ.Producers = append(msgQ.Producers, prod)
	prod.msgqRef = msgQ

	go prod.ZkPathMonitorRoutine()
	go prod.WatchForErrors()
	go prod.WatchForSuccess()

	mlog.Debug("msgQ producer initialization done")
	return prod, nil
}

func (msgQ *MsgQ) acquireZKLock(path string) (*zk.Lock, error) {
	acls := zk.WorldACL(zk.PermAll)

	lock := zk.NewLock(msgQ.Zk.Conn, path, acls)

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		err := lock.Lock()
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error acquiring zookeeper lock", err)
			} else {
				mlog.Error("Zookeeper Lock() returned with error %v", err)
				return nil, err
			}
		} else {
			return lock, err
		}
	}
}

func (msgQ *MsgQ) releaseZKLock(lock *zk.Lock) {

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		err := lock.Unlock()
		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error releasing zookeeper lock", err)
			} else {
				mlog.Error("Zookeeper Unlock() returned with error %v", err)
				return
			}
		} else {
			return
		}
	}
}

func (msgQ *MsgQ) getPartitionForTheConsumer(cons *Consumer) (int32, error) {

	lock, err := msgQ.acquireZKLock(cons.LockZkPath)
	if err != nil {
		mlog.Error("Failed to acquire zookeeper lock for %s", cons.LockZkPath)
		return -1, err
	}
	defer msgQ.releaseZKLock(lock)

	//Get the list of partitions from Kafka
	kafkaPartitions, err := msgQ.Client.Partitions(cons.TopicName)
	if err != nil {
		mlog.Error("Failed to get partitions:%v from Kafka. %s", kafkaPartitions, err)
		return -1, err
	} else if 0 == len(kafkaPartitions) {
		// In the starting when cluster is immature, sometime kafkaPartitions = 0 is recieved with err = nil that cause crash in message queue client
		err = sarama.ErrUnknownTopicOrPartition
		mlog.Error("Failed to get partitions:%v from Kafka. %s", kafkaPartitions, err)
		return -1, err
	}

	//Get blacklist partition list
	ExcludeListPartitions, err := msgQ.Zk.GetChildrenWithCreate(cons.ExcludeListZkPath)

	choosen := int32(-1)

	//Get the list of partitions from Zookeeper
	zkPartitions, _, err := msgQ.Zk.Children(cons.PartitionZkPath)
	if err == zklib.ErrNoNode {
		choosen = kafkaPartitions[0]
	} else if err != nil {
		mlog.Error("Failed to get partitions from Zookeeper. %s", err)
		return -1, err
	} else {

		var found bool
		//Look for an element in kafkaPartitions slice
		//but not in zkPartitions slice
		for i := range kafkaPartitions {
			//Check if the kafka partition is found in any zk partition
			found = false
			for j := range zkPartitions {
				val, err := strconv.Atoi(zkPartitions[j])
				if err != nil {
					mlog.Error("Failed to convert partition value to int %s", zkPartitions[j])
					return -1, err
				}
				if int32(val) == kafkaPartitions[i] {
					found = true
					break
				}
			}

			if found == false {
				//Check if its part of blacklist partitions
				for k := range ExcludeListPartitions {
					intVal, err := strconv.Atoi(ExcludeListPartitions[k])
					if err != nil {
						mlog.Error("Failed to convert blacklist partition value %s to int %v", ExcludeListPartitions[k], err)
						return -1, err
					}

					if kafkaPartitions[i] == int32(intVal) {
						mlog.Error("Partition %d is blacklisted. Trying another partition", intVal)
						found = true
						break
					}
				}
				if !found {
					choosen = kafkaPartitions[i]
					break
				}
			}
		}
	}

	if choosen < 0 {
		return -1, ErrAllPartitionUsed
	}

	//Create the zookeeper node for the choosen path
	zNode := fmt.Sprintf("%s/%d", cons.PartitionZkPath, choosen)
	err = msgQ.Zk.Create(zNode, []byte(""), true)
	if err != nil {
		mlog.Error("Failed to create Zookeeper path for choosen partition %s, %v", zNode, err)
		return -1, err
	}

	return choosen, nil
}

// NewConsumerWithRetry creates a msgq consumer.
// In case of failures, it performs infinite
// retries with an exponential back-off mechanism for the retry interval
// till the operation succeeds
func (msgQ *MsgQ) NewConsumerWithRetry(consConf *ConsumerConfig) (MsgQConsumer, error) {
	mlog.Debug("NewConsumerWithRetry")
	var retryInterval = config.RetryStartInterval
	var numRetries = 0

	for {
		cons, err := msgQ.NewConsumer(consConf)
		if err != nil {
			errMsg := "Failed to create Msgq Consumer. Retrying again..."
			waitRetryBackoff(&numRetries, &retryInterval, errMsg, err)
		} else {
			return cons, nil
		}
	}
}

func (msgQ *MsgQ) NewConsumer(consConf *ConsumerConfig) (MsgQConsumer, error) {
	var err error
	cons := &Consumer{
		TopicName:  consConf.TopicName,
		ZkCon:      msgQ.Zk,
		Encryption: consConf.Encryption,
	}

	cons.LockZkPath = fmt.Sprintf(ConsumerLockZkPath, msgQ.ServiceName, consConf.TopicName)
	cons.ExcludeListZkPath = fmt.Sprintf(ConsumerPartitionsExcludeListZkPath, msgQ.ServiceName, consConf.TopicName)
	cons.PartitionZkPath = fmt.Sprintf(ConsumerPartitionZkPath, msgQ.ServiceName, consConf.TopicName)

	//Wait to get an available partition
	for {
		cons.PartitionNum, err = msgQ.getPartitionForTheConsumer(cons)
		if err == nil {
			break
		}

		if err == ErrAllPartitionUsed {
			mlog.Info("All partitions are in use. Waiting for a partition to become available")
			time.Sleep(5 * time.Second)
		} else {
			return nil, err
		}
	}

	mlog.Info("Using Partition %d for topic %v", cons.PartitionNum, consConf.TopicName)

	cons.OffsetInfoZkPath = fmt.Sprintf(ConsumerOffsetInfoZkPath, msgQ.ServiceName, consConf.TopicName, cons.PartitionNum)

	atomic.StoreInt64(&cons.CommitOffset, int64(-1))
	// get the Ack offset value from the partition and start from there
	OffsetValue, err := cons.GetOffset()
	if err != nil && zklib.ErrNoNode != err {
		mlog.Error("Failed to get the offset for partition %v:%d - err %v", consConf.TopicName, cons.PartitionNum, err)
		return nil, err
	}

	if zklib.ErrNoNode != err {
		// This means consumer of this service group has already started to consume from this partition
		// Now start from where it was left last time
		atomic.StoreInt64(&cons.CommitOffset, OffsetValue)
		OffsetValue = OffsetValue + 1
		mlog.Info("Topic:partition-%s:%d and consumed offset:%d so far", consConf.TopicName, cons.PartitionNum, cons.CommitOffset)
	} else {
		// This means no consumer of this service group is yet started to consume from this partition
		// Now start from fresh
		mlog.Info("Topic:partition-%s:%d consumer starting first time", consConf.TopicName, cons.PartitionNum)
	}

	// initialize the offset management struct
	offsetData := offsetData{
		OffsetList: list.New(),
		OffsetMap:  make(map[int64]*list.Element),
	}
	cons.OffsetData = offsetData

	cons.BaseConsumer, err = sarama.NewConsumerFromClient(msgQ.Client)

	outOfRangeOccured := false
	numRetries := 0
	retryInterval := config.RetryStartInterval
	for {
		cons.SaramaConsumer, err = cons.BaseConsumer.ConsumePartition(consConf.TopicName,
			cons.PartitionNum, OffsetValue)
		if err == sarama.ErrOffsetOutOfRange {
			/* The offset we obtained from zookeeper is invalid. We will try with oldest known offset */
			mlog.Info("Offsetvalue value %d is OutOfRange for %v:%d, Starting consumer using oldest offset", OffsetValue, consConf.TopicName, cons.PartitionNum)

			OffsetValue = sarama.OffsetOldest
			outOfRangeOccured = true
		} else if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error ConsumePartition in Kafka", err)
			} else {
				mlog.Error("Failed to create consumer for %v:%d. Err %v", consConf.TopicName, cons.PartitionNum, err)

				zNode := fmt.Sprintf("%s/%d", cons.PartitionZkPath, cons.PartitionNum)
				err1 := msgQ.Zk.Delete(zNode)
				if err1 != nil {
					mlog.Error("Failed to delete Zookeeper path for choosen partition %s, %v", zNode, err1)
					return nil, err1
				}
				return nil, err
			}
		} else {
			break
		}
	}
	if true == outOfRangeOccured {
		//Get the chosen offset and store
		choosenOffset := cons.SaramaConsumer.Offset()
		atomic.StoreInt64(&cons.CommitOffset, choosenOffset)
	}

	mlog.Info("Starting consumer on %s:%d from offset %d", consConf.TopicName, cons.PartitionNum, OffsetValue)
	cons.NumMsgsReceivedCounter = statsObj.NewCounter("NumRecvd_" + cons.TopicName + "_" + strconv.Itoa(int(cons.PartitionNum)))
	cons.NumMsgsCommitedCounter = statsObj.NewCounter("NumCommitted_" + cons.TopicName + "_" + strconv.Itoa(int(cons.PartitionNum)))
	cons.NumRecvErrCounter = statsObj.NewCounter("NumRecvErr_" + cons.TopicName + "_" + strconv.Itoa(int(cons.PartitionNum)))
	cons.NumZkCommitFailCounter = statsObj.NewCounter("NumZkWriteFail_" + cons.TopicName + "_" + strconv.Itoa(int(cons.PartitionNum)))
	cons.ErrOffsetNotFound = statsObj.NewCounter("ErrOffsetNotFound_" + cons.TopicName + "_" + strconv.Itoa(int(cons.PartitionNum)))
	cons.OffsetKafka = statsObj.NewSet("OffsetKafka_" + cons.TopicName + "_" + strconv.Itoa(int(cons.PartitionNum)))
	cons.OffsetZkp = statsObj.NewSet("OffsetZkp_" + cons.TopicName + "_" + strconv.Itoa(int(cons.PartitionNum)))
	cons.OffsetZkp.Set(cons.CommitOffset)
	cons.OffsetKafka.Set(cons.SaramaConsumer.HighWaterMarkOffset())

	cons.ReceiveChan = make(chan *ConsumerEvent, 1024)
	cons.msgqRef = msgQ

	go cons.OffsetCommitRoutine()
	go cons.ReceiveRoutine()

	go cons.UpdateOffsetStats(config.OffsetUpdateInterval)
	return cons, nil
}

func (msgQ *MsgQ) Shutdown() error {
	mlog.Info("Mesage library.Shutdown")
	//Cleanup

	return nil
}

func (msgQ *MsgQ) ReceiveTopicAvailMonEvt() chan *TopicAvailEvent {
	return msgQ.TopicAvailChan
}

// This function check if it connection related error
func isConnError(err error) bool {
	switch err {
	case sarama.ErrOutOfBrokers, zk.ErrNoServer:
		return true
	default:
		return false
	}
}

// This function provide retry mechanism compliance (do waittime doubling until retry max backoff)
func waitRetryBackoff(numRetries *int, retryInterval *int, msg string, err error) {
	time.Sleep(time.Duration(*retryInterval) * time.Second)

	if *numRetries >= config.RetryMaxBeforeAlarm {
		mlog.Alarm("%s:%v", msg, err)
	} else {
		mlog.Error("%s:%v", msg, err)
	}

	if 2**retryInterval <= config.RetryMaxBackoff {
		*retryInterval = 2 * *retryInterval
	}

	*numRetries++
}

func IsTopicAvailMonFeatureEnabled() bool {
	return config.AvailMonFeature
}

func (msgQ *MsgQ) Partitions(topicName string) (int, error) {

	var numPartitions int
	var err error

	numRetries := 0
	retryInterval := config.RetryStartInterval

	for {
		partitions, err := msgQ.Client.Partitions(topicName)

		if err != nil {
			if (numRetries < config.NumRetries) && isConnError(err) {
				waitRetryBackoff(&numRetries, &retryInterval, "Error fetching Number of Partitions", err)
			} else {
				mlog.Error("Failed to fetch number of partitiond in topic: %s with Err: %v", topicName, err)
				numPartitions = len(partitions)
				break
			}
		} else {
			numPartitions = len(partitions)
			mlog.Debug("Number of parititions in the topic: %s is: %d", topicName, numPartitions)
			break
		}
	}

	return numPartitions, err
}
