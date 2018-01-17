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

package servdisc

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/verizonlabs/northstar/pkg/mlog"
)

type ServiceType int

const (
	UT_SERVICE ServiceType = iota
	EPHI_SERVICE
	UTSCHED_SERVICE
	UTBATTERY_SERVICE
	LOGCONSUMER_SERVICE
	TOKEN_SERVICE
	KAFKA_SERVICE
	CASSANDRA_SERVICE
	REDIS_SERVICE
	ZOOKEEPER_SERVICE
	MIMIC_SERVICE
	MMESIM_SERVICE
	INVENTORY_DATA_SERVICE
	DATA_SERVICE
	ACCOUNT_DATA_SERVICE

	NOTIFICATION_KAFKA_SERVICE
	NOTIFICATION_USER_SERVICE
	NOTIFICATION_DATA_SERVICE
	OTT_SERVICE
	AUTH_USER_SERVICE
	INFLUXDB_SERVICE_MGMT
	INFLUXDB_SERVICE_HTTP
	USERAPPS_SERVICE
	AUTH_USER_SERVICE_CACHE
	CREDENTIALS_SERVICE

	KAFKA_REST_SERVICE
	POSITION_SERVICE
	NOTIF_REST_SERVICE
	UTPROVISION_SERVICE
	UMDEVSAPI_SERVICE
	UM_SERVICE
	EXTERNAL_SERVICE
	PROVIDERAPI_SERVICE
	RULEENGINE_SERVICE
	KZADMINREST_SERVICE
	AGGREGATOR_OUT_SERVICE
	AGGREGATOR_ERR_SERVICE
	DMTS_SFTP_SERVICE
	GEOPLAN_SERVICE
	SMS_SERVICE
	POLARIS_SERVICE
	HUM_SERVICE
	HUMSIM_SERVICE
	GWTSHUM_SERVICE
	WFLS_SERVICE
	UTKILL_SERVICE
	GWOTT_SERVICE
	OTTDATA_SERVICE
	OTTPROVIDER_SERVICE
	OTTSIM_SERVICE
	SMSPROVIDER_SERVICE
	BSAA_SERVICE
	SMSBASIC_SERVICE
	LRSERVER_SERVICE
	SMSCONTROL_SERVICE
)


var ServiceNameMap = map[ServiceType]string{
	UT_SERVICE:          "Utag",
	EPHI_SERVICE:        "Ephemeris",
	UTBATTERY_SERVICE:   "UTBattery",
	TOKEN_SERVICE:       "Token",
	AUTH_USER_SERVICE:   "AuthUser",
	USERAPPS_SERVICE:    "UserApps",
	POSITION_SERVICE:    "UTPosition",
	NOTIF_REST_SERVICE:  "NotifRest",
	UTPROVISION_SERVICE: "UTProvision",
	KAFKA_REST_SERVICE:  "KafkaRest",
	UMDEVSAPI_SERVICE:   "UMDevelopersApi",
	UM_SERVICE:          "Umodem",
	EXTERNAL_SERVICE:    "External",
	PROVIDERAPI_SERVICE: "ProviderApi",
	OTTPROVIDER_SERVICE: "OttProvider",
	RULEENGINE_SERVICE:  "RuleEngine",
	KZADMINREST_SERVICE: "KzAdminRest",
	DATA_SERVICE:        "Data",
	OTTDATA_SERVICE:     "OttData",
	DMTS_SFTP_SERVICE:   "DmtsSftp",
	GEOPLAN_SERVICE:     "GeoPlan",
	SMS_SERVICE:         "Sms",
	POLARIS_SERVICE:     "Polaris",
	HUM_SERVICE:         "Hum",
	HUMSIM_SERVICE:      "HumSim",
	GWTSHUM_SERVICE:     "GwtsHum",
	WFLS_SERVICE:        "Wfls",
	OTT_SERVICE:         "Ott",
	UTKILL_SERVICE:      "UTKill",
	MIMIC_SERVICE:       "Mimic",
	GWOTT_SERVICE:       "GwOtt",
	OTTSIM_SERVICE:      "OttSim",
	SMSPROVIDER_SERVICE: "SmsProvider",
	BSAA_SERVICE:        "Bsaa",
	SMSBASIC_SERVICE:    "SmsBasic",
	LRSERVER_SERVICE:    "LrServer",
	SMSCONTROL_SERVICE:  "SmsControl",
}

var ServicePortMap = map[ServiceType]uint16{
	AUTH_USER_SERVICE:   11013,
	USERAPPS_SERVICE:    11016,
	UTPROVISION_SERVICE: 11017,
	KAFKA_REST_SERVICE:  11018,
	UMDEVSAPI_SERVICE:   11019,
	NOTIF_REST_SERVICE:  11023,
}

//Specify environment variables specified in marathon json for Dakota services
var ServiceEnvMap = map[ServiceType]string{
	KAFKA_SERVICE:           "KAFKA_BROKERS_HOST_PORT",
	ZOOKEEPER_SERVICE:       "ZOOKEEPER_HOST_PORT",
	REDIS_SERVICE:           "REDIS_HOST_PORT",
	INFLUXDB_SERVICE_HTTP:   "INFLUXDB_SERVICE_HTTP_HOST_PORT",
	CASSANDRA_SERVICE:       "CASSANDRA_HOST_PORT",
	DATA_SERVICE:            "DATA_HOST_PORT",
	OTTDATA_SERVICE:         "OTTDATA_HOST_PORT",
	INVENTORY_DATA_SERVICE:  "INVENTORY_HOST_PORT",
	ACCOUNT_DATA_SERVICE:    "ACCOUNT_DATA_HOST_PORT",
	USERAPPS_SERVICE:        "USERAPPS_HOST_PORT",
	AUTH_USER_SERVICE:       "AUTH_USER_HOST_PORT",
	AUTH_USER_SERVICE_CACHE: "AUTH_USER_CACHE_HOST_PORT",
	CREDENTIALS_SERVICE:     "CREDENTIALS_HOST_PORT",
	KAFKA_REST_SERVICE:      "KAFKA_REST_HOST_PORT",
	UTPROVISION_SERVICE:     "UTPROVISION_HOST_PORT",
	PROVIDERAPI_SERVICE:     "PROVIDERAPI_HOST_PORT",
	OTTPROVIDER_SERVICE:     "OTTPROVIDER_HOST_PORT",
	UMDEVSAPI_SERVICE:       "UMDEVSAPI_HOST_PORT",
	AGGREGATOR_OUT_SERVICE:  "AGGREGATOR_OHOST_PORT",
	AGGREGATOR_ERR_SERVICE:  "AGGREGATOR_EHOST_PORT",
	GEOPLAN_SERVICE:         "GEOPLAN_HOST_PORT",
	DMTS_SFTP_SERVICE:       "DMTS_SFTP_HOST_PORT",
	POLARIS_SERVICE:         "POLARIS_HOST_PORT",
	GWTSHUM_SERVICE:         "GWTSHUM_HOST_PORT",
	WFLS_SERVICE:            "WFLS_HOST_PORT",
	MIMIC_SERVICE:           "MIMIC_HOST_PORT",
	GWOTT_SERVICE:           "GWOTT_HOST_PORT",
	OTTSIM_SERVICE:          "OTTSIM_HOST_PORT",
	SMSPROVIDER_SERVICE:     "SMSPROVIDER_HOST_PORT",
	BSAA_SERVICE:            "BSAA_HOST_PORT",
	LRSERVER_SERVICE:        "LRSERVER_HOST_PORT",
}

//Container Ports specified in Service for Dakota CONTAINERs.
const (
	INVENTORY_DATA_CONTAINER_PORT = "8788"
	ACCOUNT_DATA_CONTAINER_PORT   = "8788"
	DATA_CONTAINER_PORT           = "8788"
)

//Container Ports specified in Service for Dakota CONTAINERs.
var host = os.Getenv("HOST")

/*
ENV variables used by dakota
For use by self only
---------------------
ADDRESS (cassandra)                                               : host
SEEDS (cassandra)                                                 : host
ADVERTISED_HOSTNAME (kafka)                                       : host
ADVERTISED_PORT (kafka)                                           : port
BROKER_ID (kafka)                                                 : unique id (integer)
DEVICE_DEST (ephemeris,token)                                     : string

For use by clients
-------------------
CASS_HOST (user,device)                                           : host
CASS_PORT (user,device)                                           : port
DS_ACCOUNT_HOST (utag)                                            : host
DS_ACCOUNT_PORT (utag)                                            : port
DS_DEVICE_HOST (ephemeris,utag)                                   : host
DS_DEVICE_PORT (ephemeris,utag)                                   : port
KAFKA_BROKERS (mimic, ephemeris, utag, utsched, mmesim, token)    : comma separated list of host : port
ZOOKEEPER (kafka, mimic, ephemeris, utag, utsched, mmesim, token) : comma separated list of host : port
REDIS_HOST (token)                                                : comma separated list of host : port
*/

//Returns array of hostport strings of format "host:port" to the client
func GetHostPortStrings(service ServiceType) (hostportInfo []string, err error) {
	svcEnv, svcExists := ServiceEnvMap[service]
	if svcExists {
		return GetHostPortInfo(service, svcEnv)
	} else {
		return nil, fmt.Errorf("No environment vars expected for service:%v\n", service)
	}
}

func replaceHost(p []string) (hp []string) {
	hostportInfo := p
	if p != nil {
		for i, s := range p {
			if s != "" {
				hostport := strings.Split(s, ":")
				if hostport[0] == "" {
					port := hostport[1]
					hostportInfo[i] = host + ":" + port
				}
			}
		}
	}
	return hostportInfo
}

func GetHostPortInfo(svc ServiceType, svcEnv string) ([]string, error) {
	hostportInfo := []string{""}
	svcEnvValue, svcEnvExists := syscall.Getenv(svcEnv)
	if !svcEnvExists {
		mlog.Error("Service environment variable not set for %v", svc)
		return hostportInfo, fmt.Errorf("Service environment variable not set for %v", svc)
	} else {
		hostportInfo = []string(strings.Split(svcEnvValue, ","))
		hostportInfo = replaceHost(hostportInfo)
		return hostportInfo, nil
	}
}
