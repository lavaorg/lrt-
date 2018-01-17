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

package utils

import (
	"strconv"

	"github.com/verizonlabs/northstar/pkg/mlog"
)

// HEADER
// Name        | MCC    | Instance |  Dev Type | Misc Flags         |Device Id    |
// ------------|--------|----------|-----------|--------------------|-------------|
// # of Digits | 3      | 3        |  1        | 2                  | 6           |
// offset      | 0-2    | 3-5      |  6        | 7-8                | 9-14        |
// Value       | Int    | Int      |  Int      | Hex                | Int         |
// Valid Value | 001    | 000-999  |  1-4      |0x00,0x01,0x02,0x03 |000000-999999|

const TEST_MCC_VALUE = 1

//Since Imsi is treated as uint64, MCC 001 shall become 1 and length will be reduced to 13 for simulated imsi
const SIMULATED_IMSI_LEN = 13

type ServiceType uint8

var (
	UtService    ServiceType = 1
	UmService    ServiceType = 2
	MimicService ServiceType = 3
	SmsService   ServiceType = 4
)

var (
	GwtsFlag   = 0x01
	SimGpsFlag = 0x02
	SpareBit1  = 0x04
	SpareBit2  = 0x08
	SpareBit3  = 0x10
	SpareBit4  = 0x20
)

//Range For Various Params
//deviceId 1 to 1000000, SvcType ServiceType values, instanceId 0-999
func GenerateImsi(deviceId uint64, svcType ServiceType, instancdId int, viaGwts bool, simulatedGPS bool) (imsi uint64) {
	flag := 0
	if viaGwts == true {
		flag = GwtsFlag
	}
	if simulatedGPS == true {
		flag |= SimGpsFlag
	}

	imsi = TEST_MCC_VALUE*1000000000000 + uint64(instancdId*1000000000) + uint64(svcType)*100000000 + uint64(flag)*1000000 + (deviceId - 1)

	return
}

//Return true if the given Imsi corresponds to a similatued device, false otherwise
func IsSimulatedDevice(imsi uint64) bool {
	imsiStr := strconv.FormatUint(imsi, 10)
	imsi1, _ := strconv.Atoi(string(imsiStr[0:1]))

	if len(imsiStr) == SIMULATED_IMSI_LEN && imsi1 == TEST_MCC_VALUE {
		return true
	} else {
		return false
	}
}

func GetInstanceId(imsi uint64) (instanceId int) {
	if false == IsSimulatedDevice(imsi) {
		mlog.Debug("Not a Simulated IMSI.. Routing Via first instance")
		return 0
	}
	imsiStr := strconv.FormatUint(imsi, 10)
	instanceId, _ = strconv.Atoi(string(imsiStr[1:4]))
	return
}
func GetDeviceType(imsi uint64) (devType ServiceType) {
	if false == IsSimulatedDevice(imsi) {
		mlog.Debug("Not a Simulated IMSI.. Default Device UT Type")
		return UtService
	}
	imsiStr := strconv.FormatUint(imsi, 10)
	dev, _ := strconv.Atoi(string(imsiStr[4:5]))
	devType = ServiceType(dev)
	return
}

func IsSimulatedGPS(imsi uint64) bool {
	if false == IsSimulatedDevice(imsi) {
		mlog.Debug("Not a Simulated IMSI.. Not a Simulated GPS")
		return false
	}
	imsiStr := strconv.FormatUint(imsi, 10)
	flags, _ := strconv.Atoi(string(imsiStr[5:7]))
	if flags&SimGpsFlag != 0 {
		return true
	}
	return false
}

func IsGwtsBasedDevice(imsi uint64) bool {
	if false == IsSimulatedDevice(imsi) {
		mlog.Debug("Not a Simulated IMSI.. Default devices is GWTS true")
		return true
	}
	imsiStr := strconv.FormatUint(imsi, 10)
	flags, err := strconv.Atoi(string(imsiStr[5:7]))
	if flags&GwtsFlag == 0 && err == nil {
		return false
	}
	return true
}

func PrintImsiInfo(imsi uint64) {
	mlog.Info("IMSI:%d\n IsSimulatedImsi:[%v]\n GetInstanceId:[%v]\n DeviceType:[%v]\n IsGwtsBasedDevice:[%v]\n IsSimulatedGPS:[%v]\n",
		imsi, IsSimulatedDevice(imsi), GetInstanceId(imsi), GetDeviceType(imsi), IsGwtsBasedDevice(imsi), IsSimulatedGPS(imsi))
}
