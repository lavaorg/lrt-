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
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/verizonlabs/northstar/pkg/mlog"
)

func TestImsiGen(t *testing.T) {
	var imsi uint64
	var devId uint64
	var svcType ServiceType
	var instId int
	var gwts, gps, fast bool
	for i := 0; i < 100000; i++ {
		rand.Seed(time.Now().UnixNano())
		devId = uint64(rand.Intn(1000000))
		svcType = SvtType()
		instId = int(rand.Intn(1000))
		gwts = Boolean()
		gps = Boolean()
		imsi = GenerateImsi(devId, svcType, instId, gwts, gps)
		if false == ValidateResult(imsi, devId, svcType, instId, gwts, gps) {
			PrintImsiInfo(imsi)
			t.Error("Error For IMSI,devId,InstId,Svc,gwts,gps ", imsi, devId, instId, svcType, gwts, gps, fast)
			break
		} else {
			mlog.Info("Success for %d", imsi)
		}
	}
}
func Boolean() bool {
	rand.Seed(time.Now().UnixNano())
	nr := rand.Intn(2)
	return nr != 0
}
func SvtType() ServiceType {
	val := rand.Intn(4) + 1
	switch val {
	case 1:
		return UtService
	case 2:
		return UmService
	case 3:
		return MimicService
	case 4:
		return SmsService
	default:
		return UtService
	}

}

func ValidateResult(imsi uint64, deviceId uint64, svcType ServiceType, instancdId int, viaGwts bool, simulatedGPS bool) bool {
	imsiStr := strconv.FormatUint(imsi, 10)
	devId, _ := strconv.Atoi(string(imsiStr[7:]))

	if svcType != GetDeviceType(imsi) {
		mlog.Error("Invalid svcType")
		return false
	} else if instancdId != GetInstanceId(imsi) {
		mlog.Error("Invalid instancdId")
		return false
	} else if simulatedGPS != IsSimulatedGPS(imsi) {
		mlog.Error("Invalid simulatedGPS")
		return false
	} else if viaGwts != IsGwtsBasedDevice(imsi) {
		mlog.Error("Invalid viaGwts")
		return false
	} else if deviceId != uint64(devId+1) {
		mlog.Error("Invalid deviceId %d %d", deviceId, devId+1)
		return false
	}
	return true
}
