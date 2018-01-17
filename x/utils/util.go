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
	"errors"
	"log"
	"net"
	"strings"
	"time"

	"github.com/verizonlabs/northstar/pkg/mlog"
	"github.com/verizonlabs/northstar/pkg/servdisc"
)

const DEV_IMSI_LEN int = 14

// Instance is a uint8 datatype (Bitmap). Its value corresponds to the value of the bits that have been set.
// The range of the bitfield is  0 to 31 (Little Endian by our convention)

/*
GetZeroBitIndices returns array of indices with 0 bit
*/
func GetZeroBitIndices(value uint, maxIndex uint8) []uint8 {
	var indices []uint8
	var i uint8
	for i = 0; i < maxIndex; value >>= 1 { // until all bits are zero
		if (value & 1) == 0 { // check lower bit
			indices = append(indices, i)
			log.Println("index", i)
		}
		i++
	}
	return indices
}

/*
GetCorrelationId returns correlataion id by merging
8 bits ServiceInstanceId
6 bits ServiceType
2 bits counter

ServiceInstanceId - 0 to 255
ServiceType - 0 to 63
Counter - 0 to 3
*/
func GetCorrelationId(sid uint16, st servdisc.ServiceType, ctr uint8) (uint16, error) {
	if sid > 255 {
		return 0, errors.New("Invalid service instance id, valid values are 0 to 255")
	}
	if ctr > 3 {
		return 0, errors.New("Invalid Counter , valid values are 0 to 3")
	}
	return uint16(sid)<<8 | uint16(st)<<2 | uint16(ctr), nil
}

// GetToken returns token merging 8-bit datacenter id, 8-bit service type and
// 16-bit correlation id.
func GetToken(did byte, st servdisc.ServiceType, cid uint16) uint32 {
	return uint32(did)<<24 | uint32(st)<<16 | uint32(cid)
}

// GetNextToken takes a token and returns next token by incrementing the
// counter part.
func GetNextToken(t uint32) uint32 {
	n := t & 0xf // Extract counter
	n++
	if n == 16 {
		n = 0
	}
	t = (t & 0xfffffff0) | n // Reset and assign counter
	return t
}

/*
GetServiceInstanceId returns service instance id (8bits) from the correlation id
*/
func GetServiceInstanceId(cId uint16) uint16 {
	return extract(cId, 8, 16)
}

/*
GetServiceType returns service type (6bits) from the correlation id
*/
func GetServiceType(cId uint16) servdisc.ServiceType {
	return servdisc.ServiceType(extract(cId, 2, 8))
}

/*
GetNextCorrelationId returns correlataion id merging 8 bits ServiceInstanceId,
6 bits ServiceType and  2 bits counter incremeted by 1.
If counter value is 3, will be reset to 0
*/
func GetNextCorrelationId(cId uint16) (uint16, error) {

	var ctr uint16 = extract(cId, 0, 2)
	sId := GetServiceInstanceId(cId)
	st := GetServiceType(cId)

	ctr++
	if ctr > 3 {
		ctr = 0
	}
	return GetCorrelationId(sId, st, uint8(ctr))
}
func extract(value uint16, begin uint16, end uint16) uint16 {
	var mask uint16 = (1 << (end - begin)) - 1
	return (value >> begin) & mask
}

// Returns the time representation of the ISO 8601 string.
func GetTimeFromISO8601(t string) (time.Time, error) {
	return time.Parse(time.RFC3339, t)
}

// Returns true if the specified datetime string if valid ISO 8601.
func IsValidISO8601(t string) bool {
	_, err := time.Parse(time.RFC3339, t)

	if err != nil {
		return false
	}

	return true
}

// Returns the ISO 8601 string representation of time.
func GetTimeInISO8601(t time.Time) string {
	return t.Format(time.RFC3339)
}

//Returns the ISO 8601 time value for a string
func GetTimeFromString(s string) time.Time {
	convertedTime, _ := time.Parse(time.RFC3339, s)
	return convertedTime
}

func GetCurrentTimeInISO8601() string {
	return time.Now().Format(time.RFC3339)
}

func HostsToIps(hosts string) string {
	hostList := strings.Split(hosts, ",")
	ipList := []string{}
	for _, host := range hostList {
		ip := net.ParseIP(host)
		if ip == nil {
			ips, err := net.LookupIP(host)
			if err == nil {
				if len(ips) != 0 {
					for _, v := range ips {
						if v4 := v.To4(); v4 != nil {
							ip = v4
							break
						}
					}
					if ip == nil {
						ip = ips[0]
					}
				}
			} else {
				mlog.Error("Host lookup failed: %s", err.Error())
			}
		}
		if ip != nil {
			ipList = append(ipList, ip.String())
		}
	}
	return strings.Join(ipList, ",")

}
