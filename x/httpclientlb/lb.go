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

package httpclientlb

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/verizonlabs/northstar/pkg/config"
	"github.com/verizonlabs/northstar/pkg/management"
	"github.com/verizonlabs/northstar/pkg/mlog"
)

var (
	gResolveMesosDns   = false
	dnsQueryTimerSec   = 0
	countIgnoreIOError = 2
)

type blackListIpInfo struct {
	ip    string
	count int
}

type peerInfo struct {
	url          string
	dnsName      string
	ipLock       sync.Mutex
	ips          []string
	blackListIps []blackListIpInfo
	portPath     string
	lastIndex    int
}

type LbClient struct {
	info             peerInfo
	dnsQueryTimerSec time.Duration
	resolveMesosDns  bool
}

// Function to check whether two IP array have same elements or not
func areIPArraytDiff(prev, curr []string) bool {
	if len(curr) != len(prev) {
		return true
	}

	for _, c := range curr {
		found := false
		for _, p := range prev {
			if p == c {
				found = true
				break
			}
		}
		if found == false {
			mlog.Debug("Ip[%s] not found in previous list[%s]", c, prev)
			return true
		}
	}

	return false
}

// Function to parse the URL and get the MESOS DNS name for the application
// hostURL should be of format
// http://data-dev4-dakota.mon-marathon-service.mesos:8788/ds/v1
// or
// http://10.10.7.1:11403/ds/v1
func getDNSNamePortFromURL(hostURL string) (string, string, error) {
	u, err := url.Parse(hostURL)
	if err != nil {
		mlog.Error(err.Error())
		return "", "", err
	}
	mm := strings.Split(u.Host, ":")
	if len(mm) != 2 {
		return "", "", fmt.Errorf("Invalid port in Url %s", hostURL)
	}

	return mm[0], mm[1] + u.Path, nil
}

func getVar() (err error) {

	gResolveMesosDns, _ = config.GetBool("HTTPCLIENTLB_RESOLVE_MESOS_DNS", false)
	if gResolveMesosDns == true {
		// Set GODEBUG for CDnsResolver as defaut DNS resolver.
		// GoDNSResolver results in failure many a times.
		if err := os.Setenv("GODEBUG", "netdns=cgo"); err != nil {
			return fmt.Errorf("Could not set GODEBUG for cgo dns resolver")
		}

		dnsQueryTimerSec, _ = config.GetInt("HTTPCLIENTLB_MESOS_DNS_QUERY_INTERVAL", 5)
	}

	mlog.Info("dnsQueryTimerSec[%d] resolveMesosDns[%v]", dnsQueryTimerSec, gResolveMesosDns)
	return nil
}

/*GetClient Application is expected to call this function for getting a client object for
 * an application.
 * Function tries to get MESOS DNS for the application from the URL given
 * and returns success only if address is resolved for the MESOS DNS.
 *
 * IMPORTANT :: User should set the DNS_SERVER_QUERY_TIME environment variable to use this library
 *
 * mesosURL : Complete URL for the application for which Client is requested
 *
 * Return ::
 * lbc 		: Client for lb http client
 * err		: In case Mesos DNS URL is not resolved, Error will be returned to application
 *			  Application can move further, once the other application DNS name is resolved
 *			  client will start sending the requests from application to requested url in
 *			  round-robin fashion
 */
func GetClient(mesosURL string) (lbc *LbClient, err error) {
	var ips []string
	var dns, portPath string

	if err = getVar(); err != nil {
		mlog.Error("Environment variable not set [%s]", err)
		return nil, err
	}

	if dns, portPath, err = getDNSNamePortFromURL(mesosURL); err != nil {
		mlog.Error("Could not get mesosdns from the url [%s] Error:%s", mesosURL, err)
		return nil, err
	}

	if ips, err = net.LookupHost(dns); err != nil {
		mlog.Info("Lookup failed for DNS[%s] Error[%s].Not Returning Error", dns, err)
	}

	mlog.Info("MesosUrl[%s] DnsName[%s] Port[%s] Ips[count:%d :: %s] dnsQueryTimerSec[%d]", mesosURL, dns, portPath, len(ips), ips, dnsQueryTimerSec)

	lbc = &LbClient{
		info: peerInfo{
			url:          mesosURL,
			dnsName:      dns,
			ips:          ips,
			blackListIps: make([]blackListIpInfo, 0),
			portPath:     portPath},
		dnsQueryTimerSec: time.Duration(dnsQueryTimerSec),
		resolveMesosDns:  gResolveMesosDns}

	if gResolveMesosDns == true {
		go lbc.startDNSQuery()
	}

	return lbc, nil
}

// function to do the DNS query recursively for a client
func (lbc *LbClient) startDNSQuery() {
	mlog.Info("Starting DNS Query routine for [%s] at an interval of [%d]sec", lbc.info.dnsName, lbc.dnsQueryTimerSec)
	for {
		if ips, err := net.LookupHost(lbc.info.dnsName); err == nil {
			lbc.info.ipLock.Lock()
			if areIPArraytDiff(lbc.info.ips, ips) == true {
				mlog.Info("Ips changed for MesosDns[%s] Prev[%s] Curr[%s]", lbc.info.url, lbc.info.ips, ips)
				lbc.info.ips = ips
			}
			lbc.info.blackListIps = lbc.info.blackListIps[:0]
			lbc.info.ipLock.Unlock()
		} else {
			mlog.Info("Ignoring Error in getting the hosts for [%s] error[%s] ips[%s]", lbc.info.dnsName, err, ips)
		}
		time.Sleep(time.Second * lbc.dnsQueryTimerSec)
	}

}

// Check whether IP can be decalred blacklisted or not
func (lbc *LbClient) isIpBlackListed(ip string) bool {
	for _, i := range lbc.info.blackListIps {
		if i.ip == ip {
			if i.count >= countIgnoreIOError {
				return true
			}
		}
	}
	return false
}

// Check whether IP is blacklisted or not
func (lbc *LbClient) isIpPresentInBlackList(ip string) bool {
	for idx, _ := range lbc.info.blackListIps {
		if lbc.info.blackListIps[idx].ip == ip {
			lbc.info.blackListIps[idx].count++
			return true
		}
	}
	return false
}

// Parse for various tarnsport layer errors in the error, if found add the IP in blackList and return true
// If error is not due to Transport layer return false.
func (lbc *LbClient) parseErrorForTransportErrors(err string) bool {
	if strings.Contains(err, "i/o timeout") == true || strings.Contains(err, "connection refused") == true ||
		strings.Contains(err, " connection reset by peer") == true || strings.Contains(err, "transport closed") == true {
		for _, val := range strings.Fields(err) {
			if u, err := url.Parse(val); err == nil && u.Host != "" {
				ip := strings.Split(u.Host, ":")[0]
				if ip != "" {
					lbc.info.ipLock.Lock()
					if lbc.isIpPresentInBlackList(ip) == false {
						lbc.info.blackListIps = append(lbc.info.blackListIps, blackListIpInfo{ip, 1})
						mlog.Info("IP [%s] added in blacklist", ip)
					}
					lbc.info.ipLock.Unlock()
					return true
				}
			}
		}
	}
	return false
}

// Function to get the URL for the APP from doing round robin on the IPs resoved from MESOS DNS for the app
func (lbc *LbClient) getHostPort() string {
	if lbc.resolveMesosDns == true {
		lbc.info.ipLock.Lock()
		defer lbc.info.ipLock.Unlock()
		for {
			if len(lbc.info.ips) == 0 {
				mlog.Debug("No Ips available for [%s]", lbc.info.dnsName)
				return ""
			}

			lbc.info.lastIndex++
			if lbc.info.lastIndex >= len(lbc.info.ips) {
				lbc.info.lastIndex = 0
			}
			ip := lbc.info.ips[lbc.info.lastIndex]
			if len(lbc.info.blackListIps) == 0 || lbc.isIpBlackListed(ip) == false {
				hostPort := "http://" + ip + ":" + lbc.info.portPath
				mlog.Debug("For MesosUrl[%s] generated url is [%s] idx[%d]", lbc.info.url, hostPort, lbc.info.lastIndex)
				return hostPort
			}
			mlog.Info("Removing blacklisted ip[%s] from ipList[%v]", ip, lbc.info.ips)
			lbc.info.ips = append(lbc.info.ips[:lbc.info.lastIndex], lbc.info.ips[lbc.info.lastIndex+1:]...)
		}
	} else {
		mlog.Debug("For MesosUrl[%s] generated url is [%s]", lbc.info.url, lbc.info.url)
		return lbc.info.url
	}
}

//PostJSON :: Perform a HTTP POST using JSON content marshaled from the given content
// returning response content as a byte array
func (lbc *LbClient) PostJSON(path string, content interface{}) ([]byte, *management.Error) {
	return lbc.PostJSONWithHeaders(path, content, nil)
}

//PostJSON :: Perform a HTTP POST using JSON content marshaled from the given content
// returning response content as a byte array
func (lbc *LbClient) PostJSONWithHeaders(path string, content interface{}, headers map[string]string) ([]byte, *management.Error) {

	for {
		hostAndPort := lbc.getHostPort()
		if hostAndPort == "" {
			return nil, management.NewError(http.StatusBadGateway, "bad_gateway", "No Host Found")
		}

		if resp, err := management.PostJSONWithHeaders(hostAndPort, path, content, headers); err == nil {
			return resp, nil
		} else {
			// Error other than transport layer errors, lets pass to caller
			if lbc.parseErrorForTransportErrors(err.Description) == false {
				return resp, err
			}
		}
	}
}

//Post :: Perform an empty HTTP POST
// returning response content as a byte array
func (lbc *LbClient) Post(path string) ([]byte, *management.Error) {

	for {
		hostAndPort := lbc.getHostPort()
		if hostAndPort == "" {
			return nil, management.NewError(http.StatusBadGateway, "bad_gateway", "No Host Found")
		}

		if resp, err := management.Post(hostAndPort, path); err == nil {
			return resp, nil
		} else {
			// Error other than i/o timeout, lets pass to caller
			if lbc.parseErrorForTransportErrors(err.Description) == false {
				return resp, err
			}
		}
	}
}

//PostWithHeaders :: Perform an HTTP Post with Headers
func (lbc *LbClient) PostWithHeaders(path string, content []byte, headers map[string]string) ([]byte, *management.Error) {
	for {
		hostAndPort := lbc.getHostPort()
		if hostAndPort == "" {
			return nil, management.NewError(http.StatusBadGateway, "bad_gateway", "No Host Found")
		}

		if resp, err := management.PostDataWithHeaders(hostAndPort, path, content, headers); err == nil {
			return resp, nil
		} else {
			// Error other than i/o timeout, lets pass to caller
			if lbc.parseErrorForTransportErrors(err.Description) == false {
				return resp, err
			}
		}
	}
}

//PutJSON :: Perform a HTTP PUT using the JSON content marsheled from the given content
// returning response content as a byte array
func (lbc *LbClient) PutJSON(path string, content interface{}) ([]byte, *management.Error) {
	return lbc.PutJSONWithHeaders(path, content, nil)
}

//PutJSON :: Perform a HTTP PUT using the JSON content marsheled from the given content
// returning response content as a byte array
func (lbc *LbClient) PutJSONWithHeaders(path string, content interface{}, headers map[string]string) ([]byte, *management.Error) {

	for {
		hostAndPort := lbc.getHostPort()
		if hostAndPort == "" {
			return nil, management.NewError(http.StatusBadGateway, "bad_gateway", "No Host Found")
		}
		if resp, err := management.PutJSONWithHeaders(hostAndPort, path, content, headers); err == nil {
			return resp, nil
		} else {
			// Error other than i/o timeout, lets pass to caller
			if lbc.parseErrorForTransportErrors(err.Description) == false {
				return resp, err
			}
		}
	}
}

//Put :: Perform an empty HTTP PUT
// returning response content as a byte array
func (lbc *LbClient) Put(path string) ([]byte, *management.Error) {

	for {
		hostAndPort := lbc.getHostPort()
		if hostAndPort == "" {
			return nil, management.NewError(http.StatusBadGateway, "bad_gateway", "No Host Found")
		}
		if resp, err := management.Put(hostAndPort, path); err == nil {
			return resp, nil
		} else {
			// Error other than i/o timeout, lets pass to caller
			if lbc.parseErrorForTransportErrors(err.Description) == false {
				return resp, err
			}
		}
	}
}

//Get :: Perform a HTTP GET
// returning response content as a byte array
func (lbc *LbClient) Get(path string) ([]byte, *management.Error) {
	return lbc.GetWithHeaders(path, nil)
}

//Get :: Perform a HTTP GET with request headers
// returning response content as a byte array
func (lbc *LbClient) GetWithHeaders(path string, headers map[string]string) ([]byte, *management.Error) {

	for {
		hostAndPort := lbc.getHostPort()
		if hostAndPort == "" {
			return nil, management.NewError(http.StatusBadGateway, "bad_gateway", "No Host Found")
		}
		if resp, err := management.GetWithHeaders(hostAndPort, path, headers); err == nil {
			return resp, nil
		} else {
			// Error other than i/o timeout, lets pass to caller
			if lbc.parseErrorForTransportErrors(err.Description) == false {
				return resp, err
			}
		}
	}
}

//Delete :: Perform a HTTP DELETE
func (lbc *LbClient) Delete(path string) *management.Error {
	return lbc.DeleteWithHeaders(path, nil)
}

//Delete :: Perform a HTTP DELETE
func (lbc *LbClient) DeleteWithHeaders(path string, headers map[string]string) *management.Error {

	for {
		hostAndPort := lbc.getHostPort()
		if hostAndPort == "" {
			return management.NewError(http.StatusBadGateway, "bad_gateway", "No Host Found")
		}
		if err := management.DeleteWithHeaders(hostAndPort, path, headers); err == nil {
			return nil
		} else {
			// Error other than i/o timeout, lets pass to caller
			if lbc.parseErrorForTransportErrors(err.Description) == false {
				return err
			}
		}
	}
}
