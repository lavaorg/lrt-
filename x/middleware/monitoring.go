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

package middleware

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pborman/uuid"
	"github.com/verizonlabs/northstar/pkg/mlog"
)

type TxnInfo struct {
	TransactionId string
}

const TXNINFO = "txnInfo"

func (l TxnInfo) Info(template string, args ...interface{}) {
	msg := fmt.Sprintf("TxnInfo: %s - %s", l.TransactionId, template)
	mlog.Info(msg, args...)
}

func (l TxnInfo) Error(template string, args ...interface{}) {
	msg := fmt.Sprintf("TxnInfo: %s - %s", l.TransactionId, template)
	mlog.Error(msg, args...)
}

func (l TxnInfo) Debug(template string, args ...interface{}) {
	msg := fmt.Sprintf("TxnInfo: %s - %s", l.TransactionId, template)
	mlog.Debug(msg, args...)
}

// Helper function to get transaction info from gin context
func GetTxnInfoFromContext(context *gin.Context) *TxnInfo {
	transactionInfo, _ := context.Get(TXNINFO)
	if transactionInfo == nil {
		mlog.Debug("No transaction info found.")
		txnInfo := &TxnInfo{}
		return txnInfo
	}
	return transactionInfo.(*TxnInfo)
}

// Returns the middleware used for monitoring. E.g., add transaction id, add latency.
func Monitoring() gin.HandlerFunc {
	return func(context *gin.Context) {
		mlog.Info("Monitoring")
		HEADER_TRANSACTION_ID := "X-Transaction-Id"
		transactionId := context.Request.Header.Get(HEADER_TRANSACTION_ID)
		if transactionId == "" {
			transactionId = uuid.NewRandom().String()
			mlog.Info("No transaction id not found in request.  Generated transaction id: %s", transactionId)
		}
		context.Set(TXNINFO, &TxnInfo{
			TransactionId: transactionId,
		})
		startTime := time.Now()
		context.Next()
		context.Writer.Header().Set("X-Latency", time.Since(startTime).String())
		context.Writer.Header().Set(HEADER_TRANSACTION_ID, transactionId)
	}
}
