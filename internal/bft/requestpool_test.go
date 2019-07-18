// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"go.uber.org/zap"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestReqPoolBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	insp := &testRequestInspector{}

	byteReq1 := makeTestRequest("1", "1", "foo")
	pool := bft.NewPool(log, insp, 3, 0)

	assert.Equal(t, 0, pool.Size())
	err = pool.Submit(byteReq1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, pool.Size())
	req1 := types.RequestInfo{
		ID:       "1",
		ClientID: "1",
	}
	err = pool.Submit(byteReq1, 1)
	assert.Error(t, err)
	assert.Equal(t, 1, pool.Size())
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)
	assert.Equal(t, 0, pool.Size())
	err = pool.Submit(byteReq1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, pool.Size())
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)
	assert.Equal(t, 0, pool.Size())

	byteReq2 := makeTestRequest("2", "2", "bar")
	err = pool.Submit(byteReq2, 2)
	assert.NoError(t, err)
	assert.Equal(t, 1, pool.Size())
	err = pool.Submit(byteReq1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, pool.Size())
	err = pool.Submit(byteReq1, 1)
	assert.Error(t, err)
	err = pool.Submit(byteReq2, 2)
	assert.Error(t, err)
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)
	err = pool.Submit(byteReq1, 1)
	assert.NoError(t, err)
	req2 := types.RequestInfo{
		ID:       "2",
		ClientID: "2",
	}
	err = pool.RemoveRequest(req2)
	assert.NoError(t, err)
	err = pool.Submit(byteReq2, 2)
	assert.NoError(t, err)

	byteReq3 := makeTestRequest("3", "3", "bog")
	err = pool.Submit(byteReq3, 3)
	assert.NoError(t, err)

	next := pool.NextRequests(4)
	assert.Equal(t, "1", insp.RequestID(next[0].Request).ID)
	assert.Equal(t, uint64(1), next[0].VerificationSqn)
	assert.Equal(t, "2", insp.RequestID(next[1].Request).ID)
	assert.Equal(t, uint64(2), next[1].VerificationSqn)
	assert.Equal(t, "3", insp.RequestID(next[2].Request).ID)
	assert.Equal(t, uint64(3), next[2].VerificationSqn)
	assert.Len(t, next, 3)

	err = pool.RemoveRequest(req2)
	assert.NoError(t, err)

	next = pool.NextRequests(4)
	assert.Equal(t, "1", insp.RequestID(next[0].Request).ID)
	assert.Equal(t, uint64(1), next[0].VerificationSqn)
	assert.Equal(t, "3", insp.RequestID(next[1].Request).ID)
	assert.Equal(t, uint64(3), next[1].VerificationSqn)
	assert.Len(t, next, 2)

	next = pool.NextRequests(1)
	assert.Equal(t, "1", insp.RequestID(next[0].Request).ID)
	assert.Equal(t, uint64(1), next[0].VerificationSqn)
	assert.Len(t, next, 1)

	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)

	req3 := types.RequestInfo{
		ID:       "3",
		ClientID: "3",
	}

	err = pool.RemoveRequest(req3)
	assert.NoError(t, err)

	next = pool.NextRequests(1)
	assert.Len(t, next, 0)

}

func TestEventuallySubmit(t *testing.T) {
	n := 100
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	pool := bft.NewPool(log, insp, 50, 0)

	wg := sync.WaitGroup{}
	wg.Add(2 * n)
	for i := 0; i < n; i++ {
		go func(i int) {
			iStr := fmt.Sprintf("%d", i)
			byteReq := makeTestRequest(iStr, iStr, "foo")
			err := pool.Submit(byteReq, 0)
			assert.NoError(t, err)
			wg.Done()
		}(i)

		go func(i int) {
			iStr := fmt.Sprintf("%d", i)
			req := types.RequestInfo{
				ID:       iStr,
				ClientID: iStr,
			}
			err := pool.RemoveRequest(req)
			for err != nil {
				err = pool.RemoveRequest(req)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestMakeRequest(t *testing.T) {
	r := makeTestRequest("AB", "CDE", "FGHI")
	assert.Equal(t, 21, len(r))

	clientID, txID, data := parseTestRequest(r)
	assert.Equal(t, "AB", clientID)
	assert.Equal(t, "CDE", txID)
	assert.Equal(t, "FGHI", data)

	ins := &testRequestInspector{}
	info := ins.RequestID(r)
	assert.Equal(t, "AB", info.ClientID)
	assert.Equal(t, "CDE", info.ID)
}

func makeTestRequest(clientID, txID, data string) []byte {
	buffLen := make([]byte, 4)
	buff := make([]byte, 12)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(clientID)))
	buff = append(buff[0:0], buffLen...)
	buff = append(buff, []byte(clientID)...)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(txID)))
	buff = append(buff, buffLen...)
	buff = append(buff, []byte(txID)...)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(data)))
	buff = append(buff, buffLen...)
	buff = append(buff, []byte(data)...)

	return buff
}

func parseTestRequest(request []byte) (clientID, txID, data string) {
	l := binary.LittleEndian.Uint32(request)
	buff := request[4:]

	clientID = string(buff[:l])
	buff = buff[l:]

	l = binary.LittleEndian.Uint32(buff)
	buff = buff[4:]

	txID = string(buff[:l])
	buff = buff[l:]

	l = binary.LittleEndian.Uint32(buff)
	buff = buff[4:]

	data = string(buff[:l])

	return
}

type testRequestInspector struct {
}

func (ins *testRequestInspector) RequestID(req []byte) types.RequestInfo {
	var info types.RequestInfo
	info.ClientID, info.ID, _ = parseTestRequest(req)
	return info
}
