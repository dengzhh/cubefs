// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"fmt"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func (u *Kv) submit(metadata *RaftCmd) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return errors.New(err.Error())
	}
	if _, err = u.partition.Submit(cmd); err != nil {
		msg := fmt.Sprintf("action[kv_submit] err:%v", err.Error())
		return errors.New(msg)
	}
	return
}

// key = #ak#accesskey, value = userInfo
func (u *Kv) syncAddKv(key, value *string) (err error) {
	return u.syncPutKv(opSyncAddKv, key, value)
}

func (u *Kv) syncDelKv(key *string) (err error) {
	return u.syncPutKv(opSyncDelKv, key, nil)
}

func (u *Kv) syncUpdateKv(key, value *string) (err error) {
	return u.syncPutKv(opSyncUpdateKv, key, value)
}

func (u *Kv) syncPutKv(opType uint32, key, value *string) (err error) {
	raftCmd := new(RaftCmd)
	raftCmd.Op = opType
	raftCmd.K = kvPrefix + *key
	if value == nil {
		raftCmd.V = []byte("")
	} else {
		raftCmd.V = []byte(*value)
	}
	return u.submit(raftCmd)
}

func (u *Kv) loadKvStore() (err error) {
	result, err := u.fsm.store.SeekForPrefix([]byte(kvPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadUserKeyInfo], err: %v", err.Error())
		return err
	}
	for key, value := range result {
		// key = "#kv#..."
		u.kvStore.Store(key[len(kvPrefix):], string(value))
		log.LogDebugf("action[loadKvStore], userID[%v]", key)
	}
	return
}
