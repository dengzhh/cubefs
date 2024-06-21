package master

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"
	"sync"
)

type Kv struct {
	fsm          *MetadataFsm
	partition    raftstore.Partition
	kvStore      sync.Map //K: key, V: value, TODO: use hash-divided-shards to reduce conflicts
	kvStoreMutex sync.RWMutex
}

func newKv(fsm *MetadataFsm, partition raftstore.Partition) (u *Kv) {
	u = new(Kv)
	u.fsm = fsm
	u.partition = partition
	return
}

func (u *Kv) addKey(key, value *string) (result *string, err error) {
	var (
		exist bool
	)
	result = value
	if key == nil || *key == "" || value == nil {
		err = proto.ErrParamError
		return
	}

	u.kvStoreMutex.Lock()
	defer u.kvStoreMutex.Unlock()
	//check duplicate
	if _, exist = u.kvStore.Load(*key); exist {
		log.LogErrorf("action[addKey], key: %v, value: %v", *key, *value)
		err = proto.ErrDuplicateKey
		return
	}

	if err = u.syncAddKv(key, value); err != nil {
		return
	}

	u.kvStore.Store(*key, *value)
	log.LogInfof("action[addKey], key: %v, value: %v", *key, *value)
	return
}

func (u *Kv) delKey(key *string) (err error) {
	u.kvStoreMutex.Lock()
	defer u.kvStoreMutex.Unlock()

	if _, exist := u.kvStore.Load(*key); !exist {
		err = proto.ErrKeyNotExists
		return
	}
	if err = u.syncDelKv(key); err != nil {
		return
	}
	u.kvStore.Delete(*key)
	log.LogInfof("action[delKey], key: %v", *key)
	return
}

func (u *Kv) updateKey(key, value *string) (result *string, err error) {
	if key == nil || *key == "" || value == nil {
		err = proto.ErrParamError
		return
	}

	u.kvStoreMutex.Lock()
	defer u.kvStoreMutex.Unlock()

	oldValue, exist := u.kvStore.Load(*key)

	valStr := *value
	if strings.HasPrefix(valStr, "%+") || strings.HasPrefix(valStr, "%-") {
		if !exist {
			oldValue = "0"
		}
		oldVal, err2 := strconv.ParseInt(oldValue.(string), 10, 64)
		if err2 != nil {
			log.LogWarnf("action[updateKey], key: %v, value: %v, err: %v", *key, valStr, err2)
		}
		diff, _ := strconv.ParseInt(valStr[1:], 10, 64)
		newVal := strconv.FormatInt(oldVal+diff, 10)
		if err = u.syncUpdateKv(key, &newVal); err != nil {
			log.LogErrorf("action[updateKey], key: %v, value: %v, err: %v", *key, valStr, err)
			return
		}
		u.kvStore.Store(*key, newVal)
		log.LogInfof("action[updateKey], key: %v, value: %v", *key, valStr)
		return &newVal, nil
	} else if strings.HasPrefix(valStr, "%1") {
		if exist {
			newVal := (*value)[2:] + "#" + strings.SplitN(oldValue.(string), "#", 2)[1]
			if err = u.syncUpdateKv(key, &newVal); err != nil {
				return
			}
			u.kvStore.Store(*key, newVal)
			log.LogInfof("action[updateKey], key: %v, value: %v", *key, valStr)
			return &newVal, nil
		} else {
			return nil, proto.ErrKeyNotExists
		}
	} else if strings.HasPrefix(valStr, "%2") {
		if exist {
			newVal := strings.SplitN(oldValue.(string), "#", 2)[0] + "#" + (*value)[2:]
			if err = u.syncUpdateKv(key, &newVal); err != nil {
				return
			}
			u.kvStore.Store(*key, newVal)
			log.LogInfof("action[updateKey], key: %v, value: %v", *key, valStr)
			return &newVal, nil
		} else {
			return nil, proto.ErrKeyNotExists
		}
	}

	result = value

	if err = u.syncUpdateKv(key, value); err != nil {
		return
	}
	u.kvStore.Store(*key, *value)

	log.LogInfof("action[updateKey], key: %v, value: %v", *key, *value)
	return
}

func (u *Kv) getKey(key *string) (value *string, err error) {
	data, exist := u.kvStore.Load(*key)
	if !exist {
		log.LogInfof("action[getKey], key[%v] not exist", *key)
		err = proto.ErrKeyNotExists
		return
	}
	log.LogInfof("action[getKey], key[%v], value: %v", *key, data.(string))
	value2 := data.(string)
	return &value2, nil
}
