package master

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func (m *Server) addKeyParam(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		data *string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.KvAdd))
	defer func() {
		doStatAndMetric(proto.KvAdd, metric, err, nil)
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	for k, v := range r.Form {
		data, err = m.kv.addKey(&k, &v[0])
		if err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}

	_ = sendOkReply(w, r, newSuccessHTTPReply(data))
}

func (m *Server) delKeyParam(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.KvDel))
	defer func() {
		doStatAndMetric(proto.KvDel, metric, err, nil)
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := r.FormValue("key")
	if err = m.kv.delKey(&key); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg := fmt.Sprintf("delete key[%v] successfully", key)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) updateKeyParam(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		data *string
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.KvUpdate))
	defer func() {
		doStatAndMetric(proto.KvUpdate, metric, err, nil)
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	for k, v := range r.Form {
		data, err = m.kv.updateKey(&k, &v[0])
		if err != nil {
			sendErrReply(w, r, newErrHTTPReply(err))
			return
		}
	}
	_ = sendOkReply(w, r, newSuccessHTTPReply(data))
}

func (m *Server) getKeyParam(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		dbResult map[string][]byte
		result   = make(map[string]string, 0)
	)
	metric := exporter.NewTPCnt(apiToMetricsName(proto.KvGet))
	defer func() {
		doStatAndMetric(proto.KvGet, metric, err, nil)
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	key := r.FormValue("key")
	kfilter := r.FormValue("kfilter")
	vfilter := r.FormValue("vfilter")

	if strings.HasSuffix(key, "*") {
		key := []byte(kvPrefix + key)
		if kfilter != "" || vfilter != "" {
			dbResult, err = m.fsm.store.SeekForPrefixAndFilter(key[:len(key)-1], kfilter, vfilter)
		} else {
			dbResult, err = m.fsm.store.SeekForPrefix(key[:len(key)-1])
		}

		if err != nil {
			log.LogErrorf("action[getKeyParam],err:%v", err.Error())
			sendErrReply(w, r, newErrHTTPReply(proto.ErrInternalError))
			return
		}
		for k, v := range dbResult {
			result[k[len(kvPrefix):]] = string(v)
		}
		sendOkReply(w, r, newSuccessHTTPReply(result))
		return
	}

	data, err := m.kv.getKey(&key)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(data))
}
