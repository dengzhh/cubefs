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
	"github.com/cubefs/cubefs/proto"
	"net/http"
	"strconv"
)

type KvAPI struct {
	mc *MasterClient
}

func (api *KvAPI) AddKvParam(key, value string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.KvAdd)
	request.addParam(fmt.Sprintf("%s", key), value)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *KvAPI) DelKvParam(key string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.KvDel)
	request.addParam("key", key)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *KvAPI) UpdateKvParam(key, value string) (err error) {
	var request = newAPIRequest(http.MethodGet, proto.KvUpdate)
	request.addParam(fmt.Sprintf("%s", key), value)
	_, err = api.mc.serveRequest(request)
	return
}

func (api *KvAPI) UpdateKvValue(key, value string) (result int64, err error) {
	var request = newAPIRequest(http.MethodGet, proto.KvUpdate)
	request.addParam(fmt.Sprintf("%s", key), value)
	resp, err := api.mc.serveRequest(request)
	return strconv.ParseInt(string(resp), 10, 64)
}

func (api *KvAPI) GetKvValue(key string) (result int64, err error) {
	var request = newAPIRequest(http.MethodGet, proto.KvGet)
	request.addParam("key", key)
	resp, err := api.mc.serveRequest(request)
	return strconv.ParseInt(string(resp), 10, 64)
}

func (api *KvAPI) GetKvParam(key string) (value string, err error) {
	var request = newAPIRequest(http.MethodGet, proto.KvGet)
	request.addParam("key", key)
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *KvAPI) GetKvParamByKeyFilter(key string, kfilter string) (value string, err error) {
	var request = newAPIRequest(http.MethodGet, proto.KvGet)
	request.addParam("key", key)
	request.addParam("kfilter", kfilter)
	data, err := api.mc.serveRequest(request)
	return string(data), err
}

func (api *KvAPI) GetKvParamByValueFilter(key string, vfilter string) (value string, err error) {
	var request = newAPIRequest(http.MethodGet, proto.KvGet)
	request.addParam("key", key)
	request.addParam("vfilter", vfilter)
	data, err := api.mc.serveRequest(request)
	return string(data), err
}
