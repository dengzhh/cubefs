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

package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
)

func (mp *metaPartition) UpdateXAttr(req *proto.UpdateXAttrRequest, p *Packet) (err error) {
	newValueList := strings.Split(req.Value, ",")
	filesInc, _ := strconv.ParseInt(newValueList[0], 10, 64)
	dirsInc, _ := strconv.ParseInt(newValueList[1], 10, 64)
	bytesInc, _ := strconv.ParseInt(newValueList[2], 10, 64)

	mp.xattrLock.Lock()
	defer mp.xattrLock.Unlock()
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		if value, exist := extend.Get([]byte(req.Key)); exist {
			oldValueList := strings.Split(string(value), ",")
			oldFiles, _ := strconv.ParseInt(oldValueList[0], 10, 64)
			oldDirs, _ := strconv.ParseInt(oldValueList[1], 10, 64)
			oldBytes, _ := strconv.ParseInt(oldValueList[2], 10, 64)
			newFiles := oldFiles + filesInc
			newDirs := oldDirs + dirsInc
			newBytes := oldBytes + bytesInc
			newValue := strconv.FormatInt(int64(newFiles), 10) + "," +
				strconv.FormatInt(int64(newDirs), 10) + "," +
				strconv.FormatInt(int64(newBytes), 10)
			var extend = NewExtend(req.Inode)
			extend.Put([]byte(req.Key), []byte(newValue))
			if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			p.PacketOkReply()
			return
		} else {
			extend.Put([]byte(req.Key), []byte(req.Value))
			if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
			p.PacketOkReply()
			return
		}
	} else {
		var extend = NewExtend(req.Inode)
		extend.Put([]byte(req.Key), []byte(req.Value))
		if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			return
		}
		p.PacketOkReply()
		return
	}
}

func (mp *metaPartition) AppendXAttr(req *proto.AppendXAttrRequest, p *Packet) (err error) {
	var response = &proto.AppendXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Attrs:       make(map[string]string),
	}
	mp.xattrLock.Lock()
	defer mp.xattrLock.Unlock()
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		for i, _ := range req.Keys {
			if value, exist := extend.Get(req.Keys[i]); exist {
				newValue := append(value, req.Values[i]...)
				extend.Put(req.Keys[i], newValue)
				response.Attrs[string(req.Keys[i])] = string(newValue)
				if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
					p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
					return
				}

			} else {
				extend.Put(req.Keys[i], req.Values[i])
				response.Attrs[string(req.Keys[i])] = string(req.Values[i])
				if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
					p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
					return
				}
			}

		}
	} else {
		var extend = NewExtend(req.Inode)
		for i, _ := range req.Keys {
			extend.Put(req.Keys[i], req.Values[i])
			response.Attrs[string(req.Keys[i])] = string(req.Values[i])
			if _, err = mp.putExtend(opFSMUpdateXAttr, extend); err != nil {
				p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
				return
			}
		}
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) SetXAttr(req *proto.SetXAttrRequest, p *Packet) (err error) {
	var extend = NewExtend(req.Inode)
	extend.Put([]byte(req.Key), []byte(req.Value))
	if _, err = mp.putExtend(opFSMSetXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) BatchSetXAttr(req *proto.BatchSetXAttrRequest, p *Packet) (err error) {
	var extend = NewExtend(req.Inode)
	for key, val := range req.Attrs {
		extend.Put([]byte(key), []byte(val))
	}

	if _, err = mp.putExtend(opFSMSetXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) GetXAttr(req *proto.GetXAttrRequest, p *Packet) (err error) {
	var response = &proto.GetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Key:         req.Key,
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		if value, exist := extend.Get([]byte(req.Key)); exist {
			response.Value = string(value)
		}
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) GetAllXAttr(req *proto.GetAllXAttrRequest, p *Packet) (err error) {
	var response = &proto.GetAllXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		Attrs:       make(map[string]string),
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		for key, val := range extend.dataMap {
			response.Attrs[key] = string(val)
		}
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) BatchGetXAttr(req *proto.BatchGetXAttrRequest, p *Packet) (err error) {
	var response = &proto.BatchGetXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		XAttrs:      make([]*proto.XAttrInfo, 0, len(req.Inodes)),
	}
	for _, inode := range req.Inodes {
		treeItem := mp.extendTree.Get(NewExtend(inode))
		if treeItem != nil {
			extend := treeItem.(*Extend)
			info := &proto.XAttrInfo{
				Inode:  inode,
				XAttrs: make(map[string]string),
			}
			for _, key := range req.Keys {
				if val, exist := extend.Get([]byte(key)); exist {
					info.XAttrs[key] = string(val)
				}
			}
			response.XAttrs = append(response.XAttrs, info)
		}
	}
	var encoded []byte
	if encoded, err = json.Marshal(response); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) RemoveXAttr(req *proto.RemoveXAttrRequest, p *Packet) (err error) {
	var extend = NewExtend(req.Inode)
	extend.Put([]byte(req.Key), nil)
	if _, err = mp.putExtend(opFSMRemoveXAttr, extend); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkReply()
	return
}

func (mp *metaPartition) ListXAttr(req *proto.ListXAttrRequest, p *Packet) (err error) {
	var response = &proto.ListXAttrResponse{
		VolName:     req.VolName,
		PartitionId: req.PartitionId,
		Inode:       req.Inode,
		XAttrs:      make([]string, 0),
	}
	treeItem := mp.extendTree.Get(NewExtend(req.Inode))
	if treeItem != nil {
		extend := treeItem.(*Extend)
		extend.Range(func(key, value []byte) bool {
			response.XAttrs = append(response.XAttrs, string(key))
			return true
		})
	}
	var encoded []byte
	encoded, err = json.Marshal(response)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}
	p.PacketOkWithBody(encoded)
	return
}

func (mp *metaPartition) putExtend(op uint32, extend *Extend) (resp interface{}, err error) {
	var marshaled []byte
	if marshaled, err = extend.Bytes(); err != nil {
		return
	}
	resp, err = mp.submit(op, marshaled)
	return
}

func (mp *metaPartition) addExtendParentIno(inode, parentIno uint64) {
	var extend = NewExtend(inode)
	var data map[uint64]int
	var e *Extend

	if parentIno == 0 {
		log.LogWarnf("addExtendParentIno Inode [%v] Parent [%v].", inode, parentIno)
		return
	}

	treeItem := mp.extendTree.CopyGet(extend)
	if treeItem == nil {
		mp.extendTree.ReplaceOrInsert(extend, true)
	} else {
		e = treeItem.(*Extend)
		e.mu.Lock()
		defer e.mu.Unlock()
		json.Unmarshal(e.dataMap[proto.ParentKey], &data)
		data[parentIno] += 1
		e.dataMap[proto.ParentKey], _ = json.Marshal(data)
	}

	log.LogInfof("AddParentIno Inode [%v] Parent [%v] success.", inode, parentIno)
	return
}

func (mp *metaPartition) delExtendParentIno(inode, parentIno uint64) (err error) {
	var data map[uint64]int
	if parentIno == 0 {
		log.LogWarnf("delExtendParentIno Inode [%v] Parent [%v].", inode, parentIno)
		return nil
	}
	treeItem := mp.extendTree.CopyGet(NewExtend(inode))
	if treeItem == nil {
		return fmt.Errorf("inode [%v] extend not found", inode)
	}
	e := treeItem.(*Extend)
	e.mu.Lock()
	defer e.mu.Unlock()
	err = json.Unmarshal(e.dataMap[proto.ParentKey], &data)
	if err != nil {
		return
	}
	if data[parentIno] > 1 {
		data[parentIno] -= 1
	} else {
		delete(data, parentIno)
	}
	e.dataMap["parent"], err = json.Marshal(data)
	return
}
