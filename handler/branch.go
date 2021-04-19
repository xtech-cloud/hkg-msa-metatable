package handler

import (
	"context"
	"hkg-msa-metatable/model"

	"github.com/micro/go-micro/v2/logger"

	proto "github.com/xtech-cloud/hkg-msp-metatable/proto/metatable"
	yaml "gopkg.in/yaml.v2"
)

type Branch struct{}

func (this *Branch) ImportYaml(_ctx context.Context, _req *proto.ImportYamlRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Branch.ImportYaml, req is %v", _req)

	_rsp.Status = &proto.Status{}

	if "" == _req.Content {
		_rsp.Status.Code = 1
		_rsp.Status.Message = "content is required"
		return nil
	}

	branch := &model.Branch{}
	err := yaml.Unmarshal([]byte(_req.Content), branch)
	if nil != err {
		_rsp.Status.Code = -1
		_rsp.Status.Message = err.Error()
		return nil
	}

	dao := model.NewBranchDAO(nil)
	branch.ID = model.ToUUID(branch.Name)
	err = dao.UpsertOne(branch)
	return err
}

func (this *Branch) List(_ctx context.Context, _req *proto.ListRequest, _rsp *proto.BranchListResponse) error {
	logger.Infof("Received Branch.List, req is %v", _req)

	_rsp.Status = &proto.Status{}
	offset := int64(0)
	if _req.Offset > 0 {
		offset = _req.Offset
	}

	count := int64(50)
	if _req.Count > 0 {
		count = _req.Count
	}

	dao := model.NewBranchDAO(nil)
	total, err := dao.Count()
	if nil != err {
		return err
	}
	_rsp.Total = total

	ary, err := dao.List(offset, count)
	if nil != err {
		return err
	}

	_rsp.Entity = make([]*proto.BranchEntity, len(ary))
	for i, v := range ary {
		_rsp.Entity[i] = &proto.BranchEntity{
			Uuid: v.ID,
			Name: v.Name,
		}
        _rsp.Entity[i].Merge = make([]*proto.MergeEntity, len(v.Merge))
        for j, m := range v.Merge {
            _rsp.Entity[i].Merge[j] = &proto.MergeEntity {
                From: m.From,
                To: m.To,
            }
        }
	}
	return nil
}

func (this *Branch) Delete(_ctx context.Context, _req *proto.DeleteRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Branch.Delete, req is %v", _req)

	_rsp.Status = &proto.Status{}

	if "" == _req.Uuid {
		_rsp.Status.Code = 1
		_rsp.Status.Message = "uuid is required"
		return nil
	}

	dao := model.NewBranchDAO(nil)
	err := dao.DeleteOne(_req.Uuid)
	return err
}
