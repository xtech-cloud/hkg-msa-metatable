package handler

import (
	"context"
	"hkg-msa-metatable/model"

	"github.com/micro/go-micro/v2/logger"

	proto "github.com/xtech-cloud/hkg-msp-metatable/proto/metatable"
	yaml "gopkg.in/yaml.v2"
)

type Format struct{}

func (this *Format) ImportYaml(_ctx context.Context, _req *proto.ImportYamlRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Format.ImportYaml, req is %v", _req)

	_rsp.Status = &proto.Status{}

	if "" == _req.Content {
		_rsp.Status.Code = 1
		_rsp.Status.Message = "content is required"
		return nil
	}

	format := &model.Format{}
	err := yaml.Unmarshal([]byte(_req.Content), format)
	if nil != err {
		_rsp.Status.Code = -1
		_rsp.Status.Message = err.Error()
		return nil
	}

	dao := model.NewFormatDAO(nil)
	format.ID = model.ToUUID(format.Name)
	err = dao.UpsertOne(format)
	return err
}

func (this *Format) List(_ctx context.Context, _req *proto.ListRequest, _rsp *proto.FormatListResponse) error {
	logger.Infof("Received Format.List, req is %v", _req)

	_rsp.Status = &proto.Status{}
	offset := int64(0)
	if _req.Offset > 0 {
		offset = _req.Offset
	}

	count := int64(50)
	if _req.Count > 0 {
		count = _req.Count
	}

	dao := model.NewFormatDAO(nil)
	total, err := dao.Count()
	if nil != err {
		return err
	}
	_rsp.Total = total

	ary, err := dao.List(offset, count)
	if nil != err {
		return err
	}

	_rsp.Entity = make([]*proto.FormatEntity, len(ary))
	for i, v := range ary {
		_rsp.Entity[i] = &proto.FormatEntity{
			Uuid: v.ID,
			Name: v.Name,
		}
        _rsp.Entity[i].Pattern = make([]*proto.PatternEntity, len(v.Pattern))
        for j, m := range v.Pattern {
            _rsp.Entity[i].Pattern[j] = &proto.PatternEntity {
                From: m.From,
                To: m.To,
            }
        }
	}
	return nil
}

func (this *Format) Delete(_ctx context.Context, _req *proto.DeleteRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Format.Delete, req is %v", _req)

	_rsp.Status = &proto.Status{}

	if "" == _req.Uuid {
		_rsp.Status.Code = 1
		_rsp.Status.Message = "uuid is required"
		return nil
	}

	dao := model.NewFormatDAO(nil)
	err := dao.DeleteOne(_req.Uuid)
	return err
}
