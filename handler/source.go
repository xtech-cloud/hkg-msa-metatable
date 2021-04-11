package handler

import (
	"context"
	"hkg-msa-metatable/model"

	"github.com/micro/go-micro/v2/logger"

	proto "github.com/xtech-cloud/hkg-msp-metatable/proto/metatable"
	yaml "gopkg.in/yaml.v2"
)

type Source struct{}

type SourceYaml struct {
	Name       string `yaml:"name"`
	Address    string `yaml:"address"`
	Expression string `yaml:"expression"`
}

func (this *Source) ImportYaml(_ctx context.Context, _req *proto.ImportYamlRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Source.ImportYaml, req is %v", _req)

	_rsp.Status = &proto.Status{}

	yamlSource := &SourceYaml{}
	err := yaml.Unmarshal([]byte(_req.Content), yamlSource)
	if nil != err {
		_rsp.Status.Code = -1
		_rsp.Status.Message = err.Error()
		return nil
	}

	dao := model.NewSourceDAO(nil)
	source := &model.Source{
		ID:         model.ToUUID(yamlSource.Name),
		Name:       yamlSource.Name,
		Address:    yamlSource.Address,
		Expression: yamlSource.Expression,
	}
	err = dao.InsertOne(source)
	return err
}

func (this *Source) List(_ctx context.Context, _req *proto.ListRequest, _rsp *proto.SourceListResponse) error {
	logger.Infof("Received Source.List, req is %v", _req)

	_rsp.Status = &proto.Status{}
	offset := int64(0)
	if _req.Offset > 0 {
		offset = _req.Offset
	}

	count := int64(50)
	if _req.Count > 0 {
		count = _req.Count
	}

	dao := model.NewSourceDAO(nil)
	total, err := dao.Count()
	if nil != err {
		return err
	}
	_rsp.Total = total

	ary, err := dao.List(offset, count)
	if nil != err {
		return err
	}

	_rsp.Entity = make([]*proto.SourceEntity, len(ary))
	for i, v := range ary {
		_rsp.Entity[i] = &proto.SourceEntity{
			Name:         v.Name,
			Address:      v.Address,
			Expression: v.Expression,
		}
	}
	return nil
}
