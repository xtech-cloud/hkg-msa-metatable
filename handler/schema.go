package handler

import (
	"context"
	"hkg-msa-metatable/model"

	"github.com/micro/go-micro/v2/logger"

	proto "github.com/xtech-cloud/hkg-msp-metatable/proto/metatable"
	yaml "gopkg.in/yaml.v2"
)

type Schema struct{}

func (this *Schema) ImportYaml(_ctx context.Context, _req *proto.ImportYamlRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Schema.ImportYaml, req is %v", _req)

	_rsp.Status = &proto.Status{}

	if "" == _req.Content {
		_rsp.Status.Code = 1
		_rsp.Status.Message = "content is required"
		return nil
	}

	schema := &model.Schema{}
	err := yaml.Unmarshal([]byte(_req.Content), schema)
	if nil != err {
		_rsp.Status.Code = -1
		_rsp.Status.Message = err.Error()
		return nil
	}

	schema.ID = model.ToUUID(schema.Name)
	dao := model.NewSchemaDAO(nil)
	err = dao.UpsertOne(schema)
	return err
}

func (this *Schema) List(_ctx context.Context, _req *proto.ListRequest, _rsp *proto.SchemaListResponse) error {
	logger.Infof("Received Schema.List, req is %v", _req)

	_rsp.Status = &proto.Status{}
	offset := int64(0)
	if _req.Offset > 0 {
		offset = _req.Offset
	}

	count := int64(50)
	if _req.Count > 0 {
		count = _req.Count
	}

	dao := model.NewSchemaDAO(nil)
	total, err := dao.Count()
	if nil != err {
		return err
	}
	_rsp.Total = total

	ary, err := dao.List(offset, count)
	if nil != err {
		return err
	}

	_rsp.Entity = make([]*proto.SchemaEntity, len(ary))
	for i, v := range ary {
		_rsp.Entity[i] = &proto.SchemaEntity{
			Uuid: v.ID,
			Name: v.Name,
		}
		_rsp.Entity[i].Rule = make([]*proto.RuleEntity, len(v.Rule))
		for j, rule := range v.Rule {
			_rsp.Entity[i].Rule[j] = &proto.RuleEntity{
				Name:    rule.Name,
				Field:   rule.Field,
				Type:    rule.Type,
				Element: rule.Element,
				Pair: &proto.PairEntity{
					Key:   rule.Pair.Key,
					Value: rule.Pair.Value,
				},
			}
		}
	}
	return nil
}

func (this *Schema) Delete(_ctx context.Context, _req *proto.DeleteRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Schema.Delete, req is %v", _req)

	_rsp.Status = &proto.Status{}

	if "" == _req.Uuid {
		_rsp.Status.Code = 1
		_rsp.Status.Message = "uuid is required"
		return nil
	}

	dao := model.NewSchemaDAO(nil)
	err := dao.DeleteOne(_req.Uuid)
	return err
}
