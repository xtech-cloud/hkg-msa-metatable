package handler

import (
	"context"
	"hkg-msa-metatable/model"

	"github.com/micro/go-micro/v2/logger"

	proto "github.com/xtech-cloud/hkg-msp-metatable/proto/metatable"
	yaml "gopkg.in/yaml.v2"
)

type Vocabulary struct{}

func (this *Vocabulary) ImportYaml(_ctx context.Context, _req *proto.ImportYamlRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Vocabulary.ImportYaml, req is %v", _req)

	_rsp.Status = &proto.Status{}

	if "" == _req.Content {
		_rsp.Status.Code = 1
		_rsp.Status.Message = "content is required"
		return nil
	}

	vocabulary := &model.Vocabulary{}
	err := yaml.Unmarshal([]byte(_req.Content), vocabulary)
	if nil != err {
		_rsp.Status.Code = -1
		_rsp.Status.Message = err.Error()
		return nil
	}

	dao := model.NewVocabularyDAO(nil)
    vocabulary.ID = model.ToUUID(vocabulary.Name)
	err = dao.UpsertOne(vocabulary)
	return err
}

func (this *Vocabulary) List(_ctx context.Context, _req *proto.ListRequest, _rsp *proto.VocabularyListResponse) error {
	logger.Infof("Received Vocabulary.List, req is %v", _req)

	_rsp.Status = &proto.Status{}
	offset := int64(0)
	if _req.Offset > 0 {
		offset = _req.Offset
	}

	count := int64(50)
	if _req.Count > 0 {
		count = _req.Count
	}

	dao := model.NewVocabularyDAO(nil)
	total, err := dao.Count()
	if nil != err {
		return err
	}
	_rsp.Total = total

	vocabularyAry, err := dao.List(offset, count)
	if nil != err {
		return err
	}

	_rsp.Entity = make([]*proto.VocabularyEntity, len(vocabularyAry))
	for i, v := range vocabularyAry {
		_rsp.Entity[i] = &proto.VocabularyEntity{
			Uuid:  v.ID,
			Name:  v.Name,
			Label: v.Label,
            Value: v.Value,
		}
	}
	return nil
}

func (this *Vocabulary) Find(_ctx context.Context, _req *proto.FindRequest, _rsp *proto.VocabularyFindResponse) error {
	logger.Infof("Received Vocabulary.Find, req is %v", _req)

	_rsp.Status = &proto.Status{}
	return nil
}

func (this *Vocabulary) Delete(_ctx context.Context, _req *proto.DeleteRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Vocabulary.Delete, req is %v", _req)

	_rsp.Status = &proto.Status{}

	if "" == _req.Uuid {
		_rsp.Status.Code = 1
		_rsp.Status.Message = "uuid is required"
		return nil
	}

	dao := model.NewVocabularyDAO(nil)
	err := dao.DeleteOne(_req.Uuid)
	return err
}
