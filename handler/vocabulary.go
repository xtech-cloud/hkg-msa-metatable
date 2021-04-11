package handler

import (
	"context"
	"hkg-msa-metatable/model"

	"github.com/micro/go-micro/v2/logger"

	proto "github.com/xtech-cloud/hkg-msp-metatable/proto/metatable"
	yaml "gopkg.in/yaml.v2"
)

type Vocabulary struct{}

type VocabularyYaml struct {
	Labels   []string `yaml:"labels"`
	Values []string `yaml:"values"`
}

func (this *Vocabulary) ImportYaml(_ctx context.Context, _req *proto.ImportYamlRequest, _rsp *proto.BlankResponse) error {
	logger.Infof("Received Vocabulary.ImportYaml, req is %v", _req)

	_rsp.Status = &proto.Status{}

	yamlVocabulary := &VocabularyYaml{}
	err := yaml.Unmarshal([]byte(_req.Content), yamlVocabulary)
	if nil != err {
		_rsp.Status.Code = -1
		_rsp.Status.Message = err.Error()
		return nil
	}

	dao := model.NewVocabularyDAO(nil)
	vocabularyAry := make([]*model.Vocabulary, len(yamlVocabulary.Values))
	for i, value := range yamlVocabulary.Values {
        uid := value 
        for _, label:= range yamlVocabulary.Labels{
            uid += label
        }
		vocabularyAry[i] = &model.Vocabulary{
            ID: model.ToUUID(uid),
			Name: value,
			Label:  yamlVocabulary.Labels,
		}
	}
	err = dao.InsertMany(vocabularyAry)
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
			Name: v.Name,
			Label:  v.Label,
		}
	}
	return nil
}

func (this *Vocabulary) Find(_ctx context.Context, _req *proto.FindRequest, _rsp *proto.VocabularyFindResponse) error {
	logger.Infof("Received Vocabulary.Find, req is %v", _req)

	_rsp.Status = &proto.Status{}
	return nil
}
