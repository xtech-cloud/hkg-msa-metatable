package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CollectionName = "hkg_msa_metatable_vocabulary"
)

type Vocabulary struct {
	ID   string   `bson:"_id"`
	Name string   `bason:"Name"`
	Tag  []string `bason:"Tag"`
}

type VocabularyDAO struct {
	conn *Conn
}

func NewVocabularyDAO(_conn *Conn) *VocabularyDAO {
	if nil == _conn {
		return &VocabularyDAO{
			conn: defaultConn,
		}
	} else {
		return &VocabularyDAO{
			conn: _conn,
		}
	}
}

func (this *VocabularyDAO) InsertMany(_vocabulary []*Vocabulary) (_err error) {
    _err = nil

	ctx, cancel := NewContext()
	defer cancel()

	documentAry := make([]interface{}, len(_vocabulary))
	for i, vocabulary := range _vocabulary {
		document, err := bson.Marshal(vocabulary)
		if nil != err {
            _err = err
			return
		}
		documentAry[i] = document
	}

	_, err := this.conn.DB.Collection(CollectionName).InsertMany(ctx, documentAry)
	if nil != err {
        // 忽略键重复的错误
        if mongo.IsDuplicateKeyError(err) {
            err = nil
        }
	}
    _err = err
	return
}

func (this *VocabularyDAO) FindOne(_name string) (*Vocabulary, error) {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"name", _name}}
	res := this.conn.DB.Collection(CollectionName).FindOne(ctx, filter)
	if res.Err() == mongo.ErrNoDocuments {
		return nil, nil
	}
	var vocabulary Vocabulary
	err := res.Decode(&vocabulary)
	return &vocabulary, err
}

func (this *VocabularyDAO) Count() (int64, error) {
	ctx, cancel := NewContext()
	defer cancel()
	count, err := this.conn.DB.Collection(CollectionName).EstimatedDocumentCount(ctx)
	return count, err
}

func (this *VocabularyDAO) List(_offset int64, _count int64) ([]*Vocabulary, error) {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{}
	// 1: 倒叙  -1：正序
	sort := bson.D{{"name", -1}}

	findOptions := options.Find()
	findOptions.SetSort(sort)
	findOptions.SetSkip(_offset)
	findOptions.SetLimit(_count)

	cur, err := this.conn.DB.Collection(CollectionName).Find(ctx, filter, findOptions)
	if nil != err {
		return make([]*Vocabulary, 0), err
	}
	defer cur.Close(ctx)

	var vocabularyAry []*Vocabulary
	for cur.Next(ctx) {
		var vocabulary Vocabulary
		err = cur.Decode(&vocabulary)
		if nil != err {
			return make([]*Vocabulary, 0), err
		}
		vocabularyAry = append(vocabularyAry, &vocabulary)
	}
	return vocabularyAry, nil
}

func (this *VocabularyDAO) UpdateOne(_vocabulary *Vocabulary) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"name", _vocabulary.Name}}
	update := bson.D{
		{"$set", bson.D{
			{"tag", _vocabulary.Tag},
		}},
	}
	_, err := this.conn.DB.Collection(CollectionName).UpdateOne(ctx, filter, update)
	if nil != err {
		return err
	}
	return nil
}
