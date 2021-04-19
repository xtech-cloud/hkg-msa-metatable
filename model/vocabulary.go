package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	VocabularyCollectionName = "hkg_metatable_vocabulary"
)

type Vocabulary struct {
	ID     string   `bson:"_id"`
	Name   string   `bason:"name" yaml:"name"`
	Label  []string `bason:"label" yaml:"label"`
	Value  []string `bason:"value" yaml:"value"`
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

func (this *VocabularyDAO) UpsertOne(_vocabulary *Vocabulary) (_err error) {
	_err = nil

	ctx, cancel := NewContext()
	defer cancel()

    filter := bson.D{{"_id", _vocabulary.ID}}
    update := bson.D {
        {"$set", bson.D{
            {"name", _vocabulary.Name},
            {"label", _vocabulary.Label},
            {"value", _vocabulary.Value},
        }},
    }

    upsert := true
    options := &options.UpdateOptions{
        Upsert: &upsert,
    }

    _, err := this.conn.DB.Collection(VocabularyCollectionName).UpdateOne(ctx, filter, update, options)
	_err = err
	return
}

func (this *VocabularyDAO) Count() (int64, error) {
	ctx, cancel := NewContext()
	defer cancel()
	count, err := this.conn.DB.Collection(VocabularyCollectionName).EstimatedDocumentCount(ctx)
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

	cur, err := this.conn.DB.Collection(VocabularyCollectionName).Find(ctx, filter, findOptions)
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
			{"label", _vocabulary.Label},
			{"value", _vocabulary.Value},
		}},
	}
	_, err := this.conn.DB.Collection(VocabularyCollectionName).UpdateOne(ctx, filter, update)
	return err
}

func (this *VocabularyDAO) DeleteOne(_id string) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"_id", _id}}
	_, err := this.conn.DB.Collection(VocabularyCollectionName).DeleteOne(ctx, filter)
	return err
}
