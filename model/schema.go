package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	SchemaCollectionName = "hkg_metatable_schema"
)

type Pair struct {
	Key   string `bason:"key" yaml:"key"`
	Value string `bason:"value" yaml:"value"`
}

type Rule struct {
	Name    string `bason:"name" yaml:"name"`
	Field   string `bason:"field" yaml:"field"`
	Type    string `bason:"type" yaml:"type"`
	Element string `bason:"element" yaml:"element"`
	Pair    Pair   `bason:"pair" yaml:"pair"`
}

type Schema struct {
	ID   string `bson:"_id"`
	Name string `bason:"name" yaml:"name"`
	Rule []Rule `bson:"rule" yaml:"rule"`
}

type SchemaDAO struct {
	conn *Conn
}

func NewSchemaDAO(_conn *Conn) *SchemaDAO {
	if nil == _conn {
		return &SchemaDAO{
			conn: defaultConn,
		}
	} else {
		return &SchemaDAO{
			conn: _conn,
		}
	}
}

func (this *SchemaDAO) InsertOne(_schema *Schema) (_err error) {
	_err = nil

	ctx, cancel := NewContext()
	defer cancel()

	document, err := bson.Marshal(_schema)
	if nil != err {
		_err = err
		return
	}

	_, err = this.conn.DB.Collection(SchemaCollectionName).InsertOne(ctx, document)
	if nil != err {
		// 忽略键重复的错误
		if mongo.IsDuplicateKeyError(err) {
			err = nil
		}
	}
	_err = err
	return
}

func (this *SchemaDAO) Count() (int64, error) {
	ctx, cancel := NewContext()
	defer cancel()
	count, err := this.conn.DB.Collection(SchemaCollectionName).EstimatedDocumentCount(ctx)
	return count, err
}

func (this *SchemaDAO) List(_offset int64, _count int64) ([]*Schema, error) {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{}
	// 1: 倒叙  -1：正序
	sort := bson.D{{"name", -1}}

	findOptions := options.Find()
	findOptions.SetSort(sort)
	findOptions.SetSkip(_offset)
	findOptions.SetLimit(_count)

	cur, err := this.conn.DB.Collection(SchemaCollectionName).Find(ctx, filter, findOptions)
	if nil != err {
		return make([]*Schema, 0), err
	}
	defer cur.Close(ctx)

	var ary []*Schema
	for cur.Next(ctx) {
		var schema Schema
		err = cur.Decode(&schema)
		if nil != err {
			return make([]*Schema, 0), err
		}
		ary = append(ary, &schema)
	}
	return ary, nil
}

func (this *SchemaDAO) UpdateOne(_schema *Schema) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"name", _schema.Name}}
	update := bson.D{
		{"$set", bson.D{
			{"rule", _schema.Rule},
		}},
	}
	_, err := this.conn.DB.Collection(SchemaCollectionName).UpdateOne(ctx, filter, update)
	return err
}

func (this *SchemaDAO) DeleteOne(_id string) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"_id", _id}}
	_, err := this.conn.DB.Collection(SchemaCollectionName).DeleteOne(ctx, filter)
	return err
}
