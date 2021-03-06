package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	SourceCollectionName = "hkg_metatable_source"
)

type Source struct {
	ID         string `bson:"_id"`
	Name       string `bason:"name" yaml:"name"`
	Address    string `bason:"address" yaml:"address"`
	Expression string `bason:"expression" yaml:"expression"`
	Attribute  string `bason:"attribute" yaml:"attribute"`
}

type SourceDAO struct {
	conn *Conn
}

func NewSourceDAO(_conn *Conn) *SourceDAO {
	if nil == _conn {
		return &SourceDAO{
			conn: defaultConn,
		}
	} else {
		return &SourceDAO{
			conn: _conn,
		}
	}
}

func (this *SourceDAO) UpsertOne(_source *Source) (_err error) {
	_err = nil

	ctx, cancel := NewContext()
	defer cancel()

    filter := bson.D{{"_id", _source.ID}}
    update := bson.D {
        {"$set", bson.D{
            {"name", _source.Name},
            {"address", _source.Address},
            {"expression", _source.Expression},
            {"attribute", _source.Attribute},
        }},
    }

    upsert := true
    options := &options.UpdateOptions{
        Upsert: &upsert,
    }

    _, err := this.conn.DB.Collection(SourceCollectionName).UpdateOne(ctx, filter, update, options)
	_err = err
	return
}

func (this *SourceDAO) Count() (int64, error) {
	ctx, cancel := NewContext()
	defer cancel()
	count, err := this.conn.DB.Collection(SourceCollectionName).EstimatedDocumentCount(ctx)
	return count, err
}

func (this *SourceDAO) List(_offset int64, _count int64) ([]*Source, error) {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{}
	// 1: 倒叙  -1：正序
	sort := bson.D{{"name", -1}}

	findOptions := options.Find()
	findOptions.SetSort(sort)
	findOptions.SetSkip(_offset)
	findOptions.SetLimit(_count)

	cur, err := this.conn.DB.Collection(SourceCollectionName).Find(ctx, filter, findOptions)
	if nil != err {
		return make([]*Source, 0), err
	}
	defer cur.Close(ctx)

	var ary []*Source
	for cur.Next(ctx) {
		var source Source
		err = cur.Decode(&source)
		if nil != err {
			return make([]*Source, 0), err
		}
		ary = append(ary, &source)
	}
	return ary, nil
}

func (this *SourceDAO) UpdateOne(_source *Source) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"name", _source.Name}}
	update := bson.D{
		{"$set", bson.D{
			{"address", _source.Address},
			{"expression", _source.Expression},
			{"attribute", _source.Attribute},
		}},
	}
	_, err := this.conn.DB.Collection(SourceCollectionName).UpdateOne(ctx, filter, update)
	return err
}

func (this *SourceDAO) DeleteOne(_id string) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"_id", _id}}
	_, err := this.conn.DB.Collection(SourceCollectionName).DeleteOne(ctx, filter)
	return err
}
