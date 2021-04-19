package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	FormatCollectionName = "hkg_metatable_format"
)

type Pattern struct {
	From []string `bason:"from" yaml:"from"`
	To   string `bason:"to" yaml:"to"`
}

type Format struct {
	ID    string  `bson:"_id"`
	Name  string  `bason:"name" yaml:"name"`
	Pattern []Pattern `bason:"pattern" yaml:"pattern"`
}

type FormatDAO struct {
	conn *Conn
}

func NewFormatDAO(_conn *Conn) *FormatDAO {
	if nil == _conn {
		return &FormatDAO{
			conn: defaultConn,
		}
	} else {
		return &FormatDAO{
			conn: _conn,
		}
	}
}

func (this *FormatDAO) UpsertOne(_format *Format) (_err error) {
	_err = nil

	ctx, cancel := NewContext()
	defer cancel()

    filter := bson.D{{"_id", _format.ID}}
    update := bson.D {
        {"$set", bson.D{
            {"name", _format.Name},
            {"pattern", _format.Pattern},
        }},
    }

    upsert := true
    options := &options.UpdateOptions{
        Upsert: &upsert,
    }

    _, err := this.conn.DB.Collection(FormatCollectionName).UpdateOne(ctx, filter, update, options)
	_err = err
	return
}

func (this *FormatDAO) Count() (int64, error) {
	ctx, cancel := NewContext()
	defer cancel()
	count, err := this.conn.DB.Collection(FormatCollectionName).EstimatedDocumentCount(ctx)
	return count, err
}

func (this *FormatDAO) List(_offset int64, _count int64) ([]*Format, error) {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{}
	// 1: 倒叙  -1：正序
	sort := bson.D{{"name", -1}}

	findOptions := options.Find()
	findOptions.SetSort(sort)
	findOptions.SetSkip(_offset)
	findOptions.SetLimit(_count)

	cur, err := this.conn.DB.Collection(FormatCollectionName).Find(ctx, filter, findOptions)
	if nil != err {
		return make([]*Format, 0), err
	}
	defer cur.Close(ctx)

	var ary []*Format
	for cur.Next(ctx) {
		var format Format
		err = cur.Decode(&format)
		if nil != err {
			return make([]*Format, 0), err
		}
		ary = append(ary, &format)
	}
	return ary, nil
}

func (this *FormatDAO) UpdateOne(_format *Format) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"name", _format.Name}}
	update := bson.D{
		{"$set", bson.D{
			{"merge", _format.Pattern},
		}},
	}
	_, err := this.conn.DB.Collection(FormatCollectionName).UpdateOne(ctx, filter, update)
	return err
}

func (this *FormatDAO) DeleteOne(_id string) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"_id", _id}}
	_, err := this.conn.DB.Collection(FormatCollectionName).DeleteOne(ctx, filter)
	return err
}
