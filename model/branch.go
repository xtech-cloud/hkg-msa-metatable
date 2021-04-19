package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	BranchCollectionName = "hkg_metatable_branch"
)

type Merge struct {
	From []string `bason:"from" yaml:"from"`
	To   string `bason:"to" yaml:"to"`
}

type Branch struct {
	ID    string  `bson:"_id"`
	Name  string  `bason:"name" yaml:"name"`
	Merge []Merge `bason:"merge" yaml:"merge"`
}

type BranchDAO struct {
	conn *Conn
}

func NewBranchDAO(_conn *Conn) *BranchDAO {
	if nil == _conn {
		return &BranchDAO{
			conn: defaultConn,
		}
	} else {
		return &BranchDAO{
			conn: _conn,
		}
	}
}

func (this *BranchDAO) UpsertOne(_branch *Branch) (_err error) {
	_err = nil

	ctx, cancel := NewContext()
	defer cancel()

    filter := bson.D{{"_id", _branch.ID}}
    update := bson.D {
        {"$set", bson.D{
            {"name", _branch.Name},
            {"merge", _branch.Merge},
        }},
    }

    upsert := true
    options := &options.UpdateOptions{
        Upsert: &upsert,
    }

    _, err := this.conn.DB.Collection(BranchCollectionName).UpdateOne(ctx, filter, update, options)
	_err = err
	return
}

func (this *BranchDAO) Count() (int64, error) {
	ctx, cancel := NewContext()
	defer cancel()
	count, err := this.conn.DB.Collection(BranchCollectionName).EstimatedDocumentCount(ctx)
	return count, err
}

func (this *BranchDAO) List(_offset int64, _count int64) ([]*Branch, error) {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{}
	// 1: 倒叙  -1：正序
	sort := bson.D{{"name", -1}}

	findOptions := options.Find()
	findOptions.SetSort(sort)
	findOptions.SetSkip(_offset)
	findOptions.SetLimit(_count)

	cur, err := this.conn.DB.Collection(BranchCollectionName).Find(ctx, filter, findOptions)
	if nil != err {
		return make([]*Branch, 0), err
	}
	defer cur.Close(ctx)

	var ary []*Branch
	for cur.Next(ctx) {
		var branch Branch
		err = cur.Decode(&branch)
		if nil != err {
			return make([]*Branch, 0), err
		}
		ary = append(ary, &branch)
	}
	return ary, nil
}

func (this *BranchDAO) UpdateOne(_branch *Branch) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"name", _branch.Name}}
	update := bson.D{
		{"$set", bson.D{
			{"merge", _branch.Merge},
		}},
	}
	_, err := this.conn.DB.Collection(SourceCollectionName).UpdateOne(ctx, filter, update)
	return err
}

func (this *BranchDAO) DeleteOne(_id string) error {
	ctx, cancel := NewContext()
	defer cancel()

	filter := bson.D{{"_id", _id}}
	_, err := this.conn.DB.Collection(BranchCollectionName).DeleteOne(ctx, filter)
	return err
}
