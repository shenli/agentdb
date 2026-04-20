package client

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	Client *mongo.Client
}

func NewMongo(ctx context.Context, uri string) (*Mongo, error) {
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return &Mongo{Client: cli}, nil
}

func (m *Mongo) Search(ctx context.Context, tenantID, query string) error {
	coll := m.Client.Database(tenantID).Collection("events")
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$search", Value: bson.D{
			{Key: "text", Value: bson.D{
				{Key: "query", Value: query},
				{Key: "path", Value: "payload.text"},
			}},
		}}},
	}
	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("aggregate: %w", err)
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
	}
	return nil
}
