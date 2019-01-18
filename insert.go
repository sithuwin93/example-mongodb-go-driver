package main

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"log"
)

func main() {
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	user := client.Database("mongo4").Collection("user")
	result, err := user.InsertOne(
		context.Background(),
		bson.NewDocument(
			bson.EC.String("item", "canvas"),
			bson.EC.Int32("qty", 100),
			bson.EC.ArrayFromElements("tags",
				bson.VC.String("cotton"),
			),
			bson.EC.SubDocumentFromElements("size",
				bson.EC.Int32("h", 28),
				bson.EC.Double("w", 35.5),
				bson.EC.String("uom", "cm"),
			),
		))
	fmt.Println(result)
}