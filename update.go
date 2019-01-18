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
	result, err := user.UpdateOne(
		context.Background(),
		bson.NewDocument(
			bson.EC.String("item", "canvas"),
		),
		bson.NewDocument(
			bson.EC.SubDocumentFromElements("$set",
				bson.EC.String("item", "nick"),
			),
		),
	)
	fmt.Printf("itemRead = %v\n", result)

}
