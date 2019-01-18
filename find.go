package main

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/bsoncodec"
	"github.com/mongodb/mongo-go-driver/mongo"
	"log"
)


// OID  objectid.ObjectID `bson:"_id,omitempty"` // omitempty not working
type User struct {
	OID  string `bson:"_id"`
	Name string
}

type Test struct {
	Item string
	Qty int
	Size struct{
		H int
	}
	Tags []string
}

func main() {
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	bsoncodec.Unmarshal(nil, nil)
	user := client.Database("mongo4").Collection("user")
	userData := Test{}
	bson.EC.
	err = user.FindOne(context.Background(), bson.NewDocument(bson.EC.String("item", "canvas"))).Decode(&userData)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("itemRead = %v\n", userData)
}
