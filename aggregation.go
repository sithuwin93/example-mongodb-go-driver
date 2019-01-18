package main

import (
	"context"
	"fmt"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"log"
	"time"
)

type ItemOneToMany struct {
	ID  string `bson:"_id"`
	Name string
	Reviews []struct{
		ID string `bson:"_id"`
		Name string
		Msg string
		UserId string `bson:"userId"`
		ProductId string
	} `bson:"reviews"`
}

type ItemManyToOne struct {
	ID  string `bson:"_id"`
	UserId string `bson:"userId"`
	Name string
	Msg string
	ProductId string
	Product []struct{
		ID string `bson:"_id"`
		Name string
	} `bson:"product"`
}

type ItemOneToOne struct {
	ID  string `bson:"_id"`
	Name string
	CitizenId string `bson:"citizenId"`
	PassportId string `bson:"passportId"`
	Passport []struct{
		ID string `bson:"_id"`
		PassportId string
		Name string
		Expired string
	} `bson:"passport"`
}

type ItemOneToOneMerge struct {
	ID  string `bson:"_id"`
	PassportId string `bson:"passportId"`
	Name string
	Expired string
	CitizenId string `bson:"citizenId"`
}

type ItemOneToSubObject struct {
	ID  string `bson:"_id"`
	Name string
	CitizenId string `bson:"citizenId"`
	PassportId string `bson:"passportId"`
	Passport struct{
		ID  string `bson:"_id"`
		PassportId string `bson:"passportId"`
		Name string
		Expired string
	}
}

type ItemManyToMany struct {
	ID  string `bson:"_id"`
	ProductId string `bson:"productId"`
	UserId string `bson:"userId"`
	Price int
	User []struct{
		ID  string `bson:"_id"`
		Name string
	}
	Product []struct{
		ID  string `bson:"_id"`
		Name string
	}
}

type OneMillion struct {
	ID    int64
	Name  string
	Email string
	Color string
	Time  int64
	Comments []struct {
		ID     int
		Msg    string
		Time   int64
		UserID int
	}
}

func OneToMany(client *mongo.Client) {
	database := client.Database("mongo4")
	collection := database.Collection("product")

	pipeline := bson.NewArray(
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements(
				"$lookup",
				bson.EC.String("from","review"),
				bson.EC.String("localField", "_id"),
				bson.EC.String("foreignField", "productId"),
				bson.EC.String("as", "reviews"),
			),
		),
	)
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	itemRead := ItemOneToMany{}
	for cursor.Next(context.Background()) {
		err := cursor.Decode(&itemRead)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("itemRead = %v\n", itemRead)
	}
}

func ManyToOne(client *mongo.Client) {
	database := client.Database("mongo4")
	collection := database.Collection("review")

	pipeline := bson.NewArray(
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements(
				"$lookup",
				bson.EC.String("from","product"),
				bson.EC.String("localField", "productId"),
				bson.EC.String("foreignField", "_id"),
				bson.EC.String("as", "product"),
			),
		),
	)
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	itemRead := ItemManyToOne{}
	for cursor.Next(context.Background()) {
		err := cursor.Decode(&itemRead)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("itemRead = %v\n", itemRead)
	}
}

func OneToOne(client *mongo.Client) {
	database := client.Database("mongo4")
	collection := database.Collection("person")

	pipeline := bson.NewArray(
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements(
				"$lookup",
				bson.EC.String("from","passport"),
				bson.EC.String("localField", "passportId"),
				bson.EC.String("foreignField", "passportId"),
				bson.EC.String("as", "passport"),
			),
		),
	)
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	itemRead := ItemOneToOne{}
	for cursor.Next(context.Background()) {
		err := cursor.Decode(&itemRead)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("itemRead = %v\n", itemRead)
	}
}

func OneToOneMergeObject(client *mongo.Client) {
	database := client.Database("mongo4")
	collection := database.Collection("person")

	pipeline := bson.NewArray(
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$lookup",
				bson.EC.String("from","passport"),
				bson.EC.String("localField", "passportId"),
				bson.EC.String("foreignField", "passportId"),
				bson.EC.String("as", "passport"),
			),
		),
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$replaceRoot",
				bson.EC.SubDocumentFromElements("newRoot",
					bson.EC.ArrayFromElements("$mergeObjects",
						bson.VC.DocumentFromElements(
							bson.EC.ArrayFromElements("$arrayElemAt",
								bson.VC.String("$passport"),
								bson.VC.Int32(0),
							),
						),
						bson.VC.String("$$ROOT"),
					),
				),
			),
		),
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$project",
				bson.EC.Int32("passport", 0),
			),
		),
	)
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	itemRead := ItemOneToOneMerge{}
	for cursor.Next(context.Background()) {
		err := cursor.Decode(&itemRead)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("itemRead = %v\n", itemRead)
	}
}

func OneToOneSubObject(client *mongo.Client) {
	database := client.Database("mongo4")
	collection := database.Collection("person")

	pipeline := bson.NewArray(
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$lookup",
				bson.EC.String("from","passport"),
				bson.EC.String("localField", "passportId"),
				bson.EC.String("foreignField", "passportId"),
				bson.EC.String("as", "passports"),
			),
		),
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$addFields",
				bson.EC.SubDocumentFromElements("passport",
					bson.EC.ArrayFromElements("$arrayElemAt",
						bson.VC.String("$passports"),
						bson.VC.Int32(0),
					),
				),
			),
		),
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$project",
				bson.EC.Int32("passports", 0),
			),
		),
	)
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	itemRead := ItemOneToSubObject{}
	for cursor.Next(context.Background()) {
		err := cursor.Decode(&itemRead)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("itemRead = %v\n", itemRead)
	}
}

func ManyToMany(client *mongo.Client) {
	database := client.Database("mongo4")
	collection := database.Collection("cart")

	pipeline := bson.NewArray(
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$lookup",
				bson.EC.String("from","user"),
				bson.EC.String("localField", "userId"),
				bson.EC.String("foreignField", "_id"),
				bson.EC.String("as", "user"),
			),
		),
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$lookup",
				bson.EC.String("from","product"),
				bson.EC.String("localField", "productId"),
				bson.EC.String("foreignField", "_id"),
				bson.EC.String("as", "product"),
			),
		),
	)
	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	itemRead := ItemManyToMany{}
	for cursor.Next(context.Background()) {
		err := cursor.Decode(&itemRead)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("itemRead = %v\n", itemRead)
	}
}

func LookupOneMillion(client *mongo.Client) {
	database := client.Database("join")
	collection := database.Collection("user")

	pipeline := bson.NewArray(
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$match",
				bson.EC.Int64("id", 700000),
			),
		),
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements("$lookup",
				bson.EC.String("from","comment"),
				bson.EC.String("localField", "id"),
				bson.EC.String("foreignField", "userID"),
				bson.EC.String("as", "comments"),
			),
		),
	)
	_, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	//itemRead := OneMillion{}
	//for cursor.Next(context.Background()) {
	//	err := cursor.Decode(&itemRead)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	fmt.Printf("itemRead = %v\n", itemRead)
	//}
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

	//OneToOne(client)
	//OneToOneMergeObject(client)
	//OneToOneSubObject(client)
	//OneToMany(client)
	//ManyToOne(client)
	//ManyToMany(client)

	// (Location4568) Total size of documents in comment matching pipeline
	// 80B / 16MB = 200000
	t := time.Now()
	LookupOneMillion(client)
	fmt.Println(time.Since(t))
}
