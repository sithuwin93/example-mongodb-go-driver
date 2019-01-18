package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"log"
)

func ConnectMysql() *sql.DB {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/")
	if err != nil {
		panic(err)
	}
	return db
}

func InitalMysql() {
	db := ConnectMysql()
	name := "benchmark"

	// Create database
	_, err := db.Exec("CREATE DATABASE " + name)
	if err != nil {
		fmt.Println(err)
	}
	_, err = db.Exec("USE " + name)
	if err != nil {
		panic(err)
	}

	// Create table
	createPerson := `
		CREATE TABLE person (
  			id int NOT NULL AUTO_INCREMENT,
 	    	firstname VARCHAR(20),
    		lastname VARCHAR(20),
			color VARCHAR(15),
			email VARCHAR(100),
			phone VARCHAR(20),
			timestamp int,
			primary key (id)
		);
	`
	_, err = db.Exec(createPerson)
	if err != nil {
		panic(err)
	}
	createReview := `
		CREATE TABLE review (
  			id int NOT NULL AUTO_INCREMENT,
 	    	name VARCHAR(40),
    		color VARCHAR(20),
			email VARCHAR(100),
			timestamp int,
			primary key (id)
		);
	`
	_, err = db.Exec(createReview)
	if err != nil {
		panic(err)
	}

	// Create index
	_, err = db.Exec("CREATE INDEX colorPerson on person(color)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE INDEX colorReview on review(color)")
	if err != nil {
		panic(err)
	}
}

func ConnectMongo() *mongo.Client {
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017/")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func InitalMongo() {
	client := ConnectMongo()

	// Create empty collection person & add index color
	person := client.Database("benchmark").Collection("person")
	person.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{bson.NewDocument(bson.EC.Int32("color", 1)),
		nil,
	})
	_, err := person.InsertOne(
		context.Background(),
		bson.NewDocument(
			bson.EC.String("id", "create"),
		))
	if err != nil {
		panic(err)
	}
	_, err = person.DeleteOne(
		context.Background(),
		bson.NewDocument(
			bson.EC.String("id", "create"),
		),
	)
	if err != nil {
		panic(err)
	}

	// Create empty collection review & add index color
	review := client.Database("benchmark").Collection("review")
	review.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{bson.NewDocument(bson.EC.Int32("color", 1)),
		nil,
	})
	_, err = review.InsertOne(
		context.Background(),
		bson.NewDocument(
			bson.EC.String("id", "create"),
		))
	if err != nil {
		panic(err)
	}
	_, err = review.DeleteOne(
		context.Background(),
		bson.NewDocument(
			bson.EC.String("id", "create"),
		),
	)
	if err != nil {
		panic(err)
	}
}

func DropMysql() {
	db := ConnectMysql()
	_, err := db.Exec("USE benchmark")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("DROP TABLE person")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("DROP TABLE review")
	if err != nil {
		panic(err)
	}
}

func DropMongo() {
	client := ConnectMongo()
	person := client.Database("benchmark").Collection("person")
	review := client.Database("benchmark").Collection("review")
	person.Drop(context.Background())
	review.Drop(context.Background())
}

//func main() {
//	//DropMysql()
//	//InitalMysql()
//	//DropMongo()
//	InitalMongo()
//}
