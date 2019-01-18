package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"log"
	"testing"
	"time"
)

// mysql

func BenchmarkSelect(b *testing.B) {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/benchmark")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = db.Exec("SELECT * FROM person where color = 'Pink';")
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/benchmark")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	t := time.Now()
	_, err = db.Exec("UPDATE person SET color = 'Change' WHERE color = 'Goldenrod';")
	if err != nil {
		panic(err)
	}
	fmt.Println(time.Since(t))
}

func BenchmarkInsert(b *testing.B) {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/benchmark")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO person (firstname, lastname, color, email, phone, timestamp) VALUES (%q, %q, %q, %q, %q, %d);",
			"to", "nqmt", "pissnk", "to@to.com", "000", time.Now().Unix()))
	}
}

func BenchmarkDelete(b *testing.B) {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/benchmark")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	t := time.Now()
	_, err = db.Exec("DELETE FROM person WHERE color = 'pissnk'")
	if err != nil {
		panic(err)
	}
	fmt.Println(time.Since(t))
}

func BenchmarkJoin(b *testing.B) {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/benchmark")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = db.Exec("SELECT * FROM person inner join review on person.color = review.color where person.color = 'join';")
		if err != nil {
			panic(err)
		}
	}
}

// mongo

func BenchmarkFind(b *testing.B) {
	type Person struct {
		Firstname string
		Lastname  string
		Color     string
		Email     string
		Phone     string
		Timestamp int64
	}
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017/")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	p := client.Database("benchmark").Collection("person")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := p.Find(context.Background(), bson.NewDocument(bson.EC.String("color", "Goldenrod")))
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMUpdate(b *testing.B) {
	type Person struct {
		Firstname string
		Lastname  string
		Color     string
		Email     string
		Phone     string
		Timestamp int64
	}
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017/")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	p := client.Database("benchmark").Collection("person")
	t := time.Now()
	_, err = p.UpdateMany(
		context.Background(),
		bson.NewDocument(
			bson.EC.String("color", "change"),
		),
		bson.NewDocument(
			bson.EC.SubDocumentFromElements("$set",
				bson.EC.String("color", "Goldenrod"),
			),
		),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(time.Since(t))
}

func BenchmarkMinsert(b *testing.B) {
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017/")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	p := client.Database("benchmark").Collection("person")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = p.InsertOne(nil, bson.NewDocument(
			bson.EC.String("firstname", "to"),
			bson.EC.String("lastname", "nqmt"),
			bson.EC.String("color", "pissnk"),
			bson.EC.String("email", "to@to.com"),
			bson.EC.String("phone", "000"),
			bson.EC.Int64("timestamp", time.Now().Unix()),
		))
	}
	if err != nil {
		panic(err)
	}
}

func BenchmarkMDelete(b *testing.B) {
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017/")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	p := client.Database("benchmark").Collection("person")
	b.ResetTimer()
	_, err = p.DeleteMany(nil, bson.NewDocument(
		bson.EC.String("color", "pissnk"),
	))
	if err != nil {
		panic(err)
	}
}

func BenchmarkAggregation(b *testing.B) {
	type Person struct {
		Firstname string
		Lastname  string
		Color     string
		Email     string
		Phone     string
		Timestamp int64
	}
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017/")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	p := client.Database("benchmark").Collection("person")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipeline := bson.NewArray(
			bson.VC.DocumentFromElements(
				bson.EC.SubDocumentFromElements("$match",
					bson.EC.String("color", "join"),
				),
			),
			bson.VC.DocumentFromElements(
				bson.EC.SubDocumentFromElements("$lookup",
					bson.EC.String("from", "review"),
					bson.EC.String("localField", "color"),
					bson.EC.String("foreignField", "color"),
					bson.EC.String("as", "reviews"),
				),
			),
		)
		_, err := p.Aggregate(context.Background(), pipeline)
		if err != nil {
			panic(err)
		}

	//type Test struct {
	//	Firstname string
	//	Lastname string
	//	Color string
	//	Email string
	//	Phone string
	//	Timestamp int64
	//	Reviews []struct{
	//		Name string
	//		Color string
	//		Email string
	//		Timestamp int64
	//	}
	//}
	//tt := Test{}
	//for cursor.Next(context.Background()) {
	//	err := cursor.Decode(&tt)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	fmt.Printf("itemRead = %v\n", tt)
	//}
	}
}

func BenchmarkLookupMillion(b *testing.B) {
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

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
	_, err = collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
}
