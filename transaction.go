package main

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"log"
	"time"
)

type Client struct {
	Client *mongo.Client
}

// 6ms commit
// 8ms
func (c *Client) transaction1(s mongo.SessionContext) error {
	t1 := c.Client.Database("mongo4").Collection("product")
	t2 := c.Client.Database("mongo4").Collection("stock")

	s.StartTransaction()
	_, err := t1.InsertOne(s, bson.NewDocument(
		bson.EC.String("_id", "022"),
		bson.EC.String("name", "iphonexx"),
	))
	if err != nil {
		s.AbortTransaction(s)
		return err
	}

	_, err = t2.InsertOne(s, bson.NewDocument(
		bson.EC.String("_id", "022"),
		bson.EC.String("productId", "002"),
		bson.EC.Int64("qty", 1),
	))
	if err != nil {
		s.AbortTransaction(s)
		return err
	}

	err = s.CommitTransaction(s)
	if err != nil {
		panic(err)
	}
	return nil
}

func (c *Client) transaction2(s mongo.SessionContext) error {
	t1 := c.Client.Database("mongo4").Collection("product")
	t2 := c.Client.Database("mongo4").Collection("stock")

	s.StartTransaction()
	_, err := t1.InsertOne(s, bson.NewDocument(
		bson.EC.String("_id", "033"),
		bson.EC.String("name", "iphonexx"),
	))
	if err != nil {
		s.AbortTransaction(s)
		return err
	}

	_, err = t2.InsertOne(s, bson.NewDocument(
		bson.EC.String("_id", "033"),
		bson.EC.String("productId", "002"),
		bson.EC.Int64("qty", 1),
	))
	if err != nil {
		s.AbortTransaction(s)
		return err
	}

	err = s.CommitTransaction(s)
	if err != nil {
		panic(err)
	}
	return nil
}

func sqlTrx(db *sqlx.DB) error {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
		return nil
	}

	// create order
	productSql := `
	INSERT INTO product(id, name)
	VALUES (?, ?);
	`
	_, err = tx.Exec(productSql, 4, "iphonex")
	if err != nil {
		tx.Rollback()
		return err
	}

	stockSql := `
	INSERT INTO stock(id, qty, productId)
	VALUES (?, ?, ?);
	`
	_, err = tx.Exec(stockSql, 4, 4, 4)
	if err != nil {
		tx.Rollback()
		return err
	}

	// success query order & order product
	err = tx.Rollback()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func tx(c mongo.Client) func (s mongo.SessionContext) error {
	return func (s mongo.SessionContext) error {
		return nil
	}
}

func main() {
	m, err := mongo.NewClient("mongodb://localhost:27017,localhost:27018,localhost:27019/mongo4?replicaSet=rs")
	if err != nil {
		log.Fatal(err)
	}

	err = m.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	//db, err := sqlx.Connect("mysql", "root:1234@tcp(127.0.0.1:3306)/transaction")
	//err = sqlTrx(db)
	//if err != nil {
	//	panic(err)
	//}

	c := Client{m}
	ctx := context.Background()
	t := time.Now()



	err = c.Client.UseSession(ctx, c.transaction1)
	if err != nil {
		panic(err)
	}

	err = c.Client.UseSession(ctx, c.transaction2)
	if err != nil {
		panic(err)
	}
	fmt.Println(time.Since(t))
}

//rs:PRIMARY> use mongo4
//switched to db mongo4
//rs:PRIMARY> db.createCollection("transaction")
