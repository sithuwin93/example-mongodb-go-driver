package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"io/ioutil"
	"log"
	"time"
)

type Person struct {
	Firstname string
	Lastname  string
	Color     string
	Email     string
	Phone     string
	Timestamp int64
}

type Review struct {
	Name      string
	Color     string
	Email     string
	Timestamp int64
}

type User struct {
	ID    int64
	Name  string
	Email string
	Color string
	Time  int64
}

type Comment struct {
	ID     int64
	Msg    string
	Time   int64
	UserID int64
}

func SeedMysql(person *[]Person, review *[]Review) {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("USE join")
	if err != nil {
		panic(err)
	}

	pp := *person
	rr := *review
	q1 := []string{}
	q2 := []string{}

	for i := 0; i < 1000000; i += 1000 {
		var b1 bytes.Buffer
		var b2 bytes.Buffer
		b1.WriteString("INSERT INTO person (firstname, lastname, color, email, phone, timestamp) VALUES")
		b2.WriteString("INSERT INTO review (name, color, email, timestamp) VALUES")
		for j := 0; j < 1000; j++ {
			if j == 999 {
				b1.WriteString(fmt.Sprintf("(%q, %q, %q, %q, %q, %d);", pp[i+j].Firstname, pp[i+j].Lastname, pp[i+j].Color, pp[i+j].Email, pp[i+j].Phone, pp[i+j].Timestamp))
				b2.WriteString(fmt.Sprintf("(%q, %q, %q, %d);", rr[i+j].Name, rr[i+j].Color, rr[i+j].Email, rr[i+j].Timestamp))
			} else {
				b1.WriteString(fmt.Sprintf("(%q, %q, %q, %q, %q, %d),", pp[i+j].Firstname, pp[i+j].Lastname, pp[i+j].Color, pp[i+j].Email, pp[i+j].Phone, pp[i+j].Timestamp))
				b2.WriteString(fmt.Sprintf("(%q, %q, %q, %d),", rr[i+j].Name, rr[i+j].Color, rr[i+j].Email, rr[i+j].Timestamp))
			}
		}
		q1 = append(q1, b1.String())
		q2 = append(q2, b2.String())
	}

	for i := 0; i < len(q1); i++ {
		_, err = db.Exec(q1[i])
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(q2[i])
		if err != nil {
			panic(err)
		}
	}
}

func SeedMysqlJoin(user *[]User, comment *[]Comment) {
	db, err := sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/join")
	if err != nil {
		panic(err)
	}

	uu := *user
	cc := *comment
	q1 := []string{}
	q2 := []string{}

	for i := 0; i < 1000000; i += 1000 {
		var b1 bytes.Buffer
		var b2 bytes.Buffer
		b1.WriteString("INSERT INTO user (name, email, color, time) VALUES")
		b2.WriteString("INSERT INTO comment (msg, time, userID) VALUES")
		for j := 0; j < 1000; j++ {
			if j == 999 {
				b1.WriteString(fmt.Sprintf("(%q, %q, %q, %d);", uu[i+j].Name, uu[i+j].Email, uu[i+j].Color, uu[i+j].Time))
				b2.WriteString(fmt.Sprintf("(%q, %d, %d);", cc[i+j].Msg, cc[i+j].Time, cc[i+j].UserID))
			} else {
				b1.WriteString(fmt.Sprintf("(%q, %q, %q, %d),", uu[i+j].Name, uu[i+j].Email, uu[i+j].Color, uu[i+j].Time))
				b2.WriteString(fmt.Sprintf("(%q, %d, %d),", cc[i+j].Msg, cc[i+j].Time, cc[i+j].UserID))
			}
		}
		q1 = append(q1, b1.String())
		q2 = append(q2, b2.String())
	}

	for i := 0; i < len(q1); i++ {
		_, err = db.Exec(q1[i])
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(q2[i])
		if err != nil {
			panic(err)
		}
	}
}

func SeedMongo(user *[]User, comment *[]Comment) {
	client, err := mongo.NewClient("mongodb://root:1234@localhost:27017/")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	uu := *user
	cc := *comment
	u := client.Database("join").Collection("user")
	c := client.Database("join").Collection("comment")
	udata := []interface{}{}
	cdata := []interface{}{}
	for i := 0; i < len(uu); i++ {
		udata = append(udata, bson.NewDocument(
			bson.EC.Int64("id", uu[i].ID),
			bson.EC.String("name", uu[i].Name),
			bson.EC.String("email", uu[i].Color),
			bson.EC.String("color", uu[i].Email),
			bson.EC.Int64("time", uu[i].Time),
		))
	}
	for i := 0; i < len(cc); i++ {
		cdata = append(cdata, bson.NewDocument(
			bson.EC.Int64("id", cc[i].ID),
			bson.EC.String("msg", cc[i].Msg),
			bson.EC.Int64("time", cc[i].Time),
			bson.EC.Int64("userID", cc[i].UserID),
		))
	}
	u.InsertMany(context.Background(), udata)
	c.InsertMany(context.Background(), cdata)
}

func ReadJSON() (*[]User, *[]Comment) {
	u, err := ioutil.ReadFile("user.json")
	if err != nil {
		panic(err)
	}
	user := []User{}
	json.Unmarshal(u, &user)

	c, err := ioutil.ReadFile("comment.json")
	if err != nil {
		panic(err)
	}
	comment := []Comment{}
	json.Unmarshal(c, &comment)
	return &user, &comment
}

func main() {
	u, c := ReadJSON()

	t := time.Now()
	//SeedMysql(u, c)
	SeedMysqlJoin(u, c)
	//SeedMongo(u, c)
	fmt.Println(time.Since(t))
}
