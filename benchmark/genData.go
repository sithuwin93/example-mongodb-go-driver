package main

import (
	"encoding/json"
	"github.com/icrowley/fake"
	"io/ioutil"
	"log"
	"time"
)

func Normal() {
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

	persons := []Person{}
	reviews := []Review{}
	for i := 1; i <= 1000000; i++ {
		persons = append(persons, Person{
			Firstname: fake.FirstName(),
			Lastname:  fake.LastName(),
			Color:     fake.Color(),
			Email:     fake.EmailAddress(),
			Phone:     fake.Phone(),
			Timestamp: time.Now().UTC().Unix(),
		})

		reviews = append(reviews, Review{
			Name:      fake.FullNameWithPrefix(),
			Color:     fake.Color(),
			Email:     fake.EmailAddress(),
			Timestamp: time.Now().UTC().Unix(),
		})
	}

	personsJson, err := json.Marshal(persons)
	if err != nil {
		log.Fatal("Cannot encode person to JSON ", err)
	}
	reviewsJson, err := json.Marshal(reviews)
	if err != nil {
		log.Fatal("Cannot encode person to JSON ", err)
	}
	ioutil.WriteFile("persons.json", personsJson, 0644)
	ioutil.WriteFile("reviews.json", reviewsJson, 0644)
}

func Join() {
	type User struct {
		ID    int
		Name  string
		Email string
		Color string
		Time  int64
	}

	type Comment struct {
		ID     int
		Msg    string
		Time   int64
		UserID int
	}

	user := []User{}
	comment := []Comment{}
	for i := 1; i <= 1000000; i++ {
		user = append(user, User{
			ID:    i,
			Name:  fake.FullName(),
			Email: fake.EmailAddress(),
			Color: fake.Color(),
			Time: time.Now().Unix(),
		})

		comment = append(comment, Comment{
			ID:     i,
			Msg:    fake.Word(),
			Time:   time.Now().Unix(),
			UserID: i,
		})

		if i <= 10000 {
			comment = append(comment, Comment{
				ID:     i,
				Msg:    fake.Word(),
				Time:   time.Now().Unix(),
				UserID: 700000,
			})
		}
	}

	personsJson, err := json.Marshal(user)
	if err != nil {
		log.Fatal("Cannot encode person to JSON ", err)
	}
	reviewsJson, err := json.Marshal(comment)
	if err != nil {
		log.Fatal("Cannot encode person to JSON ", err)
	}
	ioutil.WriteFile("user.json", personsJson, 0644)
	ioutil.WriteFile("comment.json", reviewsJson, 0644)
}

func main() {
	//Normal()
	Join()
}
