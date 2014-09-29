package main

import (
	"log"
	"os/user"
	"path"
)

func main() {

	usr, err := user.Current()

	if err != nil {
		log.Fatalf("unable to load user details: %s", err)
	}

	creds, err := LoadGoogleCreds(path.Join(usr.HomeDir, ".google/service-creds.json"))

	if err != nil {
		log.Fatalf("unable to load creds: %s", err)
	}

	store, err := NewStorage(creds)

	if err != nil {
		log.Fatalf("unable to create store: %s", err)
	}

	log.Printf("creating Dataset")

	err = store.NewDataset("DevelopmentDataset")

	if err != nil {
		log.Fatalf("unable to create dataset: %s", err)
	}

	log.Printf("creating Table")

	err = store.NewTable("DevelopmentDataset", "EventsTable")

	if err != nil {
		log.Fatalf("unable to create table: %s", err)
	}
}
