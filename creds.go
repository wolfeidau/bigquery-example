package main

import (
	"encoding/json"
	"io/ioutil"
)

type Creds struct {
	PrivateKeyID string `json:"private_key_id"` // private_key_id
	PrivateKey   string `json:"private_key"`    // private_key
	ClientEmail  string `json:"client_email"`   // client_email
	ClientID     string `json:"client_id"`      // client_id
	Type         string `json:"type"`           // type
}

func LoadGoogleCreds(fileName string) (*Creds, error) {
	buf, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	creds := &Creds{}
	err = json.Unmarshal(buf, creds)

	return creds, err
}
