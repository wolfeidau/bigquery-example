package main

import (
	"bytes"
	"log"
	"strings"

	"code.google.com/p/goauth2/oauth/jwt"
	"code.google.com/p/google-api-go-client/bigquery/v2"
	"github.com/davecgh/go-spew/spew"
)

const (
	authURL  = "https://accounts.google.com/o/oauth2/auth"
	tokenURL = "https://accounts.google.com/o/oauth2/token"
	scope    = "https://www.googleapis.com/auth/bigquery"
)

func buildTableSchema() []*bigquery.TableFieldSchema {
	return []*bigquery.TableFieldSchema{
		{
			Name: "UserID",
			Type: "STRING",
		},
		{
			Name: "Key",
			Type: "STRING",
		},
		{
			Name: "Timestamp",
			Type: "TIMESTAMP",
		},
		{
			Name: "Measurement",
			Type: "RECORD",
			Fields: []*bigquery.TableFieldSchema{
				{
					Name: "Min",
					Type: "FLOAT",
				},
				{
					Name: "Max",
					Type: "FLOAT",
				},
				{
					Name: "Mean",
					Type: "FLOAT",
				},
				{
					Name: "Value",
					Type: "FLOAT",
				},
				{
					Name: "Count",
					Type: "INTEGER",
				},
				{
					Name: "Percentile95",
					Type: "FLOAT",
				},
			},
		},
		{
			Name: "Log",
			Type: "RECORD",
			Fields: []*bigquery.TableFieldSchema{
				{
					Name: "Tag",
					Type: "STRING",
				},
				{
					Name: "Content",
					Type: "STRING",
				},
				{
					Name: "Facility",
					Type: "STRING",
				},
				{
					Name: "Severity",
					Type: "STRING",
				},
			},
		},
	}
}

type Storage struct {
	projectID string
	creds     *Creds
	token     *jwt.Token
	transport *jwt.Transport
}

func NewStorage(creds *Creds) (*Storage, error) {

	// Craft the ClaimSet and JWT token.
	token := jwt.NewToken(creds.ClientEmail, scope, bytes.NewBufferString(creds.PrivateKey).Bytes())
	token.ClaimSet.Aud = tokenURL

	transport, err := jwt.NewTransport(token)

	if err != nil {
		return nil, err
	}

	s := &Storage{
		creds:     creds,
		token:     token,
		transport: transport,
		projectID: strings.SplitN(creds.ClientID, "-", 2)[0],
	}
	return s, nil
}

func (s *Storage) NewTable(datasetID string, tableId string) error {

	service, err := bigquery.New(s.transport.Client())

	if err != nil {
		return err
	}

	tisc := service.Tables.Insert(s.projectID, datasetID, &bigquery.Table{
		TableReference: &bigquery.TableReference{
			TableId:   tableId,
			DatasetId: datasetID,
			ProjectId: s.projectID,
		},
		Schema: &bigquery.TableSchema{
			Fields: buildTableSchema(),
		},
	})

	ts, err := tisc.Do()

	log.Printf("table created %s", spew.Sdump(ts))

	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) NewDataset(datasetId string) error {

	service, err := bigquery.New(s.transport.Client())

	if err != nil {
		return err
	}

	disc := service.Datasets.Insert(s.projectID, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{
			DatasetId: datasetId,
			ProjectId: s.projectID,
		},
	})

	ds, err := disc.Do()

	log.Printf("dataset created %s", spew.Sdump(ds))

	if err != nil {
		return err
	}

	return nil
}
