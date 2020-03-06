package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	log "github.com/sirupsen/logrus"
)

var (
	PROJECT  string
	DATASET  string
	TABLE    string
	FILEPATH string
	wg       sync.WaitGroup
	c1       = make(chan string)
)

type User struct {
	id   int
	name string
}

func (u *User) Save() (map[string]bigquery.Value, string, error) {
	result := make(map[string]bigquery.Value)
	result["id"] = u.id
	result["name"] = u.name
	return result, "", nil
}

func init() {
	flag.StringVar(&PROJECT, "project", "", "GCP Project")
	flag.StringVar(&DATASET, "dataset", "", "BQ Dataset")
	flag.StringVar(&TABLE, "table", "", "BQ Table")
	flag.StringVar(&FILEPATH, "filepath", "./students-100.json.txt", "JSON file to be inserted")
}

func main() {
	start := time.Now()
	flag.Parse()

	// Creating Table
	if PROJECT == "" || DATASET == "" || TABLE == "" {
		log.Fatal(fmt.Sprintf("Table ID \"%s.%s.%s\" is invalid", PROJECT, DATASET, TABLE))
	}
	log.Info(fmt.Sprintf("Creating Table \"%s.%s.%s\"\n", PROJECT, DATASET, TABLE))
	schemaJSON := `[{"name":"id","type":"NUMERIC","mode":"NULLABLE"},{"name":"name","type":"STRING","mode":"NULLABLE"}]`
	log.Info(schemaJSON)
	err := createTableExplicitSchema(PROJECT, DATASET, TABLE, schemaJSON)
	if err != nil {
		if strings.Contains(err.Error(), "Error 409: Already Exists:") {
			log.Info("Table already exist, not created")
		} else {
			log.Fatal(err)
		}
	}

	// Read file line by line and send it to channel
	log.Info("Reading file")
	go func(filepath string, chanStr chan<- string) {
		file, scanner := getFileScanner(filepath)
		defer file.Close()
		counter := 0
		for scanner.Scan() {
			text := scanner.Text()
			chanStr <- text
			counter++
		}
		close(chanStr)
		log.Info("Finished reading ", counter, " rows")
	}(FILEPATH, c1)

	// Get text from channel, convert to JSON, and insert to BQ Table
	log.Info(fmt.Sprintf("Consume rows"))
	wg.Add(1)
	go deployWorker(c1, PROJECT, DATASET, TABLE, &wg)

	// Wait
	wg.Wait()
	log.Info("Done in ", time.Since(start).Seconds(), "seconds")
}

func deployWorker(ch <-chan string, project, dataset, table string, wg *sync.WaitGroup) {
	// Create BQ client and context
	client, ctx := getBQClient(project)
	for {
		strJSON, more := <-ch
		if !more {
			break
		} else {
			// Convert to array
			user := parseUserFromJSONStr(strJSON)
			users := []*User{&user}
			// Insert to BQ table
			bqErr := insertUsersToBQTable(ctx, client, dataset, table, users)
			if bqErr == nil {
				log.Info(fmt.Sprintf("Inserted %d rows - %s", len(users), strJSON))
			}
		}
	}
	client.Close()
	wg.Done()
}

func getBQClient(projectID string) (*bigquery.Client, context.Context) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	return client, ctx
}

func createTableExplicitSchema(projectID string, datasetID string, tableID string, schemaJSON string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	schema, err := bigquery.SchemaFromJSON([]byte(schemaJSON))
	if err != nil {
		return fmt.Errorf("bigquery.SchemaFromJSON: %v", err)
	}
	metaData := &bigquery.TableMetadata{
		Schema: schema,
	}
	tableRef := client.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(ctx, metaData); err != nil {
		return err
	}
	client.Close()
	return nil
}

func insertUsersToBQTable(ctx context.Context, client *bigquery.Client, datasetID string, tableID string, users []*User) error {
	inserter := client.Dataset(datasetID).Table(tableID).Inserter()
	if err := inserter.Put(ctx, users); err != nil {
		return err
	}
	return nil
}

func parseUserFromJSONStr(jsonStr string) User {
	var result User
	err := json.Unmarshal([]byte(jsonStr), &result)
	if err != nil {
		log.Fatal(err)
	}
	return result
}

func getFileScanner(filepath string) (*os.File, *bufio.Scanner) {
	file, err := os.Open(FILEPATH)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(file)
	return file, scanner
}
