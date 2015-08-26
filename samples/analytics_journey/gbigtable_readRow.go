package main

import (
	"fmt"
	"time"
	"encoding/json"

	"ciandt.golang.org/libs/gbigtable"
)

const (
	KeyJsonFilePath = "/home/key.json"
)

func main() {

	var bigtableClientConnData = gbigtable.ClientConnectionData {
		Project: "bigdatagarage",
		Zone: "us-central1-c",
		Cluster: "workshopanalytics",
		KeyJsonFilePath: KeyJsonFilePath,
	}

	printRowFromTable(bigtableClientConnData, "test-tmp", "mykey01")
}

func printRowFromTable(bigtableClientConnData gbigtable.ClientConnectionData, tableName string, rowKey string) {

	client := gbigtable.OpenClient(
		gbigtable.GetContext(300 * time.Second),
		bigtableClientConnData)

	table := gbigtable.OpenTable(
		tableName,
		client)

	ctx := gbigtable.GetContext(300 * time.Second)

	dtRow := gbigtable.ReadRow(ctx, table, rowKey)

	jsonString, err := json.Marshal(dtRow)

	fmt.Println(string(jsonString), err)

	defer client.Close()
}