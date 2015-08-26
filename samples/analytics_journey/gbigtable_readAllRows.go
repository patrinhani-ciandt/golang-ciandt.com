package main

import (
	"fmt"
	"time"
	"encoding/json"

	"ciandt.com/libs/gbigtable"
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
	
	printAllRowsFromTable(bigtableClientConnData, "test-tmp")
}

func printAllRowsFromTable(bigtableClientConnData gbigtable.ClientConnectionData, tableName string) {

	client := gbigtable.OpenClient(
		gbigtable.GetContext(300 * time.Second),
		bigtableClientConnData)

	table := gbigtable.OpenTable(
		tableName,
		client)

	ctx := gbigtable.GetContext(300 * time.Second)

	gbigtable.ReadAllRows(ctx, table, func(dtRow gbigtable.DtRow) {

		jsonString, err := json.Marshal(dtRow)

		fmt.Println(string(jsonString), err)
	})

	defer client.Close()
}
