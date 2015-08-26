package main

import (
	"fmt"
	"time"

	"ciandt.com/libs/gbigtable"
	"ciandt.com/libs/ioutil"
)

const (
	KeyJsonFilePath = "/home/key.json"
)

func main() {

	var bigtableClientConnData = gbigtable.ClientConnectionData {
		Project: "bigdatagarage",
		Zone: "us-central1-c",
		Cluster: "workshopanalytics",
		KeyJsonFilePath: "/home/key.json",
	}

	importCSVOnTable(bigtableClientConnData, "/home/data/ego-produto.csv", "ego-produto")
}

func importCSVOnTable(bigtableClientConnData gbigtable.ClientConnectionData, csvFilePath string, tableName string) {

	client := gbigtable.OpenClient(
		gbigtable.GetContext(300 * time.Second),
		bigtableClientConnData)

	bgTable := gbigtable.OpenTable(
		tableName,
		client)

	fmt.Println("Start processCSVFile ...")
	var strColumns []string

	csvprocessor.ProcessCSVFile(csvFilePath,
		',',
		func (headerData []string) {

			strColumns = headerData
		},
		func (lineData []string) {

			gbigtable.WriteRow(
				gbigtable.GetContext(300 * time.Second),
				bgTable,
				"__", lineData[0],
				strColumns,
				lineData, 1)
		})

	defer client.Close()
}