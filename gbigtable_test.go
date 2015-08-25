package main

import (
	"fmt"
	"time"
	// "strings"
	"encoding/json"

	// iconv "github.com/djimenez/iconv-go"

	// "golang.org/x/net/context"
	// "golang.org/x/oauth2/google"
	// "google.golang.org/cloud"
	"google.golang.org/cloud/bigtable"

	"app/ciandt.com/libs/gbigtable"
	"app/ciandt.com/libs/ioutil"
)

const (
	KeyJsonFilePath = "/home/key.json"
    // Scope is the OAuth scope for Cloud Bigtable data operations.
    // Scope = "https://www.googleapis.com/auth/bigtable.data"
    // ReadonlyScope is the OAuth scope for Cloud Bigtable read-only data operations.
    // ReadonlyScope = "https://www.googleapis.com/auth/bigtable.readonly"
    // AdminScope is the OAuth scope for Cloud Bigtable table admin operations.
    // AdminScope = "https://www.googleapis.com/auth/bigtable.admin.table"
)

func main() {

	// args := os.Args[1:]

	var bigtableClientConnData = gbigtable.ClientConnectionData {
		Project: "bigdatagarage",
		Zone: "us-central1-c",
		Cluster: "workshopanalytics",
		KeyJsonFilePath: "/home/key.json",
	}

	//gbigtable.DeleteTable(
	// 	bigtableClientConnData,
	// 	gbigtable.GetContext(300 * time.Second),
	// 	"test-tmp")

	//createTable_ego_produto(bigtableClientConnData);

	//importCSVOnTable(bigtableClientConnData, "data/test-tmp.csv", "test-tmp")

	printAllRowsFromTable(bigtableClientConnData, "test-tmp")

	// printRowFromTable(bigtableClientConnData, "test-tmp", "mykey01")

	// https://godoc.org/google.golang.org/cloud/bigtable#RowRange
	// rr := bigtable.PrefixRange("my")
	// rr := bigtable.InfiniteRange("key")
	// rr := bigtable.NewRange("begin", "end")
	// rr := bigtable.SingleRow("mykey01")

	// printFilteredRowsFromTable(bigtableClientConnData, "test-tmp", rr, nil)

	// https://godoc.org/google.golang.org/cloud/bigtable#Filter
	// var opts []bigtable.ReadOption
	// opts = append(opts, bigtable.RowFilter(bigtable.FamilyFilter(".*fam01.*")));
	// opts = append(opts, bigtable.RowFilter(bigtable.ColumnFilter(".*col.*")));
	// opts = append(opts, bigtable.RowFilter(bigtable.ColumnFilter(".*col02.*")));
	// opts = append(opts, bigtable.RowFilter(bigtable.ChainFilters(bigtable.ColumnFilter(".*col.*"), bigtable.ValueFilter(".*val.*"))));

	// printFilteredRowsFromTable(bigtableClientConnData, "test-tmp", (bigtable.RowRange{}), opts)
}

func createTable_ego_produto(bigtableClientConnData gbigtable.ClientConnectionData) {

	families := []string{ "produto", "categoria", "localizacao", "loja", "periodo", "indicador" }

	gbigtable.CreateTable(
		bigtableClientConnData,
		gbigtable.GetContext(300 * time.Second),
		"ego-produto",
		families)
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

// func filterDataFromTable(bigtableClientConnData gbigtable.ClientConnectionData, tableName string) {
// 	ctx := gbigtable.GetContext(300 * time.Second)
//
// 	client := gbigtable.OpenClient(
// 		gbigtable.GetContext(300 * time.Second),
// 		bigtableClientConnData)
//
// 	table := gbigtable.OpenTable(
// 		tableName,
// 		client)
//
// 	fmt.Println("Filtering Rows ...")
// }

// type DtRow struct {
// 	Key string
// 	Families map[string]map[string]interface{}
// }

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

func printFilteredRowsFromTable(bigtableClientConnData gbigtable.ClientConnectionData, tableName string, rowRange bigtable.RowRange, opts  []bigtable.ReadOption) {

	client := gbigtable.OpenClient(
		gbigtable.GetContext(300 * time.Second),
		bigtableClientConnData)

	table := gbigtable.OpenTable(
		tableName,
		client)

	ctx := gbigtable.GetContext(300 * time.Second)

	gbigtable.ReadRows(ctx, table, rowRange, func(dtRow gbigtable.DtRow) {

		jsonString, err := json.Marshal(dtRow)

		fmt.Println(string(jsonString), err)
	}, opts...);

	defer client.Close()
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