package main

import (
	"fmt"
	"time"
	"encoding/json"

	"google.golang.org/cloud/bigtable"

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

	// https://godoc.org/google.golang.org/cloud/bigtable#RowRange
	// rr := bigtable.PrefixRange("my")
	 rr := bigtable.InfiniteRange("")
	// rr := bigtable.NewRange("begin", "end")
	// rr := bigtable.SingleRow("mykey01")

	printFilteredRowsFromTable(bigtableClientConnData, "test-tmp", rr, nil)

	// https://godoc.org/google.golang.org/cloud/bigtable#Filter
	var opts []bigtable.ReadOption
	// opts = append(opts, bigtable.RowFilter(bigtable.FamilyFilter(".*fam01.*")));
	// opts = append(opts, bigtable.RowFilter(bigtable.ColumnFilter(".*col.*")));
	opts = append(opts, bigtable.RowFilter(bigtable.ColumnFilter(".*col02.*")));
	// opts = append(opts, bigtable.RowFilter(bigtable.ChainFilters(bigtable.ColumnFilter(".*col.*"), bigtable.ValueFilter(".*val.*"))));

	printFilteredRowsFromTable(bigtableClientConnData, "test-tmp", (bigtable.RowRange{}), opts)
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