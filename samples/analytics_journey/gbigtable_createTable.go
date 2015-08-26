package main

import (
	"time"

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

	createTable_ego_produto(bigtableClientConnData);
}

func createTable_ego_produto(bigtableClientConnData gbigtable.ClientConnectionData) {

	gbigtable.DeleteTable(
	 	bigtableClientConnData,
	 	gbigtable.GetContext(300 * time.Second),
	 	"ego-produto")

	families := []string{ "produto", "categoria", "localizacao", "loja", "periodo", "indicador" }

	gbigtable.CreateTable(
		bigtableClientConnData,
		gbigtable.GetContext(300 * time.Second),
		"ego-produto",
		families)
}