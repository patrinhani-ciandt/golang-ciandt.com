package gbigtable

import (
	"fmt"
	"time"
	"io/ioutil"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/bigtable"	
)

type ClientConnectionData struct { 
	Project string
	Zone string
	Cluster string
	KeyJsonFilePath string
}

func getClientOptionFromJsonKeyFile(ctx context.Context, keyFilePath string, scope string) (cloud.ClientOption) {

	fmt.Println("Reading [" , keyFilePath, "] ...")
	jsonKey, err := ioutil.ReadFile(keyFilePath)
	
	if err != nil {
		fmt.Println("Error on [ioutil.ReadFile]: %v", err)
	}
	
	fmt.Println("Creating config ...")
	config, err := google.JWTConfigFromJSON(jsonKey, scope)

	if err != nil {
		fmt.Println("Error on [google.JWTConfigFromJSON]]: %v", err)
	}
	
	clientOption := cloud.WithTokenSource(config.TokenSource(ctx))
	
	return clientOption
}

func GetContext(timeout time.Duration) (context.Context) {
	
	ctx, _ := context.WithTimeout(context.Background(), timeout)

    return ctx
}

func openAdminClient(ctx context.Context, connectionData ClientConnectionData) (*bigtable.AdminClient) {

	clientOpt := getClientOptionFromJsonKeyFile(ctx, connectionData.KeyJsonFilePath, bigtable.AdminScope)
		
	client, err := bigtable.NewAdminClient(ctx, connectionData.Project, connectionData.Zone, connectionData.Cluster, clientOpt)	
	
	if err != nil {
		fmt.Println("Error on [NewAdminClient]: %v", err)
	}

	return client
}

func DeleteTable(connectionData ClientConnectionData, ctx context.Context, tableName string) {

	adminClient := openAdminClient(ctx, connectionData)

	fmt.Println("Deleting table ...")
	err := adminClient.DeleteTable(ctx, tableName)
	if err != nil {
		fmt.Println("Error on [DeleteTable]: %v", err)
	}
	
	defer adminClient.Close()
}

func CreateTable(connectionData ClientConnectionData, ctx context.Context, tableName string, families []string) {
	
	adminClient := openAdminClient(ctx, connectionData)

	fmt.Println("Creating table ...")
	err := adminClient.CreateTable(ctx, tableName)
	if err != nil {
		fmt.Println("Error on [CreateTable]: %v", err)
	}

	fmt.Println("Creating families ...")	
	for i := 0; i < len(families); i++ {
		
		fmt.Println("Creating family: %v", families[i])

		err = adminClient.CreateColumnFamily(ctx, tableName, families[i])
		if err != nil {
			fmt.Println("Error on [CreateColumnFamily]: %v", err)
		}
	}
	
	defer adminClient.Close()
}
