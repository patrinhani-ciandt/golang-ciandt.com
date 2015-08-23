package main

import (
	"os"
	"fmt"
	"time"
	"strings"
	"encoding/csv"
	// "unicode/utf8"
	"io"
	"io/ioutil"
	
	// iconv "github.com/djimenez/iconv-go"
	
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/bigtable"	

	"app/ciandt.com/libs/gbigtable"
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
	
	var bigtableClientConnData = gbigtable.ClientConnectionData { 
		Project: "bigdatagarage",
		Zone: "us-central1-c",
		Cluster: "workshopanalytics",
		KeyJsonFilePath: "/home/key.json",		
	}
	
	// gbigtable.CreateTable(
	// 	bigtableClientConnData,
	// 	gbigtable.GetContext(300 * time.Second),
	// 	"test-tmp",
	// 	[]string{})

	// gbigtable.DeleteTable(
	// 	bigtableClientConnData,
	// 	gbigtable.GetContext(300 * time.Second),
	// 	"test-tmp")

	//printDataFromTable("ego-produto")
	
	// dropTable("ego-produto")
	
	// createTable_ego_produto();
		
	//importCSVOnTable("data/ego-produto.csv", "ego-produto")
}

func importCSVOnTable(csvFilePath, tableName string) {

	ctx := getContext(20 * time.Second);
		
	client := openClient(ctx, "bigdatagarage", "us-central1-c", "workshopanalytics")
	
	tbl := client.Open(tableName)
		
	fmt.Println("Start processCSVFile ...")
	var strColumns []string

	processCSVFile(csvFilePath, 
		func (headerData []string) {
			
			strColumns = headerData
		}, 
		func (lineData []string) {

			ctx := getContext(60 * time.Second);

			mut := bigtable.NewMutation()

			// rowKeyFmt := fmt.Sprintf("%d", rowKey)
			rowKeyFmt := lineData[0]

			for i := 1; i < len(lineData); i++ {

				var colSet = strings.Split(strColumns[i], "__")
				fam := colSet[0]
				col := colSet[1]
				  
				mut.Set(fam, col, 0, []byte(lineData[i]))
			}
						
			fmt.Println("Applying row: ", rowKeyFmt)
			if err := tbl.Apply(ctx, rowKeyFmt, mut); err != nil {
				fmt.Println("Mutating row %v: %v", rowKeyFmt, err)
			}
		})
	
	defer client.Close()
}

func printDataFromTable(tableName string) {
	
	ctx := getContext(10 * time.Second);
	
	client := openClient(ctx, "bigdatagarage", "us-central1-c", "workshopanalytics")
	
	table := openTable(tableName, client)
	
	fmt.Println("Reading Rows ...")
	table.ReadRows(ctx, bigtable.InfiniteRange(""), func(r bigtable.Row) bool {
    	
		for _, ris := range r {

			for _, ri := range ris {
				
				var colSet = strings.Split(ri.Column, ":")
				fam := colSet[0]
				col := colSet[1]
			
				cellValue := fmt.Sprintf("%s", ri.Value)
			
				fmt.Println("row-key: ", ri.Row, "Column:  ", ri.Column, "Col. Fam.: ", fam, "Col. Qual.: ", col, "Cell Value: ", cellValue)
			}
		}
				
		return true
	})
	
	defer client.Close()
}

func getContext(timeout time.Duration) (context.Context) {
	
	ctx, _ := context.WithTimeout(context.Background(), timeout)

    return ctx
}

func getClientOptionFromJsonKeyFile(ctx context.Context, keyFilePath string, scope string) (cloud.ClientOption) {

	fmt.Println("Reading [" , KeyJsonFilePath, "] ...")
	jsonKey, err := ioutil.ReadFile("/home/key.json")
	
	if err != nil {
		fmt.Println("Partial ReadRows: %v", err)
	}
	
	fmt.Println("Creating config ...")
	config, err := google.JWTConfigFromJSON(jsonKey, scope)

	if err != nil {
		fmt.Println("Partial ReadRows: %v", err)
	}
	
	clientOption := cloud.WithTokenSource(config.TokenSource(ctx))
	
	return clientOption
}

type processCSVLine func([]string)

func processCSVHeader(headerData []string) {

	fmt.Println("Header: ", headerData)
}

func processCSVData(data []string) {

	fmt.Println("Data: ", data)
}

func processCSVFile(filePath string, processHeader processCSVLine, processData processCSVLine) {
	
	file, err := os.Open(filePath)
	if err != nil {
		// err is printable
		// elements passed are separated by space automatically
		fmt.Println("Error:", err)
		return
	}
	// automatically call Close() at the end of current method
	defer file.Close()

	reader := csv.NewReader(file)
	// options are available at:
	// http://golang.org/src/pkg/encoding/csv/reader.go?s=3213:3671#L94
	reader.Comma = ';'
	lineCount := 0
		
	for {
		// read just one record, but we could ReadAll() as well
		record, err := reader.Read()
		// end-of-file is fitted into err
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
			return
		}
		
		if (lineCount == 0) {
			
			processHeader(record)
		} else {
			
			processData(record)
		}
		
		lineCount += 1
	}
}