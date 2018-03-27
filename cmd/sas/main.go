package main

import (
	"flag"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/storage"
)

var (
	container   string
	accountName string
	sasToken    string
)

func init() {
	flag.StringVar(&container, "container", "", "container name")
	flag.StringVar(&accountName, "account-name", "", "storage account name")
	flag.StringVar(&sasToken, "sas-token", "", "SAS token")

	flag.Parse()
}

func main() {
	testFunctionsWithSAS()
}

func testFunctionsWithSAS() {
	endpoint := fmt.Sprintf("http://%s.blob.core.windows.net/%s", accountName, container)
	basicClient, err := storage.NewAccountSASClientFromEndpointToken(endpoint, sasToken)

	blobClient := basicClient.GetBlobService()
	containerRef := blobClient.GetContainerReference(container)

	// containerRef.Exists()
	containerExists, err := containerRef.Exists()
	if err != nil {
		fmt.Printf("containerRef.Exists() failed: %v", err)
	} else {
		fmt.Printf("containerRef.Exists() succeeded: %v", containerExists)
	}

	containerCreated, err := containerRef.CreateIfNotExists(&storage.CreateContainerOptions{})
	if err != nil {
		fmt.Printf("containerRef.CreateIfNotExists() failed: %v", err)
	} else {
		fmt.Printf("containerRef.CreateIfNotExists() succeeded: %v", containerCreated)
	}

	lr, err := containerRef.ListBlobs(storage.ListBlobsParameters{})
	if err != nil {
		fmt.Printf("containerRef.ListBlobs() failed: %v", err)
	} else {
		fmt.Printf("containerRef.ListBlobs() succeeded: %v", lr)
	}

	blobRef := containerRef.GetBlobReference("test.txt")

}
