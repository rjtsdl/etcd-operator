package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
)

var (
	container   string
	accountName string
	accountKey  string
)

func init() {
	flag.StringVar(&container, "container", "", "container name")
	flag.StringVar(&accountName, "account-name", "", "storage account name")
	flag.StringVar(&accountKey, "account-key", "", "storage account key")

	flag.Parse()
}

func main() {
	sas, err := GenerateSASTokenForEtcdBackup(container, accountName, accountKey)
	fmt.Printf("sas: %s  err: %v", sas, err)
}

// GenerateSASTokenForEtcdBackup creates a SAS token for backup
func GenerateSASTokenForEtcdBackup(container, storageAccount, storageKey string) (string, error) {
	basicClient, err := storage.NewBasicClient(storageAccount, storageKey)
	if err != nil {
		return "", err
	}
	blobSvc := basicClient.GetBlobService()
	containerRef := blobSvc.GetContainerReference(container)
	opt := storage.ContainerSASOptions{
		ContainerSASPermissions: storage.ContainerSASPermissions{
			BlobServiceSASPermissions: storage.BlobServiceSASPermissions{
				Read:   true,
				Write:  true,
				Delete: true,
				Add:    true,
				Create: true,
			},
			List: true,
		},
		SASOptions: storage.SASOptions{
			APIVersion: "2017-07-29",
			Expiry:     time.Now().AddDate(5, 0, 0),
			UseHTTPS:   true,
		},
	}
	uri, err := containerRef.GetSASURI(opt)
	if err != nil {
		return "", err
	}

	token := uri
	ind := strings.IndexAny(token, "?")
	if ind != -1 {
		token = token[ind+1:]
	}

	return token, nil
}
