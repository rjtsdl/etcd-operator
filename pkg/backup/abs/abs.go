// Copyright 2017 The etcd-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package abs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"path"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
)

const v1 = "v1/"

// ABS is a helper to wrap complex ABS logic
type ABS struct {
	containerURL azblob.ContainerURL
	container    *storage.Container
	prefix       string
	client       *storage.BlobStorageClient
	ctx          context.Context
}

// New returns a new ABS object for a given container using credentials set in the environment
func New(container, accountName, accountKey, prefix string) (*ABS, error) {
	credential := azblob.NewSharedKeyCredential(accountName, accountKey)
	p := azblob.NewPipeline(credential, PipelineOptions{})
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	if err != nil {
		return nil, err
	}

	serviceURL := azblob.NewServiceURL(*u, p)
	ctx := context.Background()

	containerURL := serviceURL.NewContainerURL(container)
	_, err = containerURL.Create(ctx, Metadata{}, PublicAccessNone)
	if err != nil {
		// it could be the container is already exists. For this scenario, we need to handle error better
		// For now, we just ignore err
		fmt.Warningf("containerURL.Create failed, need to check if it is because the container already existed")
	}

	return &ABS{
		containerURL: containerURL,
		prefix:       prefix,
		ctx:          ctx,
	}, nil
}

// Put puts a chunk of data into a ABS container using the provided key for its reference
func (w *ABS) Put(key string, r io.Reader) error {
	blobName := path.Join(v1, w.prefix, key)
	blobURL := w.containerURL.NewBlockBlobURL(blobName)

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read bytes from reader failed: %v", err)
	}

	_, err = blobURL.PutBlob(
		w.ctx, bytes.NewReader(b), 
		azblob.BlobHTTPHeaders{ContentType: "text/plain"}, 
		azblob.Metadata{}, 
		azblob.BlobAccessConditions{})
	if err != nil {
		return fmt.Errorf("create block blob from reader failed: %v", err)
	}

	return nil
}

// Get gets the blob object specified by key from a ABS container
func (w *ABS) Get(key string) (io.ReadCloser, error) {
	blobName := path.Join(v1, w.prefix, key)
	blobURL := containerURL.NewBlockBlobURL(blobName)

	get, err := blobURL.GetBlob(
		w.ctx, 
		azblob.BlobRange{}, 
		azblob.BlobAccessConditions{}, false)
	if err != nil {
		log.Fatal(err)
	}

	opts := &storage.GetBlobOptions{}
	return blob.Get(opts)
}

// Delete deletes the blob object specified by key from a ABS container
func (w *ABS) Delete(key string) error {
	blobName := path.Join(v1, w.prefix, key)
	blob := w.container.GetBlobReference(blobName)

	opts := &storage.DeleteBlobOptions{}
	return blob.Delete(opts)
}

// List lists all blobs in a given ABS container
func (w *ABS) List() ([]string, error) {
	_, l, err := w.list(w.prefix)
	return l, err
}

func (w *ABS) list(prefix string) (int64, []string, error) {
	params := storage.ListBlobsParameters{Prefix: path.Join(v1, prefix) + "/"}
	resp, err := w.container.ListBlobs(params)
	if err != nil {
		return -1, nil, err
	}

	keys := []string{}
	var size int64
	for _, blob := range resp.Blobs {
		k := (blob.Name)[len(resp.Prefix):]
		keys = append(keys, k)
		size += blob.Properties.ContentLength
	}

	return size, keys, nil
}

// TotalSize returns the total size of all blobs in a ABS container
func (w *ABS) TotalSize() (int64, error) {
	size, _, err := w.list(w.prefix)
	return size, err
}

// CopyPrefix copies all blobs with given prefix
func (w *ABS) CopyPrefix(from string) error {
	_, blobs, err := w.list(from)
	if err != nil {
		return err
	}
	for _, basename := range blobs {
		blobResource := w.container.GetBlobReference(basename)

		opts := storage.CopyOptions{}
		if err = blobResource.Copy(path.Join(w.container.Name, v1, from, basename), &opts); err != nil {
			return err
		}
	}
	return nil
}
