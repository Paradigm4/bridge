// Copyright (C) 2020-2021 Paradigm4 Inc.
// All Rights Reserved.
//
// scidbbridge is a plugin for SciDB, an Open Source Array DBMS
// maintained by Paradigm4. See http://www.paradigm4.com/
//
// scidbbridge is free software: you can redistribute it and/or modify
// it under the terms of the AFFERO GNU General Public License as
// published by the Free Software Foundation.
//
// scidbbridge is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY
// KIND, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
// NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See the
// AFFERO GNU General Public License for the complete license terms.
//
// You should have received a copy of the AFFERO GNU General Public
// License along with scidbbridge. If not, see
// <http://www.gnu.org/licenses/agpl-3.0.html>
package scidbbridge

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	neturl "net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	// Arrow
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio"

	// "github.com/apache/arrow/go/parquet"

	// S3
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	// Better gzip, but creates trash at the end of some chunks
	//gzip "github.com/klauspost/pgzip"
	// need to test bgzf - should create gzip compatible files
	// "github.com/biogo/hts/bgzf"
)

// https://dev.to/flowup/using-io-reader-io-writer-in-go-to-stream-data-3i7b
type FakeS3WriterAt struct {
	w io.Writer
}

func (fw FakeS3WriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we are forcing sequential downloads
	return fw.w.Write(p)
}

type Driver struct {
	url        *neturl.URL
	s3_session *session.Session
}

func NewDriver(url *neturl.URL) (*Driver, error) {
	// check scheme
	switch url.Scheme {
	case "file":
		return &Driver{url: url}, nil
	case "s3":
		sess := session.Must(session.NewSessionWithOptions(session.Options{
			Config: aws.Config{
				// Use a custom retryer to provide custom retry rules.
				Retryer: client.DefaultRetryer{
					NumMaxRetries: client.DefaultRetryerMaxNumRetries,
				},
			},
			// Force enable Shared Config support
			SharedConfigState: session.SharedConfigEnable,
		}))
		return &Driver{url: url, s3_session: sess}, nil
	case "":
		return nil, fmt.Errorf("[NewDriver] Missing url scheme: %v", url)
	default:
		return nil, fmt.Errorf("[NewDriver] Unsupported url scheme: '%v'", url.Scheme)
	}
}

// Returns the Driver's URL
func (d *Driver) GetUrl() *neturl.URL {
	curl, _ := neturl.Parse(d.url.String())
	return curl
}

// Returns true if the array exists - can return false if something exists but is not an array directory
func (d *Driver) ArrayExists() bool {
	switch d.url.Scheme {
	case "file":
		info, err := os.Stat(d.url.Path)
		if os.IsNotExist(err) {
			return false
		}
		if !info.IsDir() {
			return false
		}
		// check for mandatory dirs/files
		for _, f := range []string{"metadata", "chunks", "index"} {
			if _, err := os.Stat(path.Join(d.url.Path, f)); os.IsNotExist(err) {
				return false
			}
		}
		return true
	case "s3":
		// chunks and index might be empty and thus don't exist (S3 doesn't really have directories)
		// so can't check for them, only if metadata exists
		return d.Exists("metadata")
	default:
		return false
	}
}

// Checks if the given suffix exists under this array
func (d *Driver) Exists(suffix string) bool {
	switch d.url.Scheme {
	case "file":
		path := path.Join(d.url.Path, suffix)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return true
		}
		return false
	case "s3":
		bucket := d.url.Host
		key := strings.TrimPrefix(d.url.Path, "/")
		key = fmt.Sprintf("%s/%s", key, suffix)
		maxKeys := int64(1)
		s3svc := s3.New(d.s3_session)
		output, err := s3svc.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(key),
			MaxKeys: &maxKeys,
		})
		if err != nil {
			// it failed, since this returned an error, assume "Not Found"
			log.Printf("not found bucket '%s', key '%s': %v\n", bucket, key, err)
			return false
		}
		if *output.KeyCount == int64(0) {
			log.Printf("no objects found at '%s/%s'\n", bucket, key)
			return false
		}
		return true
	default:
		return false
	}
}

// Deletes the Array
func (d *Driver) DeleteArray() error {
	switch d.url.Scheme {
	case "file":
		return os.RemoveAll(d.url.Path)
	default:
		return fmt.Errorf("[Driver.DeleteArray] unsupported scheme: %v", d.url.Scheme)
	}
}

// Sets up the required files for a new Array
func (d *Driver) InitializeArray(a *Array) error {
	switch d.url.Scheme {
	case "file":
		err := os.MkdirAll(d.url.Path, 0755)
		if err != nil {
			return fmt.Errorf("%v: %v", d.url.String(), err)
		}
		err = d.WriteMetadata(metadataToString(a.metadata))
		if err != nil {
			return err
		}
		for _, f := range []string{"chunks", "index"} {
			err = os.MkdirAll(path.Join(d.url.Path, f), 0755)
			if err != nil {
				return err
			}
		}
		return nil
	case "s3":
		// no such thing as directories, they are special objects
		return d.WriteMetadata(metadataToString(a.metadata))
	default:
		// shouldn't be here!
		return fmt.Errorf("unsupported schema: '%v'", d.url)
	}
}

// List files under path
// This doesn't recurse
func List(url *neturl.URL) ([]string, error) {
	res := []string{}
	switch url.Scheme {
	case "file":
		files, err := ioutil.ReadDir(url.Path)
		if err != nil {
			return res, err
		}
		for _, f := range files {
			if !f.IsDir() {
				res = append(res, fmt.Sprintf("%s/%s", url.String(), f.Name())) // full url path
			}
		}
		return res, nil
	default:
		return nil, fmt.Errorf("[Driver.List] Unsupported schema: '%v'", url)
	}
}

func (d *Driver) ReadMetadata() (string, error) {
	switch d.url.Scheme {
	case "file":
		path := filepath.Join(d.url.Path, "metadata")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return "", fmt.Errorf("[Driver.ReadMetadata] missing 'metadata' file")
		}
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case "s3":
		if d.Exists("metadata") {
			if r, err := d.NewReader("metadata"); err != nil {
				return "", err
			} else {
				buf := new(strings.Builder)
				_, err := io.Copy(buf, r)
				if err != nil {
					return "", err
				}
				return buf.String(), nil
			}
		} else {
			return "", fmt.Errorf("[Driver.ReadMetadata] missing 'metadata' file")
		}
	default:
		return "", fmt.Errorf("[Driver.ReadMetadata] unsupported schema: '%v'", d.url)
	}
}

// WriteMetadata saves given metadata string out
func (d *Driver) WriteMetadata(metadata string) error {
	// make sure we end with a LF
	metadata = strings.TrimSpace(metadata) + "\n"
	switch d.url.Scheme {
	case "file":
		path := filepath.Join(d.url.Path, "metadata")
		return ioutil.WriteFile(path, []byte(metadata), 0644)
	case "s3":
		key := filepath.Join(d.url.Path, "metadata")
		w, err := NewS3Writer(d.s3_session, d.url.Host, key)
		if err != nil {
			return err
		}
		_, err = w.Write([]byte(metadata)) // not expecting an error from S3 here
		if err != nil {
			return err
		}
		return w.Close() // here is the S3 error if any
	default:
		// shouldn't be here!
		return fmt.Errorf("[Driver.WriteMetadata] unsupported schema: '%v'", d.url)
	}
}

// NewReader returns an io.Reader for for the file(suffix)/object(key) under the partant URL
func (d *Driver) NewReader(suffix string) (io.Reader, error) {
	switch d.url.Scheme {
	case "file":
		path := filepath.Join(d.url.Path, suffix)
		r, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return r, nil
	case "s3":
		//s3: //bucket/key
		bucket := d.url.Host
		key := filepath.Join(d.url.Path, suffix)
		// https://stackoverflow.com/questions/45089248/buffered-version-of-go-io-pipe
		buf := buffer.New(32 * 1024 * 1024) // s3manager.DefaultDownloaddPartSize = 1024 * 1024 * 5
		pr, pw := nio.Pipe(buf)
		// downloader
		go func() {
			defer pw.Close()

			downloader := s3manager.NewDownloader(d.s3_session)
			downloader.Concurrency = 1
			downloader.PartSize = 32 * 1024 * 1024

			// https://stackoverflow.com/questions/46019484/buffer-implementing-io-writerat-in-go
			// requires writerAt, faking it since we set Concurrency to 1 this is okay
			_, err := downloader.Download(FakeS3WriterAt{pw},
				&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})

			if err != nil {
				log.Fatalf("[Driver.NewReader] S3 unable to download item %q from %v, %v", suffix, bucket, err)
			}
		}()
		return pr, nil
	default:
		return nil, fmt.Errorf("[Driver.NewReader] unsupported schema: '%v'", d.url.Scheme)
	}
}

func (d *Driver) NewWriter(suffix string) (io.WriteCloser, error) {
	switch d.url.Scheme {
	case "file":
		err := os.MkdirAll(d.url.Path, 0755)
		if err != nil {
			return nil, err
		}
		path := filepath.Join(d.url.Path, suffix)
		w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644) // shouldn't need O_SYNC here, if added it slows everything down
		if err != nil {
			return nil, err
		}
		return w, nil
	case "s3":
		key := filepath.Join(d.url.Path, suffix)
		w, err := NewS3Writer(d.s3_session, d.url.Host, key)
		if err != nil {
			return nil, err
		}
		return w, nil
	default:
		return nil, fmt.Errorf("[Driver.NewArrayWriter] Unsupported schema: '%v'", d.url.Scheme)
	}
}

func newStreamReader(r io.Reader, compression string, mem memory.Allocator) (*ipc.Reader, error) {

	switch compression {
	case "", "none":
		return ipc.NewReader(r)
	case "gzip":
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return ipc.NewReader(zr, ipc.WithAllocator(mem))
	case "zstd":
		// Requires >= Arrow 1.0.0 to read/decompress
		return ipc.NewReader(r, ipc.WithAllocator(mem), ipc.WithZstd())
	case "lz4":
		// Requires >= Arrow 1.0.0 to read/decompress
		return ipc.NewReader(r, ipc.WithAllocator(mem), ipc.WithLZ4())
	default:
		return nil, fmt.Errorf("[Driver] unsupported compression: %v", compression)
	}
}

func CreateArrayReader(url string, compression string, mem memory.Allocator) (*ipc.Reader, error) {
	u, err := neturl.Parse(url)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "file":
		r, err := os.Open(u.Path) // will closing the StreamReader close this?
		if err != nil {
			return nil, err
		}
		return newStreamReader(r, compression, mem)
	default:
		return nil, fmt.Errorf("[Driver.CreateReader] unsupported schema: %v", url)
	}
}

// Read all the records returning a table
func ReadAllRecords(reader *ipc.Reader, mem memory.Allocator) (array.Table, error) {
	records := []array.Record{}
	for reader.Next() {
		r := reader.Record()
		if len(records) > 1 && !records[0].Schema().Equal(r.Schema()) {
			// NewTable will panic if schemas not the same
			return nil, fmt.Errorf("[Driver.ReadAllRecords] records have different schemas")
		}
		r.Retain() // reader.Next calls r.Release() then tries to fetch the next. So now our record is down to 0 causing NewTable to panic
		records = append(records, r)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("[Driver.ReadAllRecords] empty Table")
	}
	return array.NewTableFromRecords(records[0].Schema(), records), nil
}

type ArrayWriter struct {
	wc []io.WriteCloser // intermediate streams, followed by the underlining stream at the end
	w  *ipc.Writer      // Arrow ipc writer
}

// 'cause Go IO sucks and interfaces can't take a pointer
type BufferedWriterCloser struct {
	w *bufio.Writer
}

func (bw BufferedWriterCloser) Close() error {
	return bw.w.Flush()
}

func (bw BufferedWriterCloser) Write(p []byte) (n int, err error) {
	return bw.w.Write(p)
}

func (aw *ArrayWriter) Close() error {
	// stack errors
	errs := []string{}
	if err := aw.w.Close(); err != nil {
		errs = append(errs, err.Error())
	}
	for _, wc := range aw.wc {
		if err := wc.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) == 0 {
		return nil
	} else {
		return fmt.Errorf(strings.Join(errs, "; "))
	}
}

// Helper function for compression
func newArrowStreamWriter(w io.WriteCloser, compression string, mem memory.Allocator, schema *arrow.Schema) (*ArrayWriter, error) {
	wc := []io.WriteCloser{w}
	switch compression {
	case "", "none":
		return &ArrayWriter{wc: wc, w: ipc.NewWriter(w, ipc.WithAllocator(mem), ipc.WithSchema(schema))}, nil
	case "gzip":
		// using BestCompression with builtin gzip is painfully slow - like 4x slower and no savings in space
		// best BestCompression with pgzip is about the same as regular builtin
		zw, err := gzip.NewWriterLevel(w, gzip.BestCompression)
		if err != nil {
			log.Println(err)
			// back off to normal
			zw = gzip.NewWriter(w)
		}
		wc = append([]io.WriteCloser{zw}, wc...)
		aw := ipc.NewWriter(zw, ipc.WithAllocator(mem), ipc.WithSchema(schema))
		return &ArrayWriter{wc: wc, w: aw}, nil
	case "zstd":
		// Requires >= Arrow 1.0.0 to read/decompress
		// TODO: ipc.WithCompressConcurrency(n)
		return &ArrayWriter{wc: wc, w: ipc.NewWriter(w, ipc.WithAllocator(mem), ipc.WithSchema(schema), ipc.WithZstd())}, nil
	case "lz4":
		// Requires >= Arrow 1.0.0 to read/decompress
		// TODO: ipc.WithCompressConcurrency(n)
		return &ArrayWriter{wc: wc, w: ipc.NewWriter(w, ipc.WithAllocator(mem), ipc.WithSchema(schema), ipc.WithLZ4())}, nil
	default:
		return nil, fmt.Errorf("[Driver] unsupported compression: %v", compression)
	}
}

func (d *Driver) CreateArrowWriter(suffix string, compression string, mem memory.Allocator, schema *arrow.Schema) (*ArrayWriter, error) {
	if schema == nil {
		return nil, fmt.Errorf("[Driver.CreateArrowWriter] require a Schema")
	}
	w, err := d.NewWriter(suffix)
	if err != nil {
		return nil, err
	}
	return newArrowStreamWriter(w, compression, mem, schema)
}
