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
	"bytes"
	"fmt"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Memory backed S3 Writer
// Only writes when closed
type S3Writer struct {
	s3_session *session.Session
	bucket     string
	key        string
	buf        *bytes.Buffer
	err        error
}

func NewS3Writer(s *session.Session, bucket string, key string) (*S3Writer, error) {
	w := &S3Writer{s3_session: s, bucket: bucket, key: key, buf: bytes.NewBuffer([]byte{})} // empty
	return w, nil
}

// WriteCloser Interface

// Write bytes to S3 object
// Writes are buffered, if an error occurs later the next Write or Close will return the error
func (sw *S3Writer) Write(p []byte) (int, error) {
	if sw.err != nil {
		return -1, sw.err
	}

	n, err := sw.buf.Write(p)
	sw.err = err
	return n, sw.err
}

// Close the writer
// Closing after closing returns an error
func (sw *S3Writer) Close() error {
	if sw.err != nil {
		return sw.err
	}

	sw.err = fmt.Errorf("S3Writer is closed")
	var err error
	if sw.buf.Len() >= int(s3manager.MinUploadPartSize)*s3manager.DefaultUploadConcurrency {
		inParams := &s3manager.UploadInput{
			Bucket: aws.String(sw.bucket),
			Key:    aws.String(sw.key),
			Body:   sw.buf,
		}
		uploader := s3manager.NewUploader(sw.s3_session)
		_, err = uploader.Upload(inParams)
	} else {
		buffer := sw.buf.Bytes()
		//log.Printf("PutObject: saving %d bytes at bucket `%s`, key `%s`\n", len(buffer), sw.bucket, sw.key)
		_, err = s3.New(sw.s3_session).PutObject(&s3.PutObjectInput{
			Bucket:        aws.String(sw.bucket),
			Key:           aws.String(sw.key),
			Body:          bytes.NewReader(buffer),
			ContentLength: aws.Int64(int64(sw.buf.Len())),
			ContentType:   aws.String(http.DetectContentType(buffer)),
			//ContentDisposition:   aws.String("attachment"),
			//ServerSideEncryption: aws.String("AES256"),
		})
	}
	sw.buf = bytes.NewBuffer([]byte{})
	if err != nil {
		log.Printf("[S3Writer.Close] %v\n", err)
	}
	return err
}
