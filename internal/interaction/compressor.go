// Copyright © 2023 OpenIM SDK. All rights reserved.
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

package interaction

import (
	"bytes"
	"compress/gzip"
	"errors"
	"github.com/openimsdk/tools/errs"
	"io"
	"sync"
)

// This is the decoded payload limit, independent of the negotiated wire budget.
const maxDecompressedMessageSize int64 = 128 << 20

var ErrDecompressedMessageTooLarge = errors.New("decompressed websocket message exceeds 128 MiB")

func readDecompressedMessage(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if int64(len(data)) > limit {
		return nil, ErrDecompressedMessageTooLarge
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

var (
	gzipWriterPool = sync.Pool{New: func() any { return gzip.NewWriter(nil) }}
	gzipReaderPool = sync.Pool{New: func() any { return new(gzip.Reader) }}
)

type Compressor interface {
	Compress(rawData []byte) ([]byte, error)
	CompressWithPool(rawData []byte) ([]byte, error)
	DeCompress(compressedData []byte) ([]byte, error)
	DecompressWithPool(compressedData []byte) ([]byte, error)
}

type GzipCompressor struct {
	compressProtocol string
}

func NewGzipCompressor() *GzipCompressor {
	return &GzipCompressor{compressProtocol: "gzip"}
}

func (g *GzipCompressor) Compress(rawData []byte) ([]byte, error) {
	gzipBuffer := bytes.Buffer{}
	gz := gzip.NewWriter(&gzipBuffer)
	if _, err := gz.Write(rawData); err != nil {
		return nil, errs.WrapMsg(err, "")
	}
	if err := gz.Close(); err != nil {
		return nil, errs.WrapMsg(err, "")
	}
	return gzipBuffer.Bytes(), nil
}

func (g *GzipCompressor) CompressWithPool(rawData []byte) ([]byte, error) {
	gz := gzipWriterPool.Get().(*gzip.Writer)
	defer gzipWriterPool.Put(gz)

	gzipBuffer := bytes.Buffer{}
	gz.Reset(&gzipBuffer)

	if _, err := gz.Write(rawData); err != nil {
		return nil, errs.WrapMsg(err, "")
	}
	if err := gz.Close(); err != nil {
		return nil, errs.WrapMsg(err, "")
	}
	return gzipBuffer.Bytes(), nil
}

func (g *GzipCompressor) DeCompress(compressedData []byte) ([]byte, error) {
	buff := bytes.NewBuffer(compressedData)
	reader, err := gzip.NewReader(buff)
	if err != nil {
		return nil, errs.WrapMsg(err, "NewReader failed")
	}
	defer reader.Close()
	compressedData, err = readDecompressedMessage(reader, maxDecompressedMessageSize)
	if err != nil {
		return nil, err
	}
	return compressedData, nil
}

func (g *GzipCompressor) DecompressWithPool(compressedData []byte) ([]byte, error) {
	reader := gzipReaderPool.Get().(*gzip.Reader)
	if reader == nil {
		return nil, errors.New("NewReader failed")
	}
	defer gzipReaderPool.Put(reader)

	err := reader.Reset(bytes.NewReader(compressedData))
	if err != nil {
		return nil, errs.WrapMsg(err, "NewReader failed")
	}

	defer reader.Close()
	compressedData, err = readDecompressedMessage(reader, maxDecompressedMessageSize)
	if err != nil {
		return nil, err
	}
	return compressedData, nil
}
