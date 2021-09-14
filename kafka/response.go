package kafka

import (
	"encoding/binary"
	"fmt"
	"io"

	"errors"
)

const (
	MaxResponseSize int32 = 100 * 1024 * 1024
)

type Response struct {
	Key int16

	// Version is a Kafka broker version
	Version int16

	// Is request body length
	BodyLength int32

	CorrelationID int32

	ClientID string

	Body ProtocolBody

	UsePreparedKeyVersion bool
}

func DecodeResponse(r io.Reader, correlationMap *CorrelationMap) (*Response, int, error) {
	var (
		needReadBytes = 8
		readBytes     = make([]byte, needReadBytes)
	)

	if _, err := io.ReadFull(r, readBytes); err != nil {
		return nil, needReadBytes, err
	}
	if len(readBytes) != needReadBytes {
		return nil, len(readBytes), errors.New("could define length, key, version")
	}

	length := DecodeLength(readBytes) - 4
	correlationID := DecodeCorrelationID(readBytes)

	value, ok := correlationMap.GetAndRemove(correlationID)
	if !ok {
		return nil, int(length), PacketDecodingError{
			fmt.Sprintf("could get version and key by correlationID %d", correlationID)}
	}

	key := value[0]
	version := value[1]
	body := allocateResponseBody(key, version)
	if body == nil {
		return nil, int(length), PacketDecodingError{fmt.Sprintf("unsupported body with key: %d", key)}
	}

	// check request size
	if length <= 4 || length > MaxResponseSize {
		return nil, int(length), PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", length)}
	}

	encodedResp := make([]byte, length)
	if _, err := io.ReadFull(r, encodedResp); err != nil {
		return nil, int(length), err
	}

	bytesRead := needReadBytes + len(encodedResp)
	err := body.Decode(&RealDecoder{raw: encodedResp}, version)
	if err != nil {
		return nil, bytesRead, err
	}

	resp := &Response{
		BodyLength: length,
		Key:        key,
		Version:    version,
		Body:       body,
	}

	return resp, bytesRead, nil
}

func DecodeCorrelationID(encoded []byte) int32 {
	return int32(binary.BigEndian.Uint32(encoded[4:]))
}

func allocateResponseBody(key, version int16) ProtocolBody {
	switch key {
	case 0:
		return &ProduceResponse{Version: version}
	case 1:
		return &FetchResponse{Version: version}
	}
	return nil
}
