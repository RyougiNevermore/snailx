package snailx

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/shamaton/msgpack"
)

type Codec interface {
	Marshal(v interface{}) (data []byte, err error)
	Unmarshal(data []byte, v interface{}) (err error)
}

type jsonCodec struct {}

func (c *jsonCodec) Marshal(v interface{}) (data []byte, err error) {
	data, err = json.Marshal(v)
	return
}

func (c *jsonCodec) Unmarshal(data []byte, v interface{}) (err error) {
	err = json.Unmarshal(data, v)
	return
}

type msgpackCodec struct {}

func (c *msgpackCodec) Marshal(v interface{}) (data []byte, err error) {
	data, err = msgpack.Encode(v)
	return
}

func (c *msgpackCodec) Unmarshal(data []byte, v interface{}) (err error) {
	err = msgpack.Decode(data, v)
	return
}

type protoCodec struct {}

func (c *protoCodec) Marshal(v interface{}) (data []byte, err error) {
	if vv, ok := v.(proto.Message); ok {
		data, err = proto.Marshal(vv)
		return
	}
	err = fmt.Errorf("invalid param, it do not implement proto.Message")
	return
}

func (c *protoCodec) Unmarshal(data []byte, v interface{}) (err error) {
	if vv, ok := v.(proto.Message); ok {
		err = proto.Unmarshal(data, vv)
		return
	}
	err = fmt.Errorf("invalid param, it do not implement proto.Message")
	return
}