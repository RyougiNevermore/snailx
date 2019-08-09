package snailx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"reflect"
	"sync"
	"time"
)

// header{id(22)ok(0|1)hasValue(0|!)hasError(0|1)error bytes) + body
func encodeServiceResult(codec Codec, requestId string, ok bool, v interface{}, cause error) (p []byte, err error) {
	if len(requestId) != 22 {
		err = fmt.Errorf("invalid requestId, %s", requestId)
		return
	}
	var body []byte = nil
	hasContent := v != nil
	if hasContent {
		if body, err = codec.Marshal(v); err != nil {
			return
		}
	}
	hasContent = len(body) != 0
	header := requestId
	if ok {
		header = header + "1"
	} else {
		header = header + "0"
	}
	if hasContent {
		header = header + "1"
	} else {
		header = header + "0"
	}
	if cause != nil {
		header = header + "1"
		header = header + cause.Error()
	} else {
		header = header + "0"
	}
	buf := bytes.NewBuffer([]byte{})
	if err = binary.Write(buf, binary.BigEndian, int32(len(header))+int32(len(body))+4); err != nil {
		return
	}
	if err = binary.Write(buf, binary.BigEndian, int32(len(header))); err != nil {
		return
	}
	if n, wErr := buf.WriteString(header); n != len(header) || wErr != nil {
		err = fmt.Errorf("write header failed, length is %d, wrote %d, cause %v", len(header), n, wErr)
		return
	}
	if hasContent {
		if n, wErr := buf.Write(body); n != len(body) || wErr != nil {
			err = fmt.Errorf("write body failed, length is %d, wrote %d, cause %v", len(body), n, wErr)
			return
		}
	}
	p = buf.Bytes()
	return
}

func decodeServiceResult(p []byte) (requestId string, ok bool, v []byte, err error) {
	if p == nil || len(p) == 0 {
		err = fmt.Errorf("empty data bytes")
		return
	}
	buf := bytes.NewBuffer(p)
	var length int32
	var headerLength int32
	if rErr := binary.Read(buf, binary.BigEndian, &length); rErr != nil {
		err = fmt.Errorf("invaild data bytes")
		return
	}
	if rErr := binary.Read(buf, binary.BigEndian, &headerLength); rErr != nil {
		err = fmt.Errorf("invaild header bytes")
		return
	}
	header := make([]byte, int(headerLength))
	if n, rErr := buf.Read(header); n != int(headerLength) || rErr != nil {
		err = fmt.Errorf("read header bytes")
		return
	}

	// header = int int int + string
	requestId = string(header[:22])
	ok = header[22] == 49
	hasPayload := header[23] == 49
	hasCause := header[24] == 49
	if hasCause {
		err = errors.New(string(header[25:]))
	}
	if !hasPayload {
		v = nil
		return
	}
	bodyLength := length - 4 - headerLength
	v = make([]byte, int(bodyLength))
	if n, rErr := buf.Read(v); n != int(bodyLength) || rErr != nil {
		err = fmt.Errorf("read body bytes")
		return
	}
	return
}

// header(requestId + subject) body(v)
func encodeServiceRequest(codec Codec, subject string, requestId string, v interface{}) (p []byte, err error) {
	if len(requestId) != 22 {
		err = fmt.Errorf("invalid requestId, %s", requestId)
		return
	}
	if v == nil {
		v = emptyVoid
	}
	body, bodyEncodeErr := codec.Marshal(v)
	if bodyEncodeErr != nil {
		err = bodyEncodeErr
		return
	}
	header := requestId + subject
	buf := bytes.NewBuffer([]byte{})
	if err = binary.Write(buf, binary.BigEndian, int32(len(header))+int32(len(body))+4); err != nil {
		return
	}
	if err = binary.Write(buf, binary.BigEndian, int32(len(header))); err != nil {
		return
	}
	if n, wErr := buf.WriteString(header); n != len(header) || wErr != nil {
		err = fmt.Errorf("write header failed, length is %d, wrote %d, cause %v", len(header), n, wErr)
		return
	}
	if n, wErr := buf.Write(body); n != len(body) || wErr != nil {
		err = fmt.Errorf("write body failed, length is %d, wrote %d, cause %v", len(body), n, wErr)
		return
	}
	p = buf.Bytes()
	return
}

// header(requestId + subject) body(v)
func decodeServiceRequest(codec Codec, p []byte) (subject string, requestId string, v []byte, err error) {
	if p == nil || len(p) == 0 {
		err = fmt.Errorf("empty data bytes")
		return
	}
	buf := bytes.NewBuffer(p)
	var length int32
	var headerLength int32
	if rErr := binary.Read(buf, binary.BigEndian, &length); rErr != nil {
		err = fmt.Errorf("invaild data bytes")
		return
	}
	if rErr := binary.Read(buf, binary.BigEndian, &headerLength); rErr != nil {
		err = fmt.Errorf("invaild header bytes")
		return
	}

	header := make([]byte, int(headerLength))
	if n, rErr := buf.Read(header); n != int(headerLength) || rErr != nil {
		err = fmt.Errorf("read header bytes")
		return
	}

	bodyLength := length - 4 - headerLength
	if bodyLength > 0 {
		v = make([]byte, int(bodyLength))
		if n, rErr := buf.Read(v); n != int(bodyLength) || rErr != nil {
			err = fmt.Errorf("read body bytes")
			return
		}
	}
	requestId = string(header[:22])
	subject = string(header[22:])
	return
}

func encodeServiceAck(ok bool, err error) (p []byte) {
	data := ""
	if ok {
		data = "1"
	} else {
		data = "0"
	}
	if err != nil {
		data = data + err.Error()
	}
	p = []byte(data)
	return
}

func decodeServiceAck(p []byte) (ok bool, err error) {
	ok = p[0] == 49
	if len(p) > 1 {
		err = fmt.Errorf("%v", string(p[1:]))
	}
	return
}

type natsConn struct {
	conn    *nats.Conn
	codec   Codec
	timeout time.Duration
}

type natsServiceHandler struct {
	subject   string
	requestId string
	conn      *natsConn
}

var natsServiceHandlerPool = &sync.Pool{
	New: func() interface{} {
		return &natsServiceHandler{}
	},
}

func (h *natsServiceHandler) Succeed(result interface{}) (err error) {
	p, err := encodeServiceResult(h.conn.codec, h.requestId, true, result, nil)
	if err != nil {
		failedErr := h.Failed(err)
		if failedErr != nil {
			err = fmt.Errorf("%v, %v", err, failedErr)
		}
		return
	}
	if ack, sendErr := h.conn.conn.Request(h.subject, p, h.conn.timeout); sendErr != nil {
		err = fmt.Errorf("send failed, %v", sendErr)
	} else {
		ok, cause := decodeServiceAck(ack.Data)
		if !ok {
			if cause == nil {
				err = fmt.Errorf("failed, no reason")
			} else {
				err = cause
			}
		}
	}
	return
}

func (h *natsServiceHandler) Failed(cause error) (err error) {
	p, err := encodeServiceResult(h.conn.codec, h.requestId, false, nil, cause)
	if err != nil {
		failedErr := h.Failed(err)
		if failedErr != nil {
			err = fmt.Errorf("%v, %v", err, failedErr)
		}
		return
	}
	if ack, sendErr := h.conn.conn.Request(h.subject, p, h.conn.timeout); sendErr != nil {
		err = fmt.Errorf("send failed, %v", sendErr)
	} else {
		ok, cause := decodeServiceAck(ack.Data)
		if !ok {
			if cause == nil {
				err = fmt.Errorf("failed, no reason")
			} else {
				err = cause
			}
		}
	}
	return
}

type natsServiceResponse struct {
	ok    bool
	data  []byte
	cause error
}

var natsServiceResponsePool = &sync.Pool{
	New: func() interface{} {
		return &natsServiceResponse{}
	},
}

const natsServiceQueueName = "service"

func newResponseMap() *responseChannels {
	return &responseChannels{
		mutex:     new(spinlock),
		responses: make(map[string]chan *natsServiceResponse),
	}
}

type responseChannels struct {
	mutex     sync.Locker
	responses map[string]chan *natsServiceResponse
}

func (m *responseChannels) getWithDelete(name string) chan *natsServiceResponse {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if resp, has := m.responses[name]; has {
		delete(m.responses, name)
		return resp
	}
	return nil
}

func (m *responseChannels) delete(name string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if _, has := m.responses[name]; has {
		delete(m.responses, name)
	}
}

func (m *responseChannels) put(name string, ch chan *natsServiceResponse) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.responses[name] = ch
}

func (m *responseChannels) clean() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for name, responseChan := range m.responses {
		delete(m.responses, name)
		close(responseChan)
	}
}

type natsService struct {
	address         string
	subject         string
	responseSubject string
	wg              *sync.WaitGroup
	responseMap     *responseChannels
	requestQueue    *nats.Subscription
	responseQueue   *nats.Subscription
	local           *localService
}

func (s *natsService) stop() {
	_ = s.requestQueue.Unsubscribe()
	_ = s.responseQueue.Unsubscribe()
	s.responseMap.clean()
	s.wg.Wait()
	return
}

func (s *natsService) request(conn *natsConn, arg interface{}, cb reflect.Value, resultType reflect.Type) (err error) {
	requestId := nuid.Next()
	subject := s.responseSubject
	ch := make(chan *natsServiceResponse, 1)
	s.responseMap.put(requestId, ch)
	p, encodeErr := encodeServiceRequest(conn.codec, subject, requestId, arg)
	if encodeErr != nil {
		s.responseMap.delete(requestId)
		err = encodeErr
		return
	}
	ack, sendErr := conn.conn.Request(s.subject, p, conn.timeout)
	if sendErr != nil {
		s.responseMap.delete(requestId)
		err = sendErr
	}
	if ack == nil {
		s.responseMap.delete(requestId)
		err = fmt.Errorf("empty ack")
		return
	}
	ok, cause := decodeServiceAck(ack.Data)
	if !ok {
		if cause != nil {
			s.responseMap.delete(requestId)
			err = cause
		} else {
			s.responseMap.delete(requestId)
			err = fmt.Errorf("ack failed")
		}
		return
	}

	s.wg.Add(1)
	// TODO CHANGE TO EVENT BUS
	go func(ch chan *natsServiceResponse, wg *sync.WaitGroup, cb reflect.Value, codec Codec) {
		defer wg.Done()
		resp, ok := <-ch
		if !ok {
			return
		}
		respOk := resp.ok
		resultCause := resp.cause
		var resultValue reflect.Value
		if resp.data == nil {
			resultValue = reflect.Zero(resultType)
		} else {
			resultValue = reflect.New(resultType)
			var decodeErr error
			if resultType.Kind() == reflect.Ptr {
				decodeErr = codec.Unmarshal(resp.data, resultValue.Interface())
			} else {
				decodeErr = codec.Unmarshal(resp.data, resultValue.Elem().Interface())
			}
			if decodeErr != nil {
				respOk = false
				if resultCause != nil {
					resultCause = fmt.Errorf("%v, decode result failed, %v", resultCause, decodeErr)
				} else {
					resultCause = decodeErr
				}
			}
		}

		okValue := reflect.ValueOf(respOk)
		var causeValue reflect.Value
		if resultCause != nil {
			causeValue = reflect.ValueOf(resultCause)
		} else {
			causeValue = emptyErrValue
		}

		if resultType.Kind() == reflect.Ptr {
			cb.Call([]reflect.Value{okValue, resultValue.Elem(), causeValue})
		} else {
			cb.Call([]reflect.Value{okValue, resultValue, causeValue})
		}

		resp.ok = false
		resp.data = nil
		resp.cause = nil
		natsServiceResponsePool.Put(resp)
	}(ch, s.wg, cb, conn.codec)

	return
}

func (s *natsService) response(requestId string, ok bool, v []byte, cause error) {
	ch := s.responseMap.getWithDelete(requestId)
	if ch == nil {
		logger.Warnf("can not fetch response chan, %s", requestId)
		return
	}
	resp, respOk := natsServiceResponsePool.Get().(*natsServiceResponse)
	if !respOk {
		logger.Warnf("get natsServiceResponse from pool failed")
		resp = &natsServiceResponse{}
	}
	resp.ok = ok
	resp.cause = cause
	resp.data = v
	ch <- resp
	close(ch)
}

func (s *natsService) listenRequest(conn *natsConn) {
	requestQueue, subErr := conn.conn.QueueSubscribe(s.subject, natsServiceQueueName, func(msg *nats.Msg) {
		// decode
		subject, requestId, argBytes, decodeErr := decodeServiceRequest(conn.codec, msg.Data)
		if decodeErr != nil {
			if err := msg.Respond(encodeServiceAck(false, decodeErr)); err != nil {
				logger.Warnf("send ack failed, discard service call, %v", err)
			}
			return
		}
		arg := reflect.New(s.local.argType)
		var decodeArgErr error
		if s.local.argType.Kind() == reflect.Ptr {
			decodeArgErr = conn.codec.Unmarshal(argBytes, arg.Interface())
		} else {
			decodeArgErr = conn.codec.Unmarshal(argBytes, arg.Elem().Interface())
		}
		if decodeArgErr != nil {
			if err := msg.Respond(encodeServiceAck(false, decodeArgErr)); err != nil {
				logger.Warnf("send ack failed, discard service call, %v", decodeArgErr)
			}
			return
		}
		if err := msg.Respond(encodeServiceAck(true, nil)); err != nil {
			logger.Warnf("send ack failed, discard service call, %v", err)
			return
		}
		handler, ok := natsServiceHandlerPool.Get().(*natsServiceHandler)
		if !ok {
			panic("snailx: get service handler from pool failed, bad type")
		}
		handler.subject = subject
		handler.conn = conn
		handler.requestId = requestId
		if s.local.argType.Kind() == reflect.Ptr {
			s.local.call(arg.Elem().Interface(), handler)
		} else {
			s.local.call(arg.Interface(), handler)
		}
		natsServiceHandlerPool.Put(handler)

	})
	if subErr != nil {
		panic(fmt.Errorf("snailx: build remote service failed, %v", subErr))
	}
	s.requestQueue = requestQueue

}

func (s *natsService) listenResponse(conn *natsConn) {
	responseQueue, subErr := conn.conn.QueueSubscribe(s.responseSubject, natsServiceQueueName, func(msg *nats.Msg) {
		requestId, ok, result, cause := decodeServiceResult(msg.Data)
		if requestId == "" && cause != nil {
			if err := msg.Respond(encodeServiceAck(false, cause)); err != nil {
				logger.Warnf("send ack failed, %s", err)
			}
			return
		}
		if err := msg.Respond(encodeServiceAck(true, nil)); err != nil {
			logger.Warnf("send ack failed, %s", err)
		}
		s.response(requestId, ok, result, cause)
	})
	if subErr != nil {
		panic(fmt.Errorf("snailx: build remote service failed, %v", subErr))
	}
	s.responseQueue = responseQueue
}

func newNatsServiceGroup(conn *nats.Conn, codec Codec, timeout time.Duration) ServiceGroup {
	return &natsServiceGroup{
		conn: &natsConn{
			conn:    conn,
			codec:   codec,
			timeout: timeout,
		},
		locals: &localServiceGroup{
			mutex:    new(sync.RWMutex),
			services: make(map[string]*localService),
		},
		remotes: make(map[string]*natsService),
	}
}

type natsServiceGroup struct {
	conn    *natsConn
	locals  *localServiceGroup
	remotes map[string]*natsService
}

func buildNatsServiceSubject(address string) string {
	return fmt.Sprintf("_snailx.service.%s", address)
}

func buildNatsServiceResponseSubject(address string) string {
	return fmt.Sprintf("_snailx.service.%s.response", address)
}

func (s *natsServiceGroup) Deploy(address string, service Service) (err error) {
	if err = s.locals.Deploy(address, service); err != nil {
		return
	}
	s.locals.mutex.Lock()
	defer s.locals.mutex.Unlock()
	natsService := &natsService{
		address:         address,
		subject:         buildNatsServiceSubject(address),
		responseSubject: buildNatsServiceResponseSubject(address),
		wg:              new(sync.WaitGroup),
		responseMap:     newResponseMap(),
		local:           s.locals.services[address],
	}
	natsService.listenRequest(s.conn)
	natsService.listenResponse(s.conn)
	s.remotes[address] = natsService
	return
}

func (s *natsServiceGroup) UnDeploy(address string) (err error) {
	err = s.locals.UnDeploy(address)
	if err != nil {
		return
	}
	s.locals.mutex.Lock()
	defer s.locals.mutex.Unlock()
	if service, has := s.remotes[address]; has {
		delete(s.remotes, address)
		service.stop()

	}
	return
}

func (s *natsServiceGroup) UnDeployAll() {
	s.locals.UnDeployAll()

	s.locals.mutex.Lock()
	defer s.locals.mutex.Unlock()
	addresses := make([]string, 0, len(s.remotes))
	for address := range s.remotes {
		addresses = append(addresses, address)
	}
	for _, address := range addresses {
		if service, has := s.remotes[address]; has {
			delete(s.remotes, address)
			service.stop()
		}
	}
	return
}

func (s *natsServiceGroup) Invoke(address string, arg interface{}, cb ServiceCallback, local ...bool) {
	if local != nil && len(local) > 0 && local[0] {
		s.locals.Invoke(address, arg, cb, local...)
		return
	}
	cbType := reflect.TypeOf(cb)
	if cbType.Kind() != reflect.Func {
		panic("snailx: cb needs to be a func")
	}
	if cbType.NumIn() != 3 {
		panic("snailx: cb needs 3 parameters, first type is bool, second type is the service result type, last type is error")
	}
	okType := cbType.In(0)
	if okType.Kind() != reflect.Bool {
		panic("snailx: cb needs 3 parameters, first type is bool, second type is the service result type, last type is error")
	}
	resultType := cbType.In(1)
	errType := cbType.In(2)
	if errType.Name() != "error" {
		panic("snailx: cb needs 3 parameters, first type is bool, second type is the service result type, last type is error")
	}
	s.locals.mutex.RLock()
	defer s.locals.mutex.RUnlock()
	service, hasService := s.remotes[address]
	if hasService {
		err := service.request(s.conn, arg, reflect.ValueOf(cb), resultType)
		if err != nil {
			reflect.ValueOf(cb).Call([]reflect.Value{reflect.ValueOf(false), reflect.Zero(resultType), reflect.ValueOf(err)})
		}
	} else {
		s.locals.mutex.Lock()
		natsService := &natsService{
			address:         address,
			subject:         buildNatsServiceSubject(address),
			responseSubject: buildNatsServiceResponseSubject(address),
			wg:              new(sync.WaitGroup),
			responseMap:     newResponseMap(),
			local:           s.locals.services[address],
		}
		natsService.listenResponse(s.conn)
		s.remotes[address] = natsService
		s.locals.mutex.Unlock()
		err := service.request(s.conn, arg, reflect.ValueOf(cb), resultType)
		if err != nil {
			reflect.ValueOf(cb).Call([]reflect.Value{reflect.ValueOf(false), reflect.Zero(resultType), reflect.ValueOf(err)})
		}
	}
	return
}
