package snailx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"
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
func decodeServiceRequest(p []byte) (subject string, requestId string, v []byte, err error) {
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
		responses: new(sync.Map),
	}
}

type responseChannels struct {
	responses *sync.Map
}

func (m *responseChannels) getWithDelete(name string) chan *natsServiceResponse {
	logger.Debugf("gggggggg %v %v %v", name, unsafe.Pointer(m.responses), unsafe.Pointer(m))
	if value, has := m.responses.Load(name); has {
		resp, _ := value.(chan *natsServiceResponse)
		m.responses.Delete(name)
		return resp
	} else {
		logger.Debugf("xxxxxx %v %v %v", name, unsafe.Pointer(m.responses), m.responses)
	}
	return nil
}

func (m *responseChannels) delete(name string) {
	m.responses.Delete(name)
}

func (m *responseChannels) put(name string, ch chan *natsServiceResponse) {
	m.responses.Store(name, ch)
	logger.Debugf("mmmmmmmm %v %v", unsafe.Pointer(m.responses), unsafe.Pointer(m))
}

func (m *responseChannels) clean() {
	rs := make([]string, 0, 1)
	m.responses.Range(func(key, value interface{}) bool {
		k, _ := key.(string)
		rs = append(rs, k)
		resp, _ := value.(chan *natsServiceResponse)
		close(resp)
		return true
	})
	for _, name :=  range rs {
		m.responses.Delete(name)
	}
}

func newNatsService(address string, conn *natsConn, local *localService) (s *natsService, err error) {
	eventLoop := newNatsServiceResponseEventLoop(runtime.NumCPU() * 128)
	if err = eventLoop.start(); err != nil {
		return
	}
	s = &natsService{
		address:         address,
		subject:         buildNatsServiceSubject(address),
		responseSubject: buildNatsServiceResponseSubject(address),
		wg:              new(sync.WaitGroup),
		responseMap:     newResponseMap(),
		eventLoop:       eventLoop,
	}
	if local != nil {
		s.local = local
		s.listenRequest(conn)
	}
	s.listenResponse(conn)
	return
}

func newNatsServiceMap() *natsServiceMap {
	return &natsServiceMap{
		services: new(sync.Map),
	}
}

type natsServiceMap struct {
	services *sync.Map
}

func (m *natsServiceMap) put(name string, service *natsService) {
	m.services.Store(name, service)
}
func (m *natsServiceMap) get(name string) *natsService {
	value , ok := m.services.Load(name)
	if !ok {
		return nil
	}
	service, _ := value.(*natsService)
	return service
}


func (m *natsServiceMap) getOrStore(name string, conn *natsConn, local *localService) *natsService {
	service := &natsService{
		address:         name,
		subject:         buildNatsServiceSubject(name),
		responseSubject: buildNatsServiceResponseSubject(name),
		wg:              new(sync.WaitGroup),
	}
	value , ok := m.services.LoadOrStore(name, service)

	if ok {
		service = nil
		service, _ = value.(*natsService)
		logger.Debugf("nnnnn %v %v", ok, unsafe.Pointer(service))
		return service
	}
	logger.Debugf("nnnnn %v %v", ok, unsafe.Pointer(service))
	service.responseMap = newResponseMap()
	eventLoop := newNatsServiceResponseEventLoop(runtime.NumCPU() * 128)
	if err := eventLoop.start(); err != nil {
		panic(fmt.Errorf("snalix: new service failed. %v", err))
		return nil
	}
	service.eventLoop = eventLoop
	if local != nil {
		service.local = local
		service.listenRequest(conn)
	}
	logger.Debugf("555555555", unsafe.Pointer(m))
	service.listenResponse(conn)
	return service
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
	eventLoop       *natsServiceResponseEventLoop
}

func (s *natsService) stop() {
	_ = s.requestQueue.Unsubscribe()
	_ = s.responseQueue.Unsubscribe()
	_ = s.eventLoop.stop()
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
		logger.Errorf("1 %v", encodeErr)
		s.responseMap.delete(requestId)
		err = encodeErr
		return
	}
	ack, sendErr := conn.conn.Request(s.subject, p, conn.timeout)
	if sendErr != nil {
		logger.Errorf("2 %v", sendErr)
		s.responseMap.delete(requestId)
		err = sendErr
	}
	if ack == nil {
		logger.Errorf("3 %v", ack)
		s.responseMap.delete(requestId)
		err = fmt.Errorf("empty ack")
		return
	}
	ok, cause := decodeServiceAck(ack.Data)
	if !ok {
		if cause != nil {
			logger.Errorf("4 %v", cause)
			s.responseMap.delete(requestId)
			err = cause
		} else {
			logger.Errorf("5 %v", cause)
			s.responseMap.delete(requestId)
			err = fmt.Errorf("ack failed")
		}
		return
	}

	err = s.eventLoop.send(ch, s.wg, cb, conn.codec, resultType)
	return
}

func (s *natsService) response(requestId string, ok bool, v []byte, cause error) {
	logger.Debugf("rsss2 %v %v", &s, unsafe.Pointer(s.responseMap.responses))
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
		subject, requestId, argBytes, decodeErr := decodeServiceRequest(msg.Data)
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
	logger.Debugf("listen response 1 %v %v %v", s.subject, unsafe.Pointer(s), unsafe.Pointer(s.responseMap))
	responseQueue, subErr := conn.conn.QueueSubscribe(s.responseSubject, natsServiceQueueName, func(msg *nats.Msg) {
		logger.Debugf("listen response 2 %v %v %v",s.subject, unsafe.Pointer(s), unsafe.Pointer(s.responseMap))
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
	//ch := make(chan *invokeEvent, runtime.NumCPU() * 128)
	//group := &natsServiceGroup{
	//	conn: &natsConn{
	//		conn:    conn,
	//		codec:   codec,
	//		timeout: timeout,
	//	},
	//	locals: &localServiceGroup{
	//		mutex:    new(sync.RWMutex),
	//		services: make(map[string]*localService),
	//	},
	//	remotes: newNatsServiceMap(),
	//	//mutex:&spinlock{},
	//	ch:ch,
	//	//responseMap:newResponseMap(),
	//}
	//group.listenInvoke()
	//return group
	return nil
}




func (s *natsServiceGroup) Deploy(address string, service Service) (err error) {
	if err = s.locals.Deploy(address, service); err != nil {
		return
	}
	s.locals.mutex.Lock()
	defer s.locals.mutex.Unlock()
	eventLoop := newNatsServiceResponseEventLoop(runtime.NumCPU() * 128)
	if err = eventLoop.start(); err != nil {
		return
	}
	//natsService, newServiceErr := newNatsService(address, s.conn, s.locals.services[address])
	//if newServiceErr != nil {
	//	err = newServiceErr
	//	return
	//}
	//s.remotes.put(address, natsService)
	return
}

func (s *natsServiceGroup) UnDeploy(address string) (err error) {
	err = s.locals.UnDeploy(address)
	if err != nil {
		return
	}
	s.locals.mutex.Lock()
	defer s.locals.mutex.Unlock()
	service := s.remotes.get(address)
	service.stop()
	s.remotes.services.Delete(address)

	return
}

func (s *natsServiceGroup) UnDeployAll() {
	s.locals.UnDeployAll()
	s.locals.mutex.Lock()
	defer s.locals.mutex.Unlock()
	s.remotes.services.Range(func(key, value interface{}) bool {
		service, _ := value.(*natsService)
		service.stop()
		s.remotes.services.Delete(key)
		return true
	})
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
	//resultType := cbType.In(1)
	//errType := cbType.In(2)
	//if errType.Name() != "error" {
	//	panic("snailx: cb needs 3 parameters, first type is bool, second type is the service result type, last type is error")
	//}

	//s.mutex.Lock()
	//defer s.mutex.Unlock()
	//service, hasService := s.remotes[address]
	//logger.Debugf("old %v %v %v %v", hasService, &service, &s.remotes, address)
	//if hasService {
	//	//s.mutex.RUnlock()
	//	err := s.remotes[address].request(s.conn, arg, reflect.ValueOf(cb), resultType)
	//	if err != nil {
	//		reflect.ValueOf(cb).Call([]reflect.Value{reflect.ValueOf(false), reflect.Zero(resultType), reflect.ValueOf(err)})
	//	}
	//} else {
	//	//s.mutex.RUnlock()
	//	//s.mutex.Lock()
	//	//s.cond.Wait()
	//
	//	natsService, newServiceErr := newNatsService(address, s.conn, nil)
	//	if newServiceErr != nil {
	//		s.mutex.Unlock()
	//		s.cond.Broadcast()
	//		panic("snailx: create remote service failed")
	//	}
	//	logger.Debugf("new %v", &natsService)
	//	s.remotes[address] = natsService
	//	//s.mutex.Unlock()
	//	//s.cond.Broadcast()
	//	err := natsService.request(s.conn, arg, reflect.ValueOf(cb), resultType)
	//	if err != nil {
	//		reflect.ValueOf(cb).Call([]reflect.Value{reflect.ValueOf(false), reflect.Zero(resultType), reflect.ValueOf(err)})
	//	}
	//}
	return
}


