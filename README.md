## SnailX is a tool-kit for building **reactive** applications with **Golang**

> SnailX is Asynchronous

SnailX is *event driven* and *non blocking*. This means your app can handle a lot of concurrency using a small number of kernel threads. SnailX  lets your app scale with minimal hardware.

> SnailX is Fun

Unlike restrictive traditional application containers, SnailX  gives you incredible power and agility to create compelling, scalable, 21st century applications the way you want to, with a minimum of fuss, in the language you want.



## Install

```go
go get github.com/pharosnet/snailx
```

## Basic Usage

* snailx

  ```go
  // using snailx
  x := snailx.New()
  // default service bus, event loop type
  x.Deploy(&BookSnail{})
  // worker type service bus
  x.DeployWithOptions(&BookConsumerSnail{}, snailx.SnailOptions{ServiceBusKind:snailx.WorkerServiceBus, WorkersNum:4})
  // graceful shutdown
  sigint := make(chan os.Signal,1)
  signal.Notify(sigint, syscall.SIGINT, syscall.SIGTERM)
  sig := <-sigint
  fmt.Println("sigint:", sig)
  // call snalix stop
  if err := x.Stop(); err != nil {
      fmt.Println("stop failed", err)
  }
  ```

* snailx cluster

  ```go
  c, ncErr := nats.Connect(nats.DefaultURL)
  if ncErr != nil {
  	fmt.Println(ncErr)
  	return  
  }
  // set logger default is warn level
  snailx.SetLogger(snailx.Newlogger(snailx.LoggerLevelDebug))
  // using snailx
  x := snailx.NewNatsCluster(c, snailx.NewMsgPackCodec(), 2 * time.Second)
  // default service bus, event loop type
  x.Deploy(&BookSnail{})
  // worker type service bus
  x.DeployWithOptions(&BookConsumerSnail{}, snailx.SnailOptions{ServiceBusKind:snailx.WorkerServiceBus, WorkersNum:4})
  // graceful shutdown
  sigint := make(chan os.Signal,1)
  signal.Notify(sigint, syscall.SIGINT, syscall.SIGTERM)
  sig := <-sigint
  fmt.Println("sigint:", sig)
  // call snalix stop
  if err := x.Stop(); err != nil {
      fmt.Println("stop failed", err)
  }
  ```
  

* snail app

  ```go
  // implement snailx.Snail
  type BookSnail struct {
  	bus snailx.ServiceBus
  }
  
  func (s *BookSnail) SetServiceBus(bus snailx.ServiceBus) {
  	s.bus = bus
  }
  
  func (s *BookSnail) Start()  {
  	// deploy service to service bus
  	// the service address must be identity
  	if err := s.bus.Deploy("helloWorkService", HelloWorkService); err != nil {
  		fmt.Println("deploy service failed", err)
  		return
  	}
  }
  
  func (s *BookSnail) Stop()  {
  	if err := s.bus.UnDeploy("helloWorkService"); err != nil {
  		fmt.Println("unDeploy service failed", err)
  		return
  	}
  }
  ```

  ```go
  // implement snailx.Snail
  type BookConsumerSnail struct {
  	bus snailx.ServiceBus
  }
  
  func (s *BookConsumerSnail) SetServiceBus(bus snailx.ServiceBus) {
  	s.bus = bus
  }
  
  func (s *BookConsumerSnail) Start()  {
  	// invoke service which is deployed to the service bus
  	// the first param is service address
  	// the second param must be a func which has three param (the first must be bool type, the second must be service result type, the last must be error type)
  	if err := s.bus.Invoke("helloWorkService", &BookGetArg{Name:"book name"}, func(ok bool, book *Book, err error) {
  		fmt.Println("ok:", ok, "book:", book, "err:", err)
  	}); err != nil {
  		fmt.Println("invoke service failed", err)
  	}
  }
  
  func (s *BookConsumerSnail) Stop()  {}
  ```

  

* service func

  ```go
  type BookGetArg struct {
  	Name string
  }
  
  type Book struct {
  	Id string
  	Name string
  	PublishDate time.Time
  }
  
  // service function, the type of param is must be noted.
  // the first could be every prt type
  // the second must be *snailx.ServiceHandler
  var HelloWorkService = func(arg *BookGetArg, handler snailx.ServiceHandler) {
  	bookName := arg.Name
  	book := &Book{
  		Id: "some id",
  		Name:bookName,
  		PublishDate:time.Now(),
  	}
  	// return succeeded result
  	handler.Succeed(book)
  	// return failed cause
  	//handler.Failed(fmt.Errorf("some cause"))
  }
  ```



## Todo

- [X] cluster
- [ ] snail http
- [ ] snail pg client
- [ ] snail net

