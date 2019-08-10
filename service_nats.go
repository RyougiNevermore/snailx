package snailx

import "fmt"

func buildNatsServiceSubject(address string) string {
	return fmt.Sprintf("_snailx.service.%s", address)
}

func buildNatsServiceResponseSubject(address string) string {
	return fmt.Sprintf("_snailx.service.%s.response", address)
}

type natsServiceGroup struct {
	connPool    *NatsConnections
	locals  *localServiceGroup
	remotes *natsServiceMap
}




