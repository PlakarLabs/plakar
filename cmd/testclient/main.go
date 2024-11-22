package main

import (
	"context"
	"fmt"

	"github.com/PlakarKorp/go-plakar-sdk/pkg/importer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := "localhost:50051"
	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := importer.NewImporterClient(conn)

	origin, err := client.Origin(context.Background(), &importer.OriginRequest{})
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v\n", origin.Origin)

}
