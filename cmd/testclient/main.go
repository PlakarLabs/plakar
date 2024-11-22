package main

import (
	"context"
	"errors"
	"fmt"
	"io"

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

	info, err := client.Info(context.Background(), &importer.InfoRequest{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Importer type: %v\n", info.Type)
	fmt.Printf("Importer origin: %v\n", info.Origin)
	fmt.Printf("Importer root: %v\n", info.Root)

	scanStream, err := client.Scan(context.Background(), &importer.ScanRequest{})
	if err != nil {
		panic(err)
	}
	for {
		resp, err := scanStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}
		if scanError := resp.GetError(); scanError != nil {
			fmt.Printf("[ERROR] %s: %s\n", resp.Pathname, scanError.GetMessage())
		} else if record := resp.GetRecord(); record != nil {
			fmt.Printf("[OK] %s: %v\n", resp.Pathname, record.Type)
		} else {
			panic("?? unexpected response")
		}
	}
}
