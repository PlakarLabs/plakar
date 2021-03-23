package main

import (
	"log"
	"os"

	"github.com/poolpOrg/plakar/repository"
)

func cmd_push(store repository.Store, args []string) {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	snapshot := store.Transaction().Snapshot()
	if len(args) == 0 {
		snapshot.Push(dir)
	} else {
		for i := 0; i < len(args); i++ {
			snapshot.Push(args[i])
		}
	}
	snapshot.Commit()
}
