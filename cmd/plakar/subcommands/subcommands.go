package subcommands

import (
	"fmt"

	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/repository"
)

var subcommands map[string]func(*context.Context, *repository.Repository, []string) int = make(map[string]func(*context.Context, *repository.Repository, []string) int)

func Register(command string, fn func(*context.Context, *repository.Repository, []string) int) {
	subcommands[command] = fn
}

func Execute(ctx *context.Context, repo *repository.Repository, command string, args []string) (int, error) {
	fn, exists := subcommands[command]
	if !exists {
		return 1, fmt.Errorf("unknown command: %s", command)
	}
	return fn(ctx, repo, args), nil
}

func List() []string {
	var list []string
	for command := range subcommands {
		list = append(list, command)
	}
	return list
}
