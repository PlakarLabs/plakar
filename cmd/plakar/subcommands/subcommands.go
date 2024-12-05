package subcommands

import (
	"fmt"

	"github.com/PlakarKorp/plakar/agent"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
)

var subcommands map[string]func(*context.Context, *repository.Repository, []string) int = make(map[string]func(*context.Context, *repository.Repository, []string) int)

func Register(command string, fn func(*context.Context, *repository.Repository, []string) int) {
	subcommands[command] = fn
}

func Execute(rpc bool, ctx *context.Context, repo *repository.Repository, command string, args []string) (int, error) {
	if rpc {
		err := agent.NewRPC(ctx, repo, append([]string{command}, args...))
		if err != nil {
			return 1, err
		}
		return 0, nil
	}

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
