package agent

import (
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
)

type Command struct {
	Ctx        []byte
	Repository string
	Argv       []string
}

func newCommand(ctx *context.Context, repo *repository.Repository, argv []string) Command {
	ctxBytes, err := ctx.ToBytes()
	if err != nil {
		panic(err)
	}
	return Command{Ctx: ctxBytes, Repository: repo.Location(), Argv: argv}
}

type Response struct {
	Type string
	Data []byte
}

func newError(err error) Response {
	if err != nil {
		return Response{Type: "error", Data: []byte(err.Error())}
	}
	return Response{Type: "error", Data: []byte{}}
}

func newStdout(msg string) Response {
	return Response{Type: "stdout", Data: []byte(msg)}
}

func newStderr(msg string) Response {
	return Response{Type: "stderr", Data: []byte(msg)}
}
