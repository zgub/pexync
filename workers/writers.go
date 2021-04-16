package workers

import (
	"context"

	"github.com/google/uuid"
	"github.com/zgub/pexync/core"
)

type FileWriter struct {
	ctx      context.Context
	inbox    <-chan *core.Message
	senderID uuid.UUID
}

func NewFileWriter(ctx context.Context, inbox <-chan *core.Message) FileWriter {
	return FileWriter{
		ctx:   ctx,
		inbox: inbox,
	}
}
