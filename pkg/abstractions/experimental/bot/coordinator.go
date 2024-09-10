package bot

import (
	"context"
)

/*

What does the coordinator do?
	- maintains & updates the state of the bot instance
	-
*/

type BotCoordinator struct {
	ctx context.Context
}

func NewBotCoordinator(ctx context.Context) (*BotCoordinator, error) {
	return &BotCoordinator{
		ctx: ctx,
	}, nil
}

func (c *BotCoordinator) Start() {

}
