package internal

import (
	"context"

	"github.com/raito-io/sdk-go/types"
)

func PaginationExecutor[T any, E any](ctx context.Context, loadPageFn func(ctx context.Context, cursor *string) (*types.PageInfo, []E, error), edgeFn func(edge *E) (*string, *T, error)) <-chan types.ListItem[T] {
	outputChannel := make(chan types.ListItem[T])

	go func() {
		defer close(outputChannel)

		hasNext := true
		var lastCursor *string

		for hasNext {
			select {
			case <-ctx.Done():
				return
			default:
				pageInfo, edges, err := loadPageFn(ctx, lastCursor)
				if err != nil {
					putOnChannel(ctx, types.NewListItemError[T](err), outputChannel)

					return
				}

				for i := range edges {
					cursor, item, edgeErr := edgeFn(&edges[i])
					if edgeErr != nil {
						putOnChannel(ctx, types.NewListItemError[T](edgeErr), outputChannel)

						return
					}

					if cursor != nil {
						lastCursor = cursor
					}

					if item == nil {
						continue
					}

					ctxDone := putOnChannel(ctx, types.NewListItemItem(item), outputChannel)
					if ctxDone {
						return
					}
				}

				hasNext = pageInfo != nil && pageInfo.HasNextPage != nil && *pageInfo.HasNextPage
			}
		}
	}()

	return outputChannel
}

func putOnChannel[T any](ctx context.Context, item T, outputChannel chan<- T) bool {
	select {
	case <-ctx.Done():
		return true
	case outputChannel <- item:
		return false
	}
}
