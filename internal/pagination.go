package internal

import (
	"context"
	"iter"

	"github.com/collibra/access-governance-go-sdk/types"
)

func PaginationExecutor[T any, E any](ctx context.Context, loadPageFn func(ctx context.Context, cursor *string) (*types.PageInfo, []E, error), edgeFn func(edge *E) (*string, *T, error)) iter.Seq2[*T, error] {
	return func(yield func(*T, error) bool) {
		var (
			hasNext    = true
			lastCursor *string
			edges      []E
			edgeIndex  int
		)

		for {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}

			if edgeIndex >= len(edges) {
				if !hasNext {
					return
				}

				// Fetch the next page of data.
				pageInfo, newEdges, err := loadPageFn(ctx, lastCursor)
				if err != nil {
					yield(nil, err)
					return
				}

				// Reset state for the new page.
				edges = newEdges
				edgeIndex = 0

				// Determine if there is a subsequent page.
				hasNext = pageInfo != nil && pageInfo.HasNextPage != nil && *pageInfo.HasNextPage

				// If the new page is empty, loop again to fetch the next one or exit.
				if len(edges) == 0 {
					continue
				}
			}

			// Process the current item (edge) from the page.
			edge := &edges[edgeIndex]
			edgeIndex++ // Move to the next item for the next iteration.

			cursor, item, err := edgeFn(edge)
			if err != nil {
				yield(nil, err)
				return
			}

			// Update the cursor for the next page load.
			if cursor != nil {
				lastCursor = cursor
			}

			// Skip nil items, as in the original function.
			if item == nil {
				continue
			}

			// Yield the processed item. If yield returns false, the consumer
			// has stopped iterating, so we should stop as well.
			if !yield(item, nil) {
				return
			}
		}
	}
}
