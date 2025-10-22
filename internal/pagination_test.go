package internal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/collibra/data-access-go-sdk/types"
)

func TestPaginationExecutor(t *testing.T) {
	t.Run("TestPaginationExecutor_Success", testPaginationExecutorSuccess)
	t.Run("TestPaginationExecutor_LoadPageError", testPaginationExecutorLoadPageError)
	t.Run("TestPaginationExecutor_EdgeFnError", testPaginationExecutorEdgeFnError)
	t.Run("TestPaginationExecutor_ExecutorCancel", testPaginationExecutorCancel)
}

func testPaginationExecutorSuccess(t *testing.T) {
	ctx := context.Background()

	mockLoadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []int, error) {
		pageNr := 0

		if cursor != nil {
			cursorId, _ := strconv.Atoi(*cursor)
			pageNr = (cursorId / 3) + 1
		}

		if pageNr < 2 {
			pageInfo := &types.PageInfo{HasNextPage: boolPtr(true)}
			pageOffset := 3 * pageNr
			edges := []int{pageOffset, pageOffset + 1, pageOffset + 2}

			return pageInfo, edges, nil
		} else {
			pageInfo := &types.PageInfo{HasNextPage: boolPtr(false)}
			pageOffset := 3 * pageNr
			edges := []int{pageOffset, pageOffset + 1}
			return pageInfo, edges, nil
		}

	}
	mockEdgeFn := func(edge *int) (*string, *string, error) {
		cursor := fmt.Sprintf("%d", *edge)
		item := fmt.Sprintf("item %d", *edge)

		return &cursor, &item, nil
	}

	iterator := PaginationExecutor(ctx, mockLoadPageFn, mockEdgeFn)

	var items []string
	iterator(func(item *string, err error) bool {
		if err != nil {
			t.Errorf("Error encountered: %v", err)
			return false
		}
		items = append(items, *item)
		return true
	})

	assert.Equal(t, []string{"item 0", "item 1", "item 2", "item 3", "item 4", "item 5", "item 6", "item 7"}, items)
}

func testPaginationExecutorLoadPageError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("loadPage error")
	mockLoadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []int, error) {
		return nil, nil, expectedErr
	}
	mockEdgeFn := func(edge *int) (*string, *string, error) {
		return nil, nil, nil
	}

	iterator := PaginationExecutor(ctx, mockLoadPageFn, mockEdgeFn)

	var receivedError error
	var itemsReceived int
	iterator(func(item *string, err error) bool {
		if err != nil {
			receivedError = err
			return false
		}
		itemsReceived++
		return true
	})

	assert.ErrorIs(t, receivedError, expectedErr)
	assert.Equal(t, 0, itemsReceived)
}

func testPaginationExecutorEdgeFnError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("edgeFn error")
	mockLoadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []int, error) {
		pageInfo := &types.PageInfo{HasNextPage: boolPtr(true)}
		edges := []int{1, 2, 3}
		return pageInfo, edges, nil
	}
	mockEdgeFn := func(edge *int) (*string, *string, error) {
		return nil, nil, expectedErr
	}

	iterator := PaginationExecutor(ctx, mockLoadPageFn, mockEdgeFn)

	var receivedError error
	var itemsReceived int
	iterator(func(item *string, err error) bool {
		if err != nil {
			receivedError = err
			return false
		}
		itemsReceived++
		return true
	})

	assert.ErrorIs(t, receivedError, expectedErr)
	assert.Equal(t, 0, itemsReceived)
}

func testPaginationExecutorCancel(t *testing.T) {
	ctx := context.Background()
	cancelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	mockLoadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []int, error) {
		pageNr := 0

		if cursor != nil {
			cursorId, _ := strconv.Atoi(*cursor)
			pageNr = (cursorId / 3) + 1
		}

		if pageNr < 2 {
			pageInfo := &types.PageInfo{HasNextPage: boolPtr(true)}
			pageOffset := 3 * pageNr
			edges := []int{pageOffset, pageOffset + 1, pageOffset + 2}

			return pageInfo, edges, nil
		} else {
			pageInfo := &types.PageInfo{HasNextPage: boolPtr(false)}
			pageOffset := 3 * pageNr
			edges := []int{pageOffset, pageOffset + 1}
			return pageInfo, edges, nil
		}

	}
	mockEdgeFn := func(edge *int) (*string, *string, error) {
		cursor := fmt.Sprintf("%d", *edge)
		item := fmt.Sprintf("item %d", *edge)

		return &cursor, &item, nil
	}

	iterator := PaginationExecutor(cancelCtx, mockLoadPageFn, mockEdgeFn)

	var items []string
	var errEncountered error
	iterator(func(item *string, err error) bool {
		if err != nil {
			errEncountered = err
			return false
		}
		items = append(items, *item)

		if len(items) == 5 {
			cancelFn()
		}
		return true
	})

	assert.Equal(t, []string{"item 0", "item 1", "item 2", "item 3", "item 4"}, items)
	assert.ErrorIs(t, errEncountered, context.Canceled)
}

// Utility function to get a pointer to bool
func boolPtr(b bool) *bool {
	return &b
}
