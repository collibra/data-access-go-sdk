package internal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/raito-io/sdk-go/types"
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

	outputChannel := PaginationExecutor(ctx, mockLoadPageFn, mockEdgeFn)

	var items []string
	for listItem := range outputChannel {
		if listItem.HasError() {
			t.Errorf("Error encountered: %v", listItem.GetError())
			return
		}
		items = append(items, listItem.MustGetItem())
	}

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

	outputChannel := PaginationExecutor(ctx, mockLoadPageFn, mockEdgeFn)

	for listItem := range outputChannel {
		if !listItem.HasError() {
			t.Error("Expected error, but none received")
			return
		}
		if listItem.GetError() != expectedErr {
			t.Errorf("Expected error: %v, got: %v", expectedErr, listItem.GetError())
			return
		}
	}
}

func testPaginationExecutorEdgeFnError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("edgeFn error")
	mockLoadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []int, error) {
		pageInfo := &types.PageInfo{HasNextPage: boolPtr(true)} // Adjust as needed
		edges := []int{1, 2, 3}                                 // Adjust as needed
		return pageInfo, edges, nil
	}
	mockEdgeFn := func(edge *int) (*string, *string, error) {
		return nil, nil, expectedErr
	}

	outputChannel := PaginationExecutor(ctx, mockLoadPageFn, mockEdgeFn)

	for listItem := range outputChannel {
		if !listItem.HasError() {
			t.Error("Expected error, but none received")
			return
		}
		if listItem.GetError() != expectedErr {
			t.Errorf("Expected error: %v, got: %v", expectedErr, listItem.GetError())
			return
		}
	}
}

func testPaginationExecutorCancel(t *testing.T) {
	ctx := context.Background()
	cancelCtx, cancelFn := context.WithCancel(ctx)

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

	outputChannel := PaginationExecutor(cancelCtx, mockLoadPageFn, mockEdgeFn)

	var items []string
	for listItem := range outputChannel {
		if listItem.HasError() {
			t.Errorf("Error encountered: %v", listItem.GetError())
			return
		}
		items = append(items, listItem.MustGetItem())

		if len(items) > 4 {
			cancelFn()
		}
	}

	assert.Equal(t, []string{"item 0", "item 1", "item 2", "item 3", "item 4"}, items)

}

// Utility function to get a pointer to bool
func boolPtr(b bool) *bool {
	return &b
}
