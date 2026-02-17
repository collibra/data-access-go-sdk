package services

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/Khan/genqlient/graphql"

	"github.com/collibra/data-access-go-sdk/internal"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/types"
)

type JobClient struct {
	client graphql.Client
}

func NewJobClient(client graphql.Client) *JobClient {
	return &JobClient{
		client: client,
	}
}

// GetJob returns a job by its ID.
func (c *JobClient) GetJob(ctx context.Context, id string) (*types.Job, error) {
	result, err := schema.GetJob(ctx, c.client, id)
	if err != nil {
		return nil, types.NewErrClient(err)
	}
	return &result.Job.Job, nil
}

type JobListOptions struct {
	filter *types.JobsFilter
}

// WithJobListFilter sets the filter for the job list.
func WithJobListFilter(filter *types.JobsFilter) func(options *JobListOptions) {
	return func(options *JobListOptions) {
		options.filter = filter
	}
}

// ListJobs returns a channel that will receive the list of jobs.
func (c *JobClient) ListJobs(ctx context.Context, ops ...func(*JobListOptions)) iter.Seq2[*types.Job, error] {
	options := JobListOptions{}
	for _, op := range ops {
		op(&options)
	}

	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.JobConnectionEdgesJobEdge, error) {
		output, err := schema.ListJobs(ctx, c.client, options.filter, cursor, new(internal.MaxPageSize))
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch page := output.Jobs.(type) {
		case *schema.ListJobsJobsJobConnection:
			return &page.PageInfo.PageInfo, page.Edges, nil
		case *schema.ListJobsJobsPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listJobs", page.Message)
		case *schema.ListJobsJobsInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(page.Message)
		case *schema.ListJobsJobsNotFoundError:
			return nil, nil, types.NewErrNotFound("", page.Typename, page.Message)
		default:
			return nil, nil, errors.New("unreachable")
		}
	}

	edgeFn := func(edge *types.JobConnectionEdgesJobEdge) (*string, *types.Job, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.Job, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

// GetTask returns a task by its job ID and task type.
func (c *JobClient) GetTask(ctx context.Context, jobId string, taskType string) (*types.Task, error) {
	result, err := schema.GetTask(ctx, c.client, jobId, taskType)
	if err != nil {
		return nil, types.NewErrClient(err)
	}
	return &result.JobTask.Task, nil
}

// ListTasksOfJob returns a channel that will receive the list of tasks for a job.
func (c *JobClient) ListTasksOfJob(ctx context.Context, jobId string) iter.Seq2[*types.Task, error] {
	loadPageFn := func(ctx context.Context, cursor *string) (*types.PageInfo, []types.TaskConnectionEdgesTaskEdge, error) {
		output, err := schema.ListTasksOfJob(ctx, c.client, jobId, cursor, new(internal.MaxPageSize))
		if err != nil {
			return nil, nil, types.NewErrClient(err)
		}

		switch tasks := output.Job.Tasks.(type) {
		case *schema.ListTasksOfJobJobTasksTaskConnection:
			return &tasks.PageInfo.PageInfo, tasks.Edges, nil
		case *schema.ListTasksOfJobJobTasksPermissionDeniedError:
			return nil, nil, types.NewErrPermissionDenied("listTasksOfJob", tasks.Message)
		case *schema.ListTasksOfJobJobTasksInvalidInputError:
			return nil, nil, types.NewErrInvalidInput(tasks.Message)
		case *schema.ListTasksOfJobJobTasksNotFoundError:
			return nil, nil, types.NewErrNotFound(jobId, tasks.Typename, tasks.Message)
		default:
			return nil, nil, errors.New("unreachable")
		}
	}

	edgeFn := func(edge *types.TaskConnectionEdgesTaskEdge) (*string, *types.Task, error) {
		cursor := edge.Cursor
		if edge.Node == nil {
			return cursor, nil, nil
		}
		return cursor, &edge.Node.Task, nil
	}

	return internal.PaginationExecutor(ctx, loadPageFn, edgeFn)
}

// GetSubtaskOfTask returns a subtask by its job ID, task type and subtask ID.
func (c *JobClient) GetSubtaskOfTask(ctx context.Context, jobId, taskType, subtaskId string) (*types.Subtask, error) {
	result, err := schema.GetSubtaskOfTask(ctx, c.client, jobId, taskType, subtaskId)
	if err != nil {
		return nil, types.NewErrClient(err)
	}
	return &result.JobSubtask.Subtask, nil
}

// CreateJob creates a new job.
func (c *JobClient) CreateJob(ctx context.Context, input types.JobInput) (*types.Job, error) {
	result, err := schema.CreateJob(ctx, c.client, input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}
	return &result.CreateJob.Job, nil
}

// UpdateJob updates an existing job.
func (c *JobClient) UpdateJob(ctx context.Context, id string, input types.JobInput) (*types.Job, error) {
	result, err := schema.UpdateJob(ctx, c.client, id, input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}
	return &result.UpdateJob.Job, nil
}

// AddTaskEvent adds a task event.
func (c *JobClient) AddTaskEvent(ctx context.Context, input types.TaskEventInput) (*types.Task, error) {
	result, err := schema.AddTaskEvent(ctx, c.client, input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}
	return &result.AddTaskEvent.Task, nil
}

// AddSubtaskEvent adds a subtask event.
func (c *JobClient) AddSubtaskEvent(ctx context.Context, input types.SubtaskInput) (*types.Subtask, error) {
	result, err := schema.AddSubtaskEvent(ctx, c.client, input)
	if err != nil {
		return nil, types.NewErrClient(err)
	}
	return &result.AddSubtaskEvent.Subtask, nil
}

// EndOfTargetsSync signals the end of targets sync.
func (c *JobClient) EndOfTargetsSync(ctx context.Context, input types.EndOfTargetsSyncInput) error {
	result, err := schema.EndOfTargetsSync(ctx, c.client, input)
	if err != nil {
		return types.NewErrClient(err)
	}

	if result.EndOfTargetsSync == nil {
		return types.NewErrClient(fmt.Errorf("no response from endOfTargetsSync"))
	}

	switch response := (*result.EndOfTargetsSync).(type) {
	case *schema.EndOfTargetsSyncEndOfTargetsSync:
		if success := response.GetSuccess(); success != nil && *success {
			return nil
		}

		return fmt.Errorf("end of targets sync failed")
	case *schema.EndOfTargetsSyncEndOfTargetsSyncPermissionDeniedError:
		return types.NewErrPermissionDenied("endOfTargetsSync", response.Message)
	default:
		return fmt.Errorf("unexpected response type: %T", response)
	}
}
