package services_test

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

func completeStreamingSubtask(
	suite *suite.Suite,
	jobClient *services.JobClient,
	jobId string,
	jobType string,
	subtaskId string,
	dataSourceId string,
) error {
	var (
		timeout      = 8 * time.Minute
		pollInterval = 5 * time.Second
		initialDelay = 30 * time.Second
	)

	ctx, cancel := context.WithTimeout(suite.T().Context(), timeout)
	defer cancel()

	suite.T().Logf("Waiting for initial delay of %v before polling for subtask completion", initialDelay)
	time.Sleep(initialDelay)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout: subtask with ID %s did not complete in expected time", subtaskId)
		case <-ticker.C:
			suite.T().Logf("Polling for subtask %s status", subtaskId)
			subtask, err := jobClient.GetSubtaskOfTask(ctx, jobId, jobType, subtaskId)
			suite.Require().NoError(err, "Failed to get subtask status")

			if subtask.FlowClosed != nil && *subtask.FlowClosed { // flow does not close here
				suite.T().Logf("Subtask flow %s is closed, marking as complete", subtaskId)

				_, err := jobClient.AddSubtaskEvent(ctx, schema.SubtaskInput{
					JobId:        jobId,
					DataSourceId: &dataSourceId,
					JobType:      jobType,
					SubtaskId:    subtaskId,
					Status:       schema.SubtaskStatusCompleted,
					EventTime:    time.Now(),
				})
				if err != nil {
					return fmt.Errorf("Failed to complete subtask with ID %s: %w", subtaskId, err)
				}

				suite.T().Logf("Subtask with ID %s completed successfully.", subtaskId)

				return nil
			}
		}
	}
}

func submitObjects(suite *suite.Suite, jobClient *services.JobClient, importer *services.ImporterClient, dataSourceId string, jobType string, subtaskId string, commands []schema.ImportCommand) {
	ctx := suite.T().Context()
	job, err := jobClient.CreateJob(ctx, schema.JobInput{
		DataSourceId: &dataSourceId,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create job")

	task, err := jobClient.AddTaskEvent(ctx, schema.TaskEventInput{
		JobId:        job.Id,
		DataSourceId: &dataSourceId,
		JobType:      jobType,
		Status:       schema.TaskStatusStarted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create task event")

	subtask, err := jobClient.AddSubtaskEvent(ctx, schema.SubtaskInput{
		JobId:        job.Id,
		DataSourceId: &dataSourceId,
		JobType:      jobType,
		SubtaskId:    subtaskId,
		Status:       schema.SubtaskStatusStarted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create subtask event")

	subtask, err = importer.StartImportFlow(ctx, schema.StartImportFlowInput{
		JobId:     job.Id,
		TaskId:    task.TaskType,
		SubtaskId: subtask.SubtaskId,
		Options: &schema.ImportFlowOptions{
			TagSourcesScope: []string{"Snowflake"},
		},
	})
	suite.Require().NoError(err, "Failed to start import flow")

	_, err = importer.SubmitImportObjects(ctx, schema.ImportCommands{
		FlowId:   *subtask.FlowId,
		Commands: commands,
	})

	suite.Require().NoError(err, "Failed to submit import objects")

	err = importer.FinishImportFlow(ctx, *subtask.FlowId)
	suite.Require().NoError(err, "Failed to finish import flow")

	err = completeStreamingSubtask(suite, jobClient, job.Id, task.TaskType, subtask.SubtaskId, dataSourceId)
	suite.Require().NoError(err, "Failed to complete streaming subtask")

	_, err = jobClient.AddTaskEvent(ctx, schema.TaskEventInput{
		JobId:        job.Id,
		DataSourceId: &dataSourceId,
		JobType:      jobType,
		Status:       schema.TaskStatusCompleted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to add task event")

	status := schema.JobStatusCompleted
	job, err = jobClient.UpdateJob(ctx, job.Id, schema.JobInput{
		DataSourceId: &dataSourceId,
		Status:       &status,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to end targets sync")
	suite.Require().NotNil(job, "Job update result is nil")
}

func TearDown_FinishJobAndTask_DeleteDataSource(suite *suite.Suite, sdkClient *sdk.CollibraClient, flowId *uuid.UUID, dataSourceId string, jobType string, jobId *string) {
	ctx := suite.T().Context()
	importerClient := sdkClient.Importer()
	err := importerClient.FinishImportFlow(ctx, *flowId)
	suite.Require().NoError(err, "Failed to finish import flow")

	jobClient := sdkClient.Job()
	_, err = jobClient.AddSubtaskEvent(ctx, schema.SubtaskInput{
		DataSourceId: &dataSourceId,
		JobId:        *jobId,
		JobType:      jobType,
		SubtaskId:    "UserImport",
		Status:       schema.SubtaskStatusCompleted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to complete Subtask")

	_, err = jobClient.AddTaskEvent(ctx, schema.TaskEventInput{
		DataSourceId: &dataSourceId,
		JobId:        *jobId,
		JobType:      jobType,
		Status:       schema.TaskStatusStarted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create Task")

	err = sdkClient.DataSource().DeleteDataSource(ctx, dataSourceId)
	suite.Require().NoError(err, "Failed to delete Data Source")
}
