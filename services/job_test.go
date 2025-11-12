package services_test

import (
	"testing"
	"time"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/utils"
	"github.com/stretchr/testify/suite"
)

type JobServiceTestSuite struct {
	suite.Suite

	sdkClient         *sdk.CollibraClient
	createdDataSource *schema.DataSource
	jobClient         *services.JobClient
	createdJob        *schema.Job
	createdTask       *schema.Task
	createdSubtask    *schema.Subtask
}

func (suite *JobServiceTestSuite) SetupSuite() {
	config := utils.GetEnvConfig(&suite.Suite)
	sdkClient := sdk.NewClient(
		config.User,
		config.Password,
		config.URL,
	)

	suite.Require().NotNil(sdkClient, "Failed to create SDK client")
	suite.sdkClient = sdkClient
	datasource := createDataSource(&suite.Suite, sdkClient.DataSource(), nil)
	suite.createdDataSource = datasource
	suite.jobClient = sdkClient.Job()
}

func (suite *JobServiceTestSuite) TearDownSuite() {
	err := suite.sdkClient.DataSource().DeleteDataSource(suite.T().Context(), suite.createdDataSource.Id)
	suite.Require().NoError(err, "Failed to delete data source")
}

func TestJobServiceTestSuite(t *testing.T) {
	suite.Run(t, new(JobServiceTestSuite))
}

func (suite *JobServiceTestSuite) TestA_CreateJob() {
	ctx := suite.T().Context()
	datasourceId := suite.createdDataSource.Id
	job, err := suite.jobClient.CreateJob(ctx, schema.JobInput{
		DataSourceId: &datasourceId,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create job")
	suite.Require().NotNil(job, "Created job is nil")
	suite.Require().Equal(datasourceId, job.DataSource.Id, "Data source ID does not match")
	suite.Require().NotNil(job.Id, "Created job ID is nil")
	suite.Require().NotNil(job.StartTime, "Created job EventTime is nil")
	suite.createdJob = job
}

func (suite *JobServiceTestSuite) TestB_GetJob() {
	ctx := suite.T().Context()

	createdJob := suite.createdJob
	if createdJob == nil {
		suite.T().Skip("Skipping test as createdJob is nil")
	}

	job, err := suite.jobClient.GetJob(ctx, createdJob.Id) // error here, datasource value does not exist
	suite.Require().NoError(err, "Failed to get job")
	suite.Require().NotNil(job, "Fetched job is nil")
	suite.Require().Equal(createdJob, job, "Job details do not match")
}

func (suite *JobServiceTestSuite) TestC_UpdateJob() {
	ctx := suite.T().Context()

	createdJob := suite.createdJob
	if createdJob == nil {
		suite.T().Skip("Skipping test as createdJob is nil")
	}

	jobStatusToUpdate := schema.JobStatusInprogress

	updatedJob, err := suite.jobClient.UpdateJob(ctx, createdJob.Id, schema.JobInput{
		Status:    &jobStatusToUpdate,
		EventTime: time.Now(),
	})
	suite.Require().NoError(err, "Failed to update job")
	suite.Require().NotNil(updatedJob, "Updated job is nil")
	suite.Require().Equal(schema.JobStatusInprogress, updatedJob.Status, "Job status was not updated")
	suite.createdJob = updatedJob
}

func (suite *JobServiceTestSuite) TestD_ListJobs() {
	suite.T().Skip("Skipping until https://engineering-collibra.atlassian.net/browse/DEV-151222 implemented")

	ctx := suite.T().Context()
	if suite.createdJob == nil {
		suite.T().Skip("Skipping test as createdJob is nil")
	}

	response := suite.jobClient.ListJobs(ctx)

	found := false

	for job, err := range response {
		suite.Require().NoError(err, "Failed to list jobs in the response")
		suite.Require().NotNil(job, "Listed job is nil in the response")
		// check if created job is in the list
		if job.Id == suite.createdJob.Id {
			suite.Require().Equal(suite.createdJob, job, "Listed job does not match the created job")

			found = true
		}
	}

	suite.Require().True(found, "Created job not found in the list")
}

func (suite *JobServiceTestSuite) TestE_ListJobs_WithJobListFilter() {
	ctx := suite.T().Context()
	if suite.createdJob == nil {
		suite.T().Skip("Skipping test as createdJob is nil")
	}

	response := suite.jobClient.ListJobs(ctx, services.WithJobListFilter(&schema.JobsFilter{DataSource: &suite.createdDataSource.Id}))
	found := false

	for job, err := range response {
		suite.Require().NoError(err, "Failed to list jobs in the response")
		suite.Require().NotNil(job, "Listed job is nil in the response")
		// check if created job is in the list
		if job.Id == suite.createdJob.Id {
			suite.Require().Equal(suite.createdJob, job, "Listed job does not match the created job")

			found = true
		}
	}

	suite.Require().True(found, "Created job not found in the list")
}

func (suite *JobServiceTestSuite) TestF_AddTaskEvent() {
	ctx := suite.T().Context()

	createdJob := suite.createdJob
	if createdJob == nil {
		suite.T().Skip("Skipping test as createdJob is nil")
	}

	task, err := suite.jobClient.AddTaskEvent(ctx, schema.TaskEventInput{
		JobId:        createdJob.Id,
		JobType:      "IS",
		Status:       schema.TaskStatusStarted,
		DataSourceId: &suite.createdDataSource.Id,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to add task event")
	suite.Require().NotNil(task, "Created task event is nil")
	suite.Require().NotNil(task.Status, "Created task event Status is nil")
	suite.createdTask = task
}

func (suite *JobServiceTestSuite) TestG_GetTask() {
	ctx := suite.T().Context()
	createdJob := suite.createdJob

	createdTask := suite.createdTask
	if createdJob == nil || createdTask == nil {
		suite.T().Skip("Skipping test as createdJob or createdTask is nil")
	}

	task, err := suite.jobClient.GetTask(ctx, createdJob.Id, "IS")
	suite.Require().NoError(err, "Failed to get task")
	suite.Require().NotNil(task, "Fetched task is nil")
	suite.Require().Equal(createdTask, task, "Task details do not match")
}

func (suite *JobServiceTestSuite) TestH_ListTasksOfJob() {
	ctx := suite.T().Context()
	createdJob := suite.createdJob

	createdTask := suite.createdTask
	if createdJob == nil || createdTask == nil {
		suite.T().Skip("Skipping test as createdJob or createdTask is nil")
	}

	response := suite.jobClient.ListTasksOfJob(ctx, createdJob.Id)

	found := false

	for task, err := range response {
		suite.Require().NoError(err, "Failed to list tasks in the response")
		suite.Require().NotNil(task, "Listed task is nil in the response")
		// check if created task is in the list
		if task.JobId == createdTask.JobId && task.TaskType == createdTask.TaskType {
			suite.Require().Equal(createdTask, task, "Listed task does not match the created task")

			found = true
		}
	}

	suite.Require().True(found, "Created task not found in the list")
}

func (suite *JobServiceTestSuite) TestI_AddSubTaskEvent() {
	ctx := suite.T().Context()

	createdTask := suite.createdTask
	if createdTask == nil {
		suite.T().Skip("Skipping test as createdTask is nil")
	}

	subtask, err := suite.jobClient.AddSubtaskEvent(ctx, schema.SubtaskInput{
		JobId:     createdTask.JobId,
		JobType:   createdTask.TaskType,
		Status:    schema.SubtaskStatusStarted,
		SubtaskId: "SampleSubtaskID",
		EventTime: time.Now(),
	})
	suite.Require().NoError(err, "Failed to add subtask event")
	suite.Require().NotNil(subtask, "Created subtask event is nil")
	suite.createdSubtask = subtask
}

func (suite *JobServiceTestSuite) TestJ_GetSubtaskOfTask() {
	ctx := suite.T().Context()
	createdTask := suite.createdTask

	createdSubtask := suite.createdSubtask
	if createdTask == nil || createdSubtask == nil {
		suite.T().Skip("Skipping test as createdTask or createdSubtask is nil")
	}

	subtask, err := suite.jobClient.GetSubtaskOfTask(ctx, createdTask.JobId, createdTask.TaskType, createdSubtask.SubtaskId)
	suite.Require().NoError(err, "Failed to get subtask")
	suite.Require().NotNil(subtask, "Fetched subtask is nil")
	suite.Require().Equal(createdSubtask, subtask, "Subtask details do not match")
}
