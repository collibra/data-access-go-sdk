package services_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/utils"
	"github.com/stretchr/testify/suite"
)

type ImporterServiceTestSuite struct {
	suite.Suite
	sdkClient         *sdk.CollibraClient
	createdDataSource *schema.DataSource
	job               *schema.Job
	task              *schema.Task
	subtask           *schema.Subtask
	jobType           string
}

func readUsersFromFile(suite *suite.Suite, filePath string) []schema.ImportCommand {
	usersJson, err := os.ReadFile("testdata/test_users.json")
	suite.Require().NoError(err, "Failed to read users file")
	var users []schema.UserImport

	err = json.Unmarshal(usersJson, &users)
	suite.Require().NoError(err, "Failed to unmarshal users json")

	commands := make([]schema.ImportCommand, 0, len(users))
	for i := range users {
		commands = append(commands, schema.ImportCommand{
			UpsertUser: &users[i],
		})
	}
	return commands
}

func (suite *ImporterServiceTestSuite) SetupSuite() {
	config := utils.GetEnvConfig(&suite.Suite)
	sdkClient := sdk.NewClient(
		config.User,
		config.Password,
		config.URL,
	)

	suite.Require().NotNil(sdkClient, "Failed to create SDK client")
	suite.sdkClient = sdkClient

	dataSource := createDataSource(&suite.Suite, sdkClient.DataSource(), nil)

	suite.createdDataSource = dataSource

	ctx := suite.T().Context()
	// setup job -> task -> subtask
	jobClient := sdkClient.Job()
	suite.Require().NotNil(jobClient, "Failed to create Job client")
	startedJobStatus := schema.JobStatusStarted
	job, err := jobClient.CreateJob(ctx, schema.JobInput{
		DataSourceId: &dataSource.Id,
		Status:       &startedJobStatus,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create Job")
	suite.job = job
	suite.jobType = "IS"
	// start task
	task, err := jobClient.AddTaskEvent(ctx, schema.TaskEventInput{
		DataSourceId: &dataSource.Id,
		JobId:        job.Id,
		JobType:      suite.jobType,
		Status:       schema.TaskStatusStarted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create Task")
	suite.task = task

	// start subtask
	subtask, err := jobClient.AddSubtaskEvent(ctx, schema.SubtaskInput{
		DataSourceId: &dataSource.Id,
		JobId:        job.Id,
		JobType:      suite.jobType,
		SubtaskId:    "UserImport",
		Status:       schema.SubtaskStatusStarted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create Subtask")
	suite.Require().NotNil(subtask)
	suite.subtask = subtask

	// start import flow
	importerClient := sdkClient.Importer()
	subtask, err = importerClient.StartImportFlow(ctx, schema.StartImportFlowInput{
		JobId:     job.Id,
		TaskId:    suite.jobType,
		SubtaskId: subtask.SubtaskId,
	})
	suite.Require().NoError(err, "Failed to start import flow")
	suite.Require().NotNil(subtask)
	suite.subtask = subtask
}

func (suite *ImporterServiceTestSuite) TestImportHeartbeat() {
	ctx := suite.T().Context()
	importerClient := suite.sdkClient.Importer()
	subtask := suite.subtask
	importHeartBeatSubtask, err := importerClient.ImportHeartbeat(ctx, *subtask.FlowId)
	suite.Require().NoError(err, "Failed to send import heartbeat")
	suite.Require().NotNil(importHeartBeatSubtask, "Import heartbeat returned nil subtask")
	suite.Equal(subtask, importHeartBeatSubtask, "Import heartbeat subtask does not match original subtask")
}

func (suite *ImporterServiceTestSuite) TearDownSuite() {
	ctx := suite.T().Context()
	sdkClient := suite.sdkClient
	importerClient := sdkClient.Importer()
	err := importerClient.FinishImportFlow(ctx, *suite.subtask.FlowId)
	suite.Require().NoError(err, "Failed to finish import flow")
	jobClient := sdkClient.Job()
	_, err = jobClient.AddSubtaskEvent(ctx, schema.SubtaskInput{
		DataSourceId: &suite.createdDataSource.Id,
		JobId:        suite.job.Id,
		JobType:      suite.jobType,
		SubtaskId:    "UserImport",
		Status:       schema.SubtaskStatusCompleted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to complete Subtask")

	_, err = jobClient.AddTaskEvent(ctx, schema.TaskEventInput{
		DataSourceId: &suite.createdDataSource.Id,
		JobId:        suite.job.Id,
		JobType:      suite.jobType,
		Status:       schema.TaskStatusStarted,
		EventTime:    time.Now(),
	})
	suite.Require().NoError(err, "Failed to create Task")

	err = sdkClient.DataSource().DeleteDataSource(ctx, suite.createdDataSource.Id)
	suite.Require().NoError(err, "Failed to delete Data Source")
}

func Test_ImporterServiceTestSuite(t *testing.T) {
	suite.Run(t, new(ImporterServiceTestSuite))
}
