package services_test

import (
	"testing"
	"time"

	sdk "github.com/collibra/data-access-go-sdk"
	"github.com/collibra/data-access-go-sdk/internal/schema"
	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/types"
	"github.com/collibra/data-access-go-sdk/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type ExporterServiceTestSuite struct {
	suite.Suite

	sdkClient            *sdk.CollibraClient
	createdDataSource    *schema.DataSource
	createdUser          *schema.User
	createdAccessControl *schema.AccessControl
	job                  *schema.Job
	task                 *schema.Task
	subtask              *schema.Subtask
	jobType              string
}

func createAccessControl(suite *ExporterServiceTestSuite) {
	ctx := suite.T().Context()
	name := "Test Access Control " + uuid.New().String()
	action := schema.AccessControlActionGrant
	user := suite.createdUser.Id
	dataSource := suite.createdDataSource.Id
	fullName := "RAITO_DBT"
	whatDataObjects := schema.AccessControlWhatInputDO{
		DataObjectByName: []schema.AccessControlWhatDoByNameInput{
			{
				DataSource: dataSource,
				FullName:   fullName,
			},
		},
	}
	accessControl, err := suite.sdkClient.AccessControl().CreateAccessControl(ctx, schema.AccessControlInput{
		Name:   &name,
		Action: &action,
		DataSources: []schema.AccessControlDataSourceInput{
			{DataSource: dataSource},
		},
		WhoItems: []schema.WhoItemInput{
			{User: &user},
		},
		WhatDataObjects: []schema.AccessControlWhatInputDO{whatDataObjects},
	})
	suite.Require().NoError(err, "Failed to create access control")
	suite.Require().NotNil(accessControl, "Created access control is nil")
	suite.createdAccessControl = accessControl
}

func createTestUser(suite *ExporterServiceTestSuite) {
	// create test user
	userName := "Test User " + uuid.NewString()
	userEmail := "test.user+" + uuid.NewString() + "@example.com"
	userType := schema.UserTypeHuman
	user, err := suite.sdkClient.User().CreateUser(suite.T().Context(), schema.UserInput{
		Name:  &userName,
		Email: &userEmail,
		Type:  &userType,
	})
	suite.Require().NoError(err, "Failed to create test user")
	suite.Require().NotNil(user, "Created user is nil")
	suite.createdUser = user
}

func TestExporterServiceTestSuite(t *testing.T) {
	suite.Run(t, new(ExporterServiceTestSuite))
}

func (suite *ExporterServiceTestSuite) SetupSuite() {
	url, clientOptions := utils.GetEnvConfig(&suite.Suite)
	sdkClient := sdk.NewClient(url, clientOptions...)

	suite.Require().NotNil(sdkClient, "Failed to create SDK client")
	suite.sdkClient = sdkClient

	dataSource := createDataSource(&suite.Suite, sdkClient.DataSource(), nil)
	dataSourceWithMetaData := setDataSourceMetadata(&suite.Suite, sdkClient.DataSource(), dataSource.Id, nil)

	suite.createdDataSource = dataSourceWithMetaData
	// import data objects
	importDataObjects(&suite.Suite, sdkClient, suite.createdDataSource.Id)
	createTestUser(suite)
	createAccessControl(suite)
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
	suite.jobType = "DA"
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
		SubtaskId:    "AccessControlFeedbackImport",
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

func (suite *ExporterServiceTestSuite) TearDownSuite() {
	TearDown_FinishJobAndTask_DeleteDataSource(&suite.Suite, suite.sdkClient, suite.subtask.FlowId, suite.createdDataSource.Id, suite.jobType, &suite.job.Id)
}

func (suite *ExporterServiceTestSuite) TestExportAccessControls() {
	suite.verifyExportAccessControls()
}

func (suite *ExporterServiceTestSuite) TestExportAccessControls_WithExportOutOfSyncOnly() {
	suite.verifyExportAccessControls(services.WithExportOutOfSyncOnly())
}

// helper function to reduce code duplication in the above tests
func (suite *ExporterServiceTestSuite) verifyExportAccessControls(opts ...func(options *services.ExportOptions)) {
	ctx := suite.T().Context()
	exporter := suite.sdkClient.Exporter()
	suite.Require().NotNil(exporter, "Failed to create Exporter client")

	_, err := exporter.StartExportFlow(ctx, *suite.subtask.FlowId, opts...)
	suite.Require().NoError(err, "Failed to start export flow")

	response := exporter.FetchExportAccessControls(ctx, *suite.subtask.FlowId)
	found := false

	for accessControl, err := range response {
		suite.Require().NoError(err, "Error while exporting access controls")
		suite.NotNil(accessControl, "Exported access control is nil")

		switch ac := accessControl.(type) {
		case *types.ExportedItemExportAccessControl:
			if ac.Id == suite.createdAccessControl.Id {
				suite.Equal(suite.createdAccessControl.Name, ac.Name)
				suite.Equal(suite.createdAccessControl.Action, ac.Action)

				found = true
				break
			}
		}
	}

	suite.Require().True(found, "Failed to find exported access control")

	_, err = exporter.FinishExportFlow(ctx, *suite.subtask.FlowId, time.Now())
	suite.Require().NoError(err, "Failed to finish export flow")
}
