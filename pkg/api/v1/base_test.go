package apiv1

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/repository"
)

func addWorkspaceRowQuery(mockHttpDetails *repository.MockHttpDetails) {
	mockHttpDetails.Mock.ExpectQuery("SELECT (.+) FROM workspace").WillReturnRows(
		sqlmock.NewRows(
			[]string{
				"id",
				"external_id",
			},
		).AddRow(
			mockHttpDetails.TokenForTest.Workspace.Id,
			mockHttpDetails.TokenForTest.Workspace.ExternalId,
		),
	)
}
