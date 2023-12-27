package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var client *gorm.DB

type PostgresBeamRepository struct {
	client *gorm.DB
}

func NewBeamPostgresRepository() (BeamRepository, error) {
	// If client is not nil, it was already initialized. Just return a new PostgresBeamRepository with the existing client.
	if client != nil {
		return &PostgresBeamRepository{
			client: client,
		}, nil
	}

	host := common.Secrets().Get("DB_HOST")
	port := common.Secrets().GetInt("DB_PORT")
	user := common.Secrets().Get("DB_USER")
	password := common.Secrets().Get("DB_PASS")
	dbName := common.Secrets().Get("DB_NAME")

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable", host, user, password, dbName, port)
	var err error
	client, err = gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return nil, err
	}

	return &PostgresBeamRepository{
		client: client,
	}, nil
}

func (c *PostgresBeamRepository) GetDeployments(appId string) ([]types.BeamAppDeployment, error) {
	var app types.BeamApp
	var deployments []types.BeamAppDeployment

	result := c.client.Where(&types.BeamApp{ShortId: appId}).First(&app)
	err := result.Error

	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App <%s> not found", appId)
		return nil, err
	}

	c.client.Where(&types.BeamAppDeployment{AppId: app.ID}).Find(&deployments)
	return deployments, nil
}

func (c *PostgresBeamRepository) GetDeployment(appId string, version *uint) (*types.BeamAppDeployment, *types.BeamApp, *types.BeamAppDeploymentPackage, error) {
	var app types.BeamApp
	var latestDeployment types.BeamAppDeployment
	var deploymentPackage types.BeamAppDeploymentPackage

	{
		result := c.client.Where(&types.BeamApp{ShortId: appId}).First(&app)
		err := result.Error

		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App <%s> not found.", appId)
			return nil, nil, nil, err
		}
	}

	var beamAppDeployment types.BeamAppDeployment = types.BeamAppDeployment{AppId: app.ID}
	if version != nil {
		beamAppDeployment.Version = *version
	}

	result := c.client.Where(&beamAppDeployment).Order("version DESC").First(&latestDeployment)
	err := result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App deployment not found for app <%s>.", appId)
		return nil, nil, nil, err
	}

	result = c.client.Where(&types.BeamApp{ID: latestDeployment.AppId}).Take(&app)
	err = result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App not found for deployment <%s>.", latestDeployment.ExternalId)
		return nil, nil, nil, err
	}

	result = c.client.Where(&types.BeamAppDeploymentPackage{ID: latestDeployment.PackageId}).Take(&deploymentPackage)
	err = result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App deployment package not found for deployment <%s>.", latestDeployment.ExternalId)
		return nil, nil, nil, err
	} else if err != nil {
		log.Printf("Error retrieving app deployment package <%v>.", err)
		return nil, nil, nil, err
	}

	return &latestDeployment, &app, &deploymentPackage, nil
}

func (c *PostgresBeamRepository) GetDeploymentById(appDeploymentId string) (*types.BeamAppDeployment, *types.BeamApp, *types.BeamAppDeploymentPackage, error) {
	var deployment types.BeamAppDeployment
	var app types.BeamApp
	var deploymentPackage types.BeamAppDeploymentPackage

	{
		result := c.client.Where(&types.BeamAppDeployment{ExternalId: appDeploymentId}).Take(&deployment)
		err := result.Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App deployment <%s> not found.", appDeploymentId)
			return nil, nil, nil, err
		}
	}

	result := c.client.Where(&types.BeamApp{ID: deployment.AppId}).Take(&app)
	err := result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App not found for deployment <%s>.", appDeploymentId)
		return nil, nil, nil, err
	}

	result = c.client.Where(&types.BeamAppDeploymentPackage{ID: deployment.PackageId}).Take(&deploymentPackage)
	err = result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App deployment package not found for deployment <%s>.", appDeploymentId)
		return nil, nil, nil, err
	}

	return &deployment, &app, &deploymentPackage, nil
}

func (c *PostgresBeamRepository) GetServe(appId string, serveId string) (*types.BeamAppServe, *types.BeamApp, error) {
	var app types.BeamApp
	var serve types.BeamAppServe

	result := c.client.Where(&types.BeamApp{ShortId: appId}).First(&app)
	err := result.Error

	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App <%s> not found", appId)
		return nil, nil, err
	}

	result = c.client.Where(&types.BeamAppServe{AppId: app.ID, ExternalId: serveId}).Where("ended_at IS NULL").First(&serve)
	err = result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App serve <%s> not found", serveId)
		return nil, nil, err
	}

	return &serve, &app, nil
}

func (c *PostgresBeamRepository) EndServe(appId string, serveId string, identityId string) error {
	var app types.BeamApp
	var serve types.BeamAppServe
	var stat types.Stats

	result := c.client.Where(&types.BeamApp{ShortId: appId}).First(&app)
	err := result.Error

	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App <%s> not found", appId)
		return err
	}

	result = c.client.Where(&types.BeamAppServe{AppId: app.ID, ExternalId: serveId}).Where("ended_at IS NULL").First(&serve)
	err = result.Error

	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App serve <%s> not found", serveId)
		return nil
	}

	// We try getting the metric that includes the serve time

	query := c.client.Where(
		&types.Stats{
			Topic:    "beam",
			Category: "worker",
			Identity: "container",
			Metric:   "lifecycle",
		},
	)

	query.Where("tags->>'identity_id' = ?", identityId)
	query.Where("tags->>'container_id' = ?", fmt.Sprintf("serve-%s-%s", appId, serveId))
	query.Where("tags->>'status' = ?", "STOPPED")

	statResult := query.First(&stat)
	err = statResult.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		timestampNow := time.Now().Unix() * 1000 // convert to milliseconds since UNIX function returns seconds
		serve.EndedAt = &timestampNow
	}

	if serve.EndedAt == nil {
		timestampEnded, err := strconv.ParseInt(stat.Value, 10, 64)
		if err != nil {
			return err
		}

		serve.EndedAt = &timestampEnded
	}

	c.client.Save(&serve)

	return nil
}

func (c *PostgresBeamRepository) UpdateDeployment(appDeploymentId string, status string, errorMsg string) (*types.BeamAppDeployment, error) {
	tx := c.client.Begin()
	var deployment types.BeamAppDeployment

	{
		result := tx.Where(&types.BeamAppDeployment{ExternalId: appDeploymentId}).Take(&deployment)
		err := result.Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App deployment <%s> not found.", appDeploymentId)
			return nil, err
		}
	}

	deployment.Status = status
	deployment.ErrorMsg = errorMsg
	deployment.Updated = time.Now()

	tx.Save(&deployment)
	tx.Commit()

	return &deployment, nil
}

func (c *PostgresBeamRepository) CreateDeploymentTask(appDeploymentId string, taskId string, taskPolicyRaw []byte) (*types.BeamAppTask, error) {
	var appDeployment types.BeamAppDeployment
	var app types.BeamApp
	var appTask types.BeamAppTask

	tx := c.client.Begin()

	{
		result := tx.Where(&types.BeamAppDeployment{ExternalId: appDeploymentId}).Find(&appDeployment)
		err := result.Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App deployment <%s> not found.", appDeploymentId)
			return nil, err
		}
	}

	{
		result := tx.Where(&types.BeamApp{ID: appDeployment.AppId}).Find(&app)
		err := result.Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App <%d> not found.", appDeployment.AppId)
			return nil, err
		}
	}

	appTask.ExternalId = objectId()
	appTask.TaskId = taskId
	appTask.AppDeploymentId = &appDeployment.ID
	appTask.Status = types.BeamAppTaskStatusPending
	appTask.IdentityId = app.IdentityId
	appTask.AppRunId = nil
	appTask.TaskPolicy = taskPolicyRaw

	result := tx.Create(&appTask)
	err := result.Error
	if err != nil {
		log.Printf("Unable to create app task <%s>.", err)
		return nil, err
	}

	tx.Commit()
	return &appTask, nil
}

func (c *PostgresBeamRepository) CreateServeTask(appServeId string, taskId string, taskPolicyRaw []byte) (*types.BeamAppTask, error) {
	var appServe types.BeamAppServe
	var app types.BeamApp
	var appTask types.BeamAppTask

	tx := c.client.Begin()

	{
		result := tx.Where(&types.BeamAppServe{ExternalId: appServeId}).Find(&appServe)
		err := result.Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App serve <%s> not found.", appServeId)
			return nil, err
		}
	}

	{
		result := tx.Where(&types.BeamApp{ID: appServe.AppId}).Find(&app)
		err := result.Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App <%d> not found.", appServe.AppId)
			return nil, err
		}
	}

	appTask.ExternalId = objectId()
	appTask.TaskId = taskId
	appTask.AppServeId = &appServe.ID
	appTask.Status = types.BeamAppTaskStatusPending
	appTask.IdentityId = app.IdentityId
	appTask.AppRunId = nil
	appTask.TaskPolicy = taskPolicyRaw

	result := tx.Create(&appTask)
	err := result.Error
	if err != nil && errors.Is(err, gorm.ErrInvalidData) {
		log.Printf("Unable to create app task <%s>.", err)
		return nil, err
	}

	tx.Commit()
	return &appTask, nil
}

func (c *PostgresBeamRepository) GetAppTask(taskId string) (*types.BeamAppTask, error) {
	var appTask types.BeamAppTask

	result := c.client.Where(&types.BeamAppTask{TaskId: taskId}).Find(&appTask)
	err := result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("App Task <%s> not found.", taskId)
		return nil, err
	}

	return &appTask, nil
}

func (c *PostgresBeamRepository) UpdateActiveTask(taskId string, status string, identityExternalId string) (*types.BeamAppTask, error) {
	tx := c.client.Begin()
	var appTask types.BeamAppTask
	terminationStatuses := []string{types.BeamAppTaskStatusComplete, types.BeamAppTaskStatusFailed, types.BeamAppTaskStatusCancelled}

	{
		result := tx.Joins("JOIN identity_identity ON identity_identity.id = beam_app_task.identity_id").
			Where(&types.BeamAppTask{TaskId: taskId}).
			Where("identity_identity.external_id = ?", identityExternalId).
			Not("beam_app_task.status IN ?", terminationStatuses).
			Take(&appTask)

		err := result.Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App task <%s> not found.", taskId)
			tx.Rollback()
			return nil, err
		}
	}

	appTask.Status = status
	appTask.Updated = time.Now()

	switch status {
	case types.BeamAppTaskStatusRunning:
		now := time.Now()
		appTask.StartedAt = &now
		appTask.EndedAt = nil
	case types.BeamAppTaskStatusComplete, types.BeamAppTaskStatusFailed, types.BeamAppTaskStatusCancelled:
		now := time.Now()
		appTask.EndedAt = &now
	case types.BeamAppTaskStatusRetry:
		appTask.StartedAt = nil // reset started at to reset timeout timer
	}

	if err := tx.Save(&appTask).Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	return &appTask, nil
}

func (c *PostgresBeamRepository) DeleteTask(taskId string) error {
	tx := c.client.Begin()
	var appTask types.BeamAppTask
	terminationStatuses := []string{types.BeamAppTaskStatusComplete, types.BeamAppTaskStatusFailed, types.BeamAppTaskStatusCancelled}

	{
		result := tx.Where(&types.BeamAppTask{TaskId: taskId}).Not(
			"status IN (?)", terminationStatuses,
		).Take(&appTask)
		err := result.Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("App task <%s> not found.", taskId)
			return err
		}
	}

	if err := tx.Delete(&appTask).Error; err != nil {
		log.Printf("Error occurred while deleting task <%s>: %s\n", taskId, err)
		return err
	}

	tx.Commit()
	return nil
}

// GetTotalUsageOfUserMs returns the total usage aggregated across all apps of a user in milliseconds
func (c *PostgresBeamRepository) GetTotalUsageOfUserMs(identity types.Identity) (totalUsageMs float64, err error) {
	var beamApps []types.BeamApp
	var stats []types.Stats
	var identityConditions []string
	args := make([]interface{}, 0)

	err = c.client.Where(&types.BeamApp{IdentityId: uint(identity.ID)}).Find(&beamApps).Error
	if err != nil {
		return 0, err
	}

	if len(beamApps) == 0 {
		return 0, nil
	}

	shortIds := make([]string, len(beamApps))
	for i, app := range beamApps {
		shortIds[i] = app.ShortId
	}

	// Here we add a like query for identities that start with the shortId of the app
	for _, id := range shortIds {
		identityConditions = append(identityConditions, "identity LIKE ?")
		args = append(args, id+"%")
	}

	identityQuery := strings.Join(identityConditions, " OR ")

	query := c.client.Where(&types.Stats{Topic: "beam", Subcategory: "usage", Type: "timer"})
	query = query.Where(identityQuery, args...)

	err = query.Find(&stats).Error
	if err != nil {
		return 0, err
	}

	// The values of timers should be an array of floats so we need to unmarshal them and sum them
	for _, stat := range stats {
		var values []float64
		err = json.Unmarshal([]byte(stat.Value), &values)
		if err != nil {
			return 0, err
		}

		for _, value := range values {
			totalUsageMs += value
		}
	}

	return totalUsageMs, nil
}

func (c *PostgresBeamRepository) RetrieveUserByPk(pk uint) (*types.Identity, error) {
	var identity types.Identity

	result := c.client.Where(&types.Identity{ID: pk}).Take(&identity)

	err := result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("Identity not found <%d>.", pk)
		return nil, err
	}

	return &identity, nil
}

func (c *PostgresBeamRepository) AuthorizeApiKey(clientId, clientSecret string) (bool, *types.Identity, error) {
	var apiKey types.IdentityApiKey

	result := c.client.Preload("Identity").Where(&types.IdentityApiKey{ClientId: clientId, ClientSecret: clientSecret}).Take(&apiKey)

	err := result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil, err
	}

	if !apiKey.IsEnabled {
		return false, &apiKey.Identity, nil
	}

	return true, &apiKey.Identity, nil
}

func (c *PostgresBeamRepository) AuthorizeApiKeyWithAppId(appId string, clientId string, clientSecret string) (bool, error) {
	var app types.BeamApp

	// Authorize the API key
	authorized, identity, err := c.AuthorizeApiKey(clientId, clientSecret)
	if !authorized || err != nil {
		return false, err
	}

	// Check if the app exists
	result := c.client.Where(&types.BeamApp{ShortId: appId}).Take(&app)

	err = result.Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		return false, fmt.Errorf("app <%v> not found: %v", appId, err)
	}

	// Check if the app belongs to the identity
	if app.IdentityId != identity.ID {
		return false, fmt.Errorf("app <%v> does not belong to this identity <%v>", app.IdentityId, identity.ID)
	}

	return true, nil
}

func (c *PostgresBeamRepository) DeploymentRequiresAuthorization(appId string, appVersion string) (bool, error) {
	v, err := types.ParseAppVersion(appVersion)
	if err != nil {
		return false, err
	}

	version := &v.Value
	if v.Value == 0 {
		version = nil
	}

	_, _, deploymentPackage, err := c.GetDeployment(appId, version)
	if err != nil {
		return false, err
	}

	var appConfig types.BeamAppConfig
	err = json.Unmarshal(deploymentPackage.Config, &appConfig)
	if err != nil {
		return false, err
	}

	// If either the authorized field is nil, or it is true, don't allow user in
	if appConfig.Triggers[0].Authorized == nil || *appConfig.Triggers[0].Authorized {
		return false, nil
	}

	return true, nil
}

func (c *PostgresBeamRepository) ServeRequiresAuthorization(appId string, serveId string) (bool, error) {
	serve, _, err := c.GetServe(appId, serveId)
	if err != nil {
		return false, err
	}

	var appConfig types.BeamAppConfig
	err = json.Unmarshal(serve.Config, &appConfig)
	if err != nil {
		return false, err
	}

	// If either the authorized field is nil, or it is true, don't allow user in
	if appConfig.Triggers[0].Authorized == nil || *appConfig.Triggers[0].Authorized {
		return false, nil
	}

	return true, nil
}

func (c *PostgresBeamRepository) AuthorizeServiceToServiceToken(token string) (*types.Identity, bool, error) {
	var s2sToken types.ServiceToServiceToken

	if err := c.client.Where(&types.ServiceToServiceToken{Key: token}).First(&s2sToken).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	// Check token entity type
	if s2sToken.EntityType == types.S2SEntityBeamApi || s2sToken.EntityType == types.S2SEntityBeamDeployment || s2sToken.EntityType == types.S2SEntityBeamRun || s2sToken.EntityType == types.S2SEntityBeamServe {
		identity, err := c.RetrieveUserByPk(s2sToken.IdentityID)
		if err != nil {
			return nil, false, err
		}

		return identity, true, nil
	}

	return nil, false, nil
}

func objectId() string {
	id := uuid.NewString()
	id = strings.ReplaceAll(id, "-", "")
	return id[:30]
}

// Gets Agent by Name and Identity's ExternalId.
// Also hydrates Agent.Identity.
func (c *PostgresBeamRepository) GetAgent(name, identityExternalId string) (*types.Agent, error) {
	var a types.Agent
	var i types.Identity

	result := c.client.Where(&types.Identity{ExternalID: identityExternalId}).Take(&i)
	if result.Error != nil && errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, result.Error
	}

	result = c.client.Where(&types.Agent{Name: name, IdentityId: i.ID}).Take(&a)
	if result.Error != nil && errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, result.Error
	}

	a.Identity = i

	return &a, nil
}

// Gets Agent by Token.
// Also hydrates Agent.Identity.
func (c *PostgresBeamRepository) GetAgentByToken(token string) (*types.Agent, error) {
	var agent types.Agent

	result := c.client.Preload("Identity").Where(&types.Agent{Token: token}).Take(&agent)
	if result.Error != nil && errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, result.Error
	}

	return &agent, nil
}

func (c *PostgresBeamRepository) UpdateAgent(agent *types.Agent) (*types.Agent, error) {
	res := c.client.Model(&types.Agent{}).
		Where("id = ?", agent.ID).
		Updates(map[string]interface{}{
			"CloudProvider": agent.CloudProvider,
			"IsOnline":      agent.IsOnline,
			"Pools":         agent.Pools,
			"Version":       agent.Version,
			"Updated":       time.Now(),
		})

	if res.Error != nil {
		return nil, fmt.Errorf("error updating agent <%s>: %v", agent.Name, res.Error)
	}
	if res.RowsAffected == 0 {
		return nil, fmt.Errorf("no rows updated for agent <%s>", agent.Name)
	}

	return agent, nil
}

func (c *PostgresBeamRepository) GetIdentityQuota(identityId string) (*types.IdentityQuota, error) {
	var identityQuota types.IdentityQuota

	res := (c.client.
		Joins("JOIN identity_identity ON identity_identity_quota.identity_id = identity_identity.id").
		Where("identity_identity.external_id = ?", identityId).
		Take(&identityQuota))

	if res.Error != nil {
		return &identityQuota, res.Error
	}

	return &identityQuota, nil
}
