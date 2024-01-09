package auth

import (
	"github.com/gin-gonic/gin"
)

// func basicAuthMiddleware(beamRepo repository.BeamRepository) gin.HandlerFunc {
// 	versionRegex, err := types.GetAppVersionRegex()
// 	if err != nil {
// 		log.Fatalf("Failed to compile versioned URL regex: %v", err)
// 	}

// 	return func(ctx *gin.Context) {
// 		appId := ctx.Param("appId")
// 		appVersion := ctx.Param("version")
// 		subPath := ctx.Param("subPath")
// 		serveId := ctx.Param("serveId")

// 		// If we are using subdomain routing, extract from context
// 		if ctx.GetBool("subDomain") {
// 			appId = ctx.GetString("appId")
// 			appVersion = ctx.GetString("appVersion")
// 			serveId = ctx.GetString("serveId")
// 		} else if appVersion == "" && subPath != "" {
// 			// If appVersion param is not defined, but subPath is defined
// 			// attempt to parse app version string from subpath
// 			versionMatch := versionRegex.FindStringSubmatch(subPath)
// 			if versionMatch != nil {
// 				appVersion = versionMatch[1]
// 			}
// 		}

// 		auth := strings.SplitN(ctx.Request.Header.Get("Authorization"), " ", 2)

// 		ctx.Header("Access-Control-Allow-Origin", "*")
// 		ctx.Header("Access-Control-Allow-Credentials", "true")
// 		ctx.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
// 		ctx.Header("Access-Control-Allow-Methods", "POST,HEAD,PATCH,OPTIONS,GET,PUT")

// 		if ctx.Request.Method == "OPTIONS" {
// 			ctx.AbortWithStatus(204)
// 			return
// 		}

// 		// If no authentication is provided, check if this deployment requires authorization
// 		if len(auth) != 2 || auth[0] != "Basic" {

// 			if appId != "" && serveId == "" {
// 				authorized, err := beamRepo.DeploymentRequiresAuthorization(appId, appVersion)
// 				if err != nil || !authorized {
// 					respondWithError(http.StatusUnauthorized, "Unauthorized", ctx)
// 					return
// 				}
// 			} else if serveId != "" {
// 				authorized, err := beamRepo.ServeRequiresAuthorization(appId, serveId)
// 				if err != nil || !authorized {
// 					respondWithError(http.StatusUnauthorized, "Unauthorized", ctx)
// 					return
// 				}
// 			}

// 			ctx.Next()
// 			return
// 		}

// 		if !authorizeUser(beamRepo, auth, appId) {
// 			respondWithError(http.StatusForbidden, "Unauthorized", ctx)
// 			return
// 		}

// 		ctx.Next()
// 	}
// }

// func serviceAuthMiddleware(beamRepo repository.BeamRepository) gin.HandlerFunc {
// 	return func(ctx *gin.Context) {
// 		auth := strings.SplitN(ctx.Request.Header.Get("Authorization"), " ", 2)

// 		ctx.Header("Access-Control-Allow-Origin", "*")
// 		ctx.Header("Access-Control-Allow-Credentials", "true")
// 		ctx.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
// 		ctx.Header("Access-Control-Allow-Methods", "POST,HEAD,PATCH,OPTIONS,GET,PUT")

// 		if ctx.Request.Method == "OPTIONS" {
// 			ctx.AbortWithStatus(204)
// 			return
// 		}

// 		if len(auth) != 2 || auth[0] != "s2s" {
// 			respondWithError(http.StatusUnauthorized, "Unauthorized", ctx)
// 			return
// 		}

// 		if !authorizeInternalService(beamRepo, auth) {
// 			respondWithError(http.StatusForbidden, "Unauthorized", ctx)
// 			return
// 		}

// 		ctx.Next()
// 	}
// }

// func authorizeUser(beamRepo repository.BeamRepository, auth []string, appId string) bool {
// 	payload, _ := base64.StdEncoding.DecodeString(auth[1])

// 	pair := strings.SplitN(string(payload), ":", 2)
// 	if len(pair) != 2 {
// 		return false
// 	}

// 	// Extract client ID & client secret from decoded auth string
// 	clientId := pair[0]
// 	clientSecret := pair[1]

// 	// encodedBasicAuthString := auth[1] // Used to set auth w/ expiry in the store
// 	// if stateStore.IsAuthorized(appId, encodedBasicAuthString) {
// 	// 	return true
// 	// }

// 	authorized, err := beamRepo.AuthorizeApiKeyWithAppId(appId, clientId, clientSecret)
// 	if err != nil || !authorized {
// 		log.Printf("Unable to authorize user: %v", err)
// 		return false
// 	}

// 	// Set temporary authorization in store (1 day)
// 	// stateStore.Authorize(appId, encodedBasicAuthString)
// 	return true
// }

func respondWithError(code int, message string, c *gin.Context) {
	resp := map[string]string{"error": message}
	c.JSON(code, resp)
	c.Abort()
}
