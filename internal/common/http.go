package common

import (
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
)

// Gets URIs that should not be logged during a request.
func getUnloggedURIs() []string {
	value := Secrets().GetWithDefault("BEAM_HTTP_NO_LOG_URI", "")

	paths := strings.Split(value, ",")
	paths = lo.FilterMap(paths, func(s string, _ int) (string, bool) {
		if str := strings.Trim(s, ""); str != "" {
			return str, true
		}
		return "", false
	})

	// Add default paths to ignore if none are provided.
	if len(paths) == 0 {
		paths = []string{"/health", "/healthz"}
	}

	return paths
}

// Logging middleware that conditionally logs requests based on URIs.
func LoggingMiddleware(next http.Handler) http.Handler {
	paths := getUnloggedURIs()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !lo.Contains(paths, r.RequestURI) {
			log.Printf("Request %s %s", r.Method, r.RequestURI)
		}

		next.ServeHTTP(w, r)
	})
}

// Logging middleware for Gin that conditionally logs requests based on URIs.
func LoggingMiddlewareGin() gin.HandlerFunc {
	paths := getUnloggedURIs()

	return gin.LoggerWithConfig(gin.LoggerConfig{
		SkipPaths: paths,
	})
}
