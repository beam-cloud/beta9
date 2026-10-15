package vm

import (
	"bytes"
	_ "embed"
	"html/template"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v4"
)

//go:embed share_expired.html
var expiredShareHTML string

var expiredSharePage = template.Must(template.New("expired-share").Parse(expiredShareHTML))

func setBrowserHeaders(c echo.Context) {
	c.Response().Header().Set("Cache-Control", "no-store")
	c.Response().Header().Set("Referrer-Policy", "no-referrer")
}

func browserNavigation(r *http.Request) bool {
	if r.Method != http.MethodGet || !strings.Contains(r.Header.Get("Accept"), "text/html") || r.Header.Get("Upgrade") != "" {
		return false
	}
	if mode := r.Header.Get("Sec-Fetch-Mode"); mode != "" && mode != "navigate" {
		return false
	}
	if dest := r.Header.Get("Sec-Fetch-Dest"); dest != "" && dest != "document" {
		return false
	}
	return true
}

func cleanBrowserURL(c echo.Context) (*url.URL, error) {
	// RequestURI retains the browser path before the VM host router rewrites it.
	u, err := url.ParseRequestURI(c.Request().RequestURI)
	if err != nil || u.IsAbs() || u.Host != "" || !strings.HasPrefix(u.Path, "/") || strings.HasPrefix(u.Path, "//") || strings.Contains(u.Path, "\\") {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "invalid request URI")
	}
	query := u.Query()
	query.Del(sessionParameter)
	u.RawQuery = query.Encode()
	return u, nil
}

func (s *Service) invalidShareLink(c echo.Context, cleanURL string) error {
	data := struct{ OwnerURL string }{}
	if s.browserAuthURL != "" {
		data.OwnerURL = cleanURL
	}
	var page bytes.Buffer
	if err := expiredSharePage.Execute(&page, data); err != nil {
		return err
	}
	setBrowserHeaders(c)
	c.Response().Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
	return c.HTML(http.StatusForbidden, page.String())
}
