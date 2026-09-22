package httpapi

import (
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"net/http"
)

// openAPISpec é o contrato OpenAPI canônico servido pelo processo. Embutir o
// arquivo mantém a documentação e o binário sincronizados em qualquer deploy.
//
//go:embed openapi.yaml
var openAPISpec []byte

// handleOpenAPI serve a especificação que pode ser importada por clientes,
// gateways e ferramentas de geração de SDK.
func handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_, _ = w.Write(openAPISpec)
}

// handleSwaggerUI serve uma página mínima do Swagger UI. Os artefatos do UI
// vêm do CDN oficial do swagger-ui-dist; a API continua funcional sem acesso
// externo e a especificação permanece disponível em /openapi.yaml.
func handleSwaggerUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/swagger" {
		http.Redirect(w, r, "/swagger/", http.StatusMovedPermanently)
		return
	}
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		http.Error(w, "unable to initialize Swagger UI", http.StatusInternalServerError)
		return
	}
	nonce := hex.EncodeToString(nonceBytes)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", fmt.Sprintf("default-src 'self'; style-src 'self' 'unsafe-inline' https://unpkg.com; script-src 'self' https://unpkg.com 'nonce-%s'; img-src 'self' data: https://unpkg.com", nonce))
	_, _ = fmt.Fprintf(w, swaggerUIHTML, nonce)
}

const swaggerUIHTML = `<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Wagering API — Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script nonce="%s">
    window.ui = SwaggerUIBundle({
      url: '/openapi.yaml',
      dom_id: '#swagger-ui',
      deepLinking: true,
      persistAuthorization: true
    });
  </script>
</body>
</html>`
