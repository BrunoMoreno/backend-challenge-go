//go:build integration && faultinject

// M9 — cenários de multi-instância e falhas com TRÊS processos independentes
// do binário (cada um com pool e memória próprios) contra PostgreSQL + LocalStack
// + Keycloak reais (docs/TESTING.md §2). O harness compila o binário com a build
// tag `faultinject` (nunca usada em produção), sobe instâncias com portas e
// APP_ROLES distintos, aguarda /health/ready e encerra com SIGTERM (graceful)
// ou SIGKILL (crash).
//
// Cenários (docs/CONTEXT.md M9, docs/TESTING.md §4):
//   - disputa 100.00 × 2×80.00 entre instâncias distintas → 1 PROCESSED,
//     1 INSUFFICIENT_FUNDS, saldo 20.00, 1 débito no ledger;
//   - 50 envios idênticos em paralelo por 3 instâncias → 1 débito;
//   - crash do consumidor após o commit e antes do delete
//     (FAULT_AFTER_COMMIT_BEFORE_DELETE) → reentrega sem duplicação;
//   - conferência final: saldo armazenado = Σ créditos − Σ débitos do ledger
//     e POST /reconciliation consistente.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sqsEndpoint    = "http://localhost:4566"
	inputQueueURL  = sqsEndpoint + "/000000000000/wager-transactions.fifo"
	crashQueueURL  = sqsEndpoint + "/000000000000/wager-crash-test.fifo"
	dlqURL         = sqsEndpoint + "/000000000000/wager-transactions-dlq.fifo"
	eventsQueueURL = sqsEndpoint + "/000000000000/wager-events.fifo"
	dbAppURL       = "postgres://wager_app:wager_app@localhost:5432/wagering?sslmode=disable"
	dbMigrateURL   = "postgres://app:app@localhost:5432/wagering?sslmode=disable"
	keycloakToken  = "http://localhost:8081/realms/wagering/protocol/openid-connect/token"

	httpPortBase = 18100
	metricBase   = 19100
)

var (
	appBinary string
	runID     = "e2e-" + strconv.Itoa(os.Getpid())
	sqsClient *awssqs.Client
)

func appURL() string {
	if u := os.Getenv("APP_DATABASE_URL"); u != "" {
		return u
	}
	return dbAppURL
}

func migrateURL() string {
	if u := os.Getenv("DATABASE_URL"); u != "" {
		return u
	}
	return dbMigrateURL
}

// TestMain purga as filas, limpa o banco (role de migração) e compila o binário
// com faultinject uma única vez para todos os testes do pacote.
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Elimina instâncias órfãs de execuções anteriores (um `go test` morto por
	// timeout não roda os cleanups e deixa consumidores disputando as filas).
	killOrphanInstances()

	client, err := newSQSClient(ctx)
	if err != nil {
		log.Fatalf("sqs client: %v (rode `make up`)", err)
	}
	sqsClient = client

	// Recria a fila de entrada e a DLQ do zero: PurgeQueue não remove mensagens
	// em voo (invisíveis), o que deixaria reentregas de execuções anteriores
	// contaminando o cofre FIFO — e o crash do consumidor depende de reentrega
	// confiável.
	recreateInputQueues(ctx, sqsClient)
	_, _ = sqsClient.PurgeQueue(ctx, &awssqs.PurgeQueueInput{QueueUrl: aws.String(eventsQueueURL)})

	pool, err := pgxpool.New(ctx, migrateURL())
	if err != nil {
		log.Fatalf("postgres: %v (rode `make up` + `make migrate-up`)", err)
	}
	_, _ = pool.Exec(ctx, `TRUNCATE outbox_events, inbox, wallet_ledger_entries,
		wager_transactions, wallets RESTART IDENTITY CASCADE`)
	pool.Close()

	appBinary = buildBinary()
	code := m.Run()
	os.Exit(code)
}

// killOrphanInstances encerra processos-app do e2e que sobraram de execuções
// abortadas (go test -timeout não executa os cleanups do testing). O binário é
// reconhecível pelo caminho temporário e2e-bin.
func killOrphanInstances() {
	out, err := exec.Command("pgrep", "-f", `e2e-bin[^ ]*/app`).Output()
	if err != nil {
		return // sem órfãos (ou pgrep indisponível)
	}
	for _, pidStr := range strings.Fields(string(out)) {
		if pid, err := strconv.Atoi(pidStr); err == nil {
			_ = syscall.Kill(pid, syscall.SIGTERM)
		}
	}
	time.Sleep(500 * time.Millisecond)
	for _, pidStr := range strings.Fields(string(out)) {
		if pid, err := strconv.Atoi(pidStr); err == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

// recreateInputQueues apaga e recria a fila de entrada e a DLQ com os mesmos
// atributos do provisionamento (queues.sh), eliminando inclusive mensagens em
// voo que um PurgeQueue não remove.
func recreateInputQueues(ctx context.Context, client *awssqs.Client) {
	for _, q := range []string{dlqURL, inputQueueURL, crashQueueURL} {
		_, _ = client.DeleteQueue(ctx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(q)})
	}
	base := map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "false",
		"VisibilityTimeout":         "30",
	}
	dlqAttr := maps.Clone(base)
	if _, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String("wager-transactions-dlq.fifo"),
		Attributes: dlqAttr,
	}); err != nil {
		log.Fatalf("criar DLQ: %v", err)
	}
	inputAttr := maps.Clone(base)
	inputAttr["RedrivePolicy"] = `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:wager-transactions-dlq.fifo","maxReceiveCount":5}`
	if _, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String("wager-transactions.fifo"),
		Attributes: inputAttr,
	}); err != nil {
		log.Fatalf("criar fila de entrada: %v", err)
	}
	// Fila dedicada do cenário de crash: isolada da entrada compartilhada, o
	// crash do consumidor nunca é "roubado" por outras instâncias em drenagem
	// de cenários anteriores.
	if _, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String("wager-crash-test.fifo"),
		Attributes: maps.Clone(base),
	}); err != nil {
		log.Fatalf("criar fila do cenário de crash: %v", err)
	}
}

func buildBinary() string {
	dir, err := os.MkdirTemp("", "e2e-bin")
	if err != nil {
		log.Fatalf("tempdir: %v", err)
	}
	bin := filepath.Join(dir, "app")
	cmd := exec.Command("go", "build", "-tags=faultinject", "-o", bin,
		"github.com/BrunoMoreno/backend-challenge-go/cmd/app")
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Fatalf("build binário (faultinject): %v\n%s", err, out)
	}
	return bin
}

func newSQSClient(ctx context.Context) (*awssqs.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithBaseEndpoint(sqsEndpoint),
		awsconfig.WithCredentialsProvider(aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		)),
	)
	if err != nil {
		return nil, err
	}
	return awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.Region = "us-east-1"
	}), nil
}

// --- instâncias de processo ---

type instance struct {
	t     *testing.T
	url   string
	roles string
	cmd   *exec.Cmd
	done  chan error
}

// startInstance sobe UM processo independente do binário com APP_ROLES e portas
// próprias. faultEnv carrega pontos de falha do faultinject (ex.:
// FAULT_AFTER_COMMIT_BEFORE_DELETE=1).
func startInstance(t *testing.T, roles string, idx int, faultEnv ...string) *instance {
	t.Helper()
	if appBinary == "" {
		t.Fatal("binary não compilado; TestMain falhou?")
	}
	httpAddr := fmt.Sprintf(":%d", httpPortBase+idx)
	metricAddr := fmt.Sprintf(":%d", metricBase+idx)
	logf, err := os.CreateTemp("", "e2e-inst-*.log")
	if err != nil {
		t.Fatalf("log file: %v", err)
	}
	cmd := exec.Command(appBinary)
	cmd.Env = overrideEnv(childEnv(httpAddr, metricAddr, roles), faultEnv...)
	cmd.Stdout = logf
	cmd.Stderr = logf

	if err := cmd.Start(); err != nil {
		t.Fatalf("start instância [%s]: %v", roles, err)
	}
	in := &instance{
		t:     t,
		url:   fmt.Sprintf("http://localhost:%d", httpPortBase+idx),
		roles: roles,
		cmd:   cmd,
		done:  make(chan error, 1),
	}
	go func() { in.done <- cmd.Wait() }()
	t.Cleanup(func() { in.stop() })

	in.waitReady()
	t.Logf("instância %s em http://localhost:%d ok", roles, httpPortBase+idx)
	return in
}

// childEnv monta o ambiente de cada instância: mesmo banco, filas e Keycloak;
// portas distintas. Os parâmetros de tempo são baixos para os testes serem
// rápidos e determinísticos.
func childEnv(httpAddr, metricAddr, roles string) []string {
	env := []string{
		"APP_ROLES=" + roles,
		"APP_HTTP_ADDR=" + httpAddr,
		"APP_METRICS_ADDR=" + metricAddr,
		"APP_DATABASE_URL=" + appURL(),
		"APP_SQS_ENDPOINT=" + sqsEndpoint,
		"APP_SQS_REGION=us-east-1",
		"APP_SQS_QUEUE_URL=" + inputQueueURL,
		"APP_SQS_DLQ_URL=" + dlqURL,
		"APP_SQS_EVENTS_QUEUE_URL=" + eventsQueueURL,
		"APP_SQS_MAX_RECEIVE_COUNT=5",
		"APP_SQS_VISIBILITY_TIMEOUT=5s",
		"APP_SQS_CONSUMER_CONCURRENCY=2",
		"APP_SQS_CONSUMER_BACKOFF_BASE=500ms",
		"APP_SQS_CONSUMER_BACKOFF_MAX=5s",
		"APP_KEYCLOAK_ISSUER=" + "http://localhost:8081/realms/wagering",
		"APP_KEYCLOAK_JWKS_URL=" + "http://localhost:8081/realms/wagering/protocol/openid-connect/certs",
		"APP_KEYCLOAK_CLIENT_ID=wager-api",
		"APP_LOG_LEVEL=error",
		"APP_SHUTDOWN_TIMEOUT=10s",
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"AWS_EC2_METADATA_DISABLED=true",
	}
	// Mantém o PATH do runner.
	return append(env, "PATH="+os.Getenv("PATH"))
}

// overrideEnv aplica extras sobre uma lista de env base, removendo antes
// qualquer entrada com a MESMA chave (a primeira ocorrência vence no runtime do
// Go; um simples append não sobreporia FAULT_*/APP_SQS_QUEUE_URL do base).
func overrideEnv(base []string, extras ...string) []string {
	for _, e := range extras {
		key, _, _ := strings.Cut(e, "=")
		for i := 0; i < len(base); {
			if k, _, _ := strings.Cut(base[i], "="); k == key {
				base = append(base[:i], base[i+1:]...)
				continue
			}
			i++
		}
		base = append(base, e)
	}
	return base
}

// waitReady aguarda /health/ready responder 200 (probe de PostgreSQL e, nos
// papéis de consumidor, da fila de entrada).
func (in *instance) waitReady() {
	in.t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(in.url + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case err := <-in.done:
			in.t.Fatalf("instância [%s] morreu antes do ready: %v", in.roles, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
	in.t.Fatalf("instância [%s] não ficou pronta em 90s", in.roles)
}

// stop encerra a instância com SIGTERM (graceful) e, se estourar o prazo, SIGKILL.
func (in *instance) stop() {
	if in.cmd == nil || in.cmd.Process == nil {
		return
	}
	// Já encerrou (ex.: crash capturado por waitExit, que drenou o canal done):
	// nada a fazer — esperar de novo travaria o teste.
	if in.cmd.ProcessState != nil {
		return
	}
	select {
	case err := <-in.done:
		in.t.Logf("instância [%s] encerrada (exit=%v)", in.roles, err)
		return
	default:
	}
	_ = in.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-in.done:
	case <-time.After(15 * time.Second):
		_ = in.cmd.Process.Kill()
		select {
		case <-in.done:
		case <-time.After(5 * time.Second):
		}
	}
}

// waitExit aguarda o encerramento abrupto da instância (crash) e devolve o erro.
func (in *instance) waitExit(timeout time.Duration) error {
	select {
	case err := <-in.done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("instância [%s] não encerrou em %v", in.roles, timeout)
	}
}

// --- API HTTP ---

type walletDTO struct {
	ID       string   `json:"id"`
	PlayerID string   `json:"playerId"`
	Balance  moneyDTO `json:"balance"`
	Version  int64    `json:"version"`
}

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type processResultDTO struct {
	TransactionID    string    `json:"transactionId"`
	Status           string    `json:"status"`
	Balance          *moneyDTO `json:"balance,omitempty"`
	FailureCode      string    `json:"failureCode,omitempty"`
	IdempotentReplay bool      `json:"idempotentReplay"`
}

type reconciliationDTO struct {
	WalletID          string   `json:"walletId"`
	StoredBalance     moneyDTO `json:"storedBalance"`
	CalculatedBalance moneyDTO `json:"calculatedBalance"`
	Difference        moneyDTO `json:"difference"`
	Consistent        bool     `json:"consistent"`
	CheckedEntries    int64    `json:"checkedEntries"`
}

type ledgerEntryDTO struct {
	ID            string   `json:"id"`
	TransactionID string   `json:"transactionId"`
	Direction     string   `json:"direction"`
	Money         moneyDTO `json:"money"`
	BalanceBefore moneyDTO `json:"balanceBefore"`
	BalanceAfter  moneyDTO `json:"balanceAfter"`
}

type ledgerPageDTO struct {
	Items []ledgerEntryDTO `json:"items"`
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// doRequest executa uma chamada HTTP autenticada e devolve status e corpo.
func doRequest(t *testing.T, method, target, token, idemKey string, body any) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, target, rd)
	if err != nil {
		t.Fatalf("request %s %s: %v", method, target, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, target, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// openWallet abre uma carteira com saldo inicial via POST /wallets (role interna).
func openWallet(t *testing.T, in *instance, internalToken, player, amount string) string {
	t.Helper()
	code, raw := doRequest(t, "POST", in.url+"/wallets", internalToken, "", map[string]any{
		"playerId": player,
		"initialBalance": map[string]any{
			"amount":   amount,
			"currency": "BRL",
		},
	})
	if code != http.StatusCreated {
		t.Fatalf("open wallet = %d %s", code, raw)
	}
	var w walletDTO
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatalf("decode wallet: %v (%s)", err, raw)
	}
	return w.ID
}

// submit envia POST /wagering/transactions, repetindo 503 UNAVAILABLE (conflito
// transitório de claim; Retry-After) até estabilizar.
func submit(t *testing.T, in *instance, providerToken, key string, body map[string]any) (int, processResultDTO) {
	t.Helper()
	for attempt := 0; attempt < 30; attempt++ {
		code, raw := doRequest(t, "POST", in.url+"/wagering/transactions", providerToken, key, body)
		if code == http.StatusServiceUnavailable {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		var res processResultDTO
		if err := json.Unmarshal(raw, &res); err != nil {
			t.Fatalf("decode submit: %v (%s)", err, raw)
		}
		return code, res
	}
	t.Fatal("submit: 503 persistente (retry esgotado)")
	return 0, processResultDTO{}
}

// getWallet lê a carteira via instância HTTP.
func getWallet(t *testing.T, in *instance, internalToken, walletID string) walletDTO {
	t.Helper()
	code, raw := doRequest(t, "GET", in.url+"/wallets/"+walletID, internalToken, "", nil)
	if code != http.StatusOK {
		t.Fatalf("get wallet = %d %s", code, raw)
	}
	var w walletDTO
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatalf("decode wallet: %v (%s)", err, raw)
	}
	return w
}

// ledger lê o extrato completo da carteira.
func ledger(t *testing.T, in *instance, internalToken, walletID string) []ledgerEntryDTO {
	t.Helper()
	code, raw := doRequest(t, "GET", in.url+"/wallets/"+walletID+"/ledger?limit=200", internalToken, "", nil)
	if code != http.StatusOK {
		t.Fatalf("ledger = %d %s", code, raw)
	}
	var page ledgerPageDTO
	if err := json.Unmarshal(raw, &page); err != nil {
		t.Fatalf("decode ledger: %v (%s)", err, raw)
	}
	return page.Items
}

func reconcile(t *testing.T, in *instance, internalToken, walletID string) reconciliationDTO {
	t.Helper()
	code, raw := doRequest(t, "POST", in.url+"/wallets/"+walletID+"/reconciliation", internalToken, "", nil)
	if code != http.StatusOK {
		t.Fatalf("reconciliation = %d %s", code, raw)
	}
	var r reconciliationDTO
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("decode reconciliation: %v (%s)", err, raw)
	}
	return r
}

// submitBody monta o corpo de POST /wagering/transactions.
func submitBody(provider, ext, player, walletID, round, game, kind, amount, ref string) map[string]any {
	body := map[string]any{
		"providerId":            provider,
		"externalTransactionId": ext,
		"playerId":              player,
		"walletId":              walletID,
		"roundId":               round,
		"gameId":                game,
		"kind":                  kind,
		"amount": map[string]any{
			"amount":   amount,
			"currency": "BRL",
		},
	}
	if ref != "" {
		body["referenceExternalTransactionId"] = ref
	}
	return body
}

// --- Keycloak ---

// providerToken obtém um access token real de client_credentials (RF-02).
func clientToken(t *testing.T, client, secret string) string {
	t.Helper()
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {client},
		"client_secret": {secret},
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := http.PostForm(keycloakToken, form)
		if err == nil {
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var tok struct {
					AccessToken string `json:"access_token"`
				}
				if json.Unmarshal(raw, &tok) == nil && tok.AccessToken != "" {
					return tok.AccessToken
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("keycloak indisponível em %s (rode `make up`)", keycloakToken)
		}
		time.Sleep(1 * time.Second)
	}
}

func providerAToken(t *testing.T) string { return clientToken(t, "provider-a", "provider-a-secret") }
func internalToken(t *testing.T) string {
	return clientToken(t, "wagering-internal", "wagering-internal-secret")
}

// --- SQS direto (LocalStack) ---

func sendSQS(t *testing.T, queue, group, dedup, body string) {
	t.Helper()
	_, err := sqsClient.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl:               aws.String(queue),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(dedup),
	})
	if err != nil {
		t.Fatalf("send SQS: %v", err)
	}
}

// visibleMessages lê o contador de mensagens visíveis da fila
// (ApproximateNumberOfMessages) para o teste observar a reentrega após o crash
// sem roubar a mensagem do polling dos consumidores.
func visibleMessagesErr(queue string) (int64, error) {
	ctx := context.Background()
	out, err := sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queue),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(out.Attributes["ApproximateNumberOfMessages"], 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func visibleMessages(t *testing.T, queue string) int64 {
	t.Helper()
	n, err := visibleMessagesErr(queue)
	if err != nil {
		t.Fatalf("get attributes: %v", err)
	}
	return n
}

// inboxCompleted consulta a inbox do consumidor process-wager: a mensagem foi
// concluída durável (delete perseguido pela reentrega).
func inboxCompleted(t *testing.T, messageID string) bool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, appURL())
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	var completed bool
	err = pool.QueryRow(ctx,
		`SELECT completed_at IS NOT NULL FROM inbox WHERE consumer_name = 'process-wager' AND message_id = $1`,
		messageID).Scan(&completed)
	if err != nil {
		t.Fatalf("inbox read: %v", err)
	}
	return completed
}

// --- banco (conferência final) ---

type ledgerTotals struct {
	credits int64
	debits  int64
}

func sumLedger(t *testing.T, walletID string) ledgerTotals {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, appURL())
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	var credits, debits int64
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(sum(amount_minor) FILTER (WHERE direction='CREDIT'),0),
		        coalesce(sum(amount_minor) FILTER (WHERE direction='DEBIT'),0)
		   FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&credits, &debits); err != nil {
		t.Fatalf("sum ledger: %v", err)
	}
	return ledgerTotals{credits: credits, debits: debits}
}

func countWagers(t *testing.T, provider, ext string) int {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, appURL())
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM wager_transactions
		  WHERE provider_id = $1 AND external_transaction_id = $2`, provider, ext).Scan(&n); err != nil {
		t.Fatalf("count wagers: %v", err)
	}
	return n
}

func assertSame(t *testing.T, want, got string, msgf string) {
	t.Helper()
	if want != got {
		t.Fatalf("%s: want %s, got %s", msgf, want, got)
	}
}

// startPool sobe N instâncias completas (todos os papéis) do harness.
func startPool(t *testing.T, n int, faultEnv ...string) []*instance {
	t.Helper()
	insts := make([]*instance, n)
	for i := range insts {
		var env []string
		if i == 0 {
			env = faultEnv
		}
		insts[i] = startInstance(t, "http,sqs-consumer,outbox-publisher,reference-worker", i, env...)
	}
	return insts
}

// --- cenários ---

// TestDisputeAcrossInstances — RF-04/G2: carteira 100.00 recebe 2 apostas de
// 80.00 enviadas SIMULTANEAMENTE a instâncias distintas. Coordenadas pelo lock
// no banco: uma PROCESSED, uma INSUFFICIENT_FUNDS, saldo 20.00, 1 débito, e os
// reenvios não alteram (idempotência + replay).
func TestDisputeAcrossInstances(t *testing.T) {
	insts := startPool(t, 3)
	pTok, iTok := providerAToken(t), internalToken(t)
	player := runID + "-player-dispute"

	walID := openWallet(t, insts[0], iTok, player, "100.00")

	bet1 := submitBody("provider-a", runID+"-ext-bet-1", player, walID, "r-1", "g-1", "BET", "80.00", "")
	bet2 := submitBody("provider-a", runID+"-ext-bet-2", player, walID, "r-2", "g-1", "BET", "80.00", "")

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		res1         processResultDTO
		code1, code2 int
		res2         processResultDTO
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		c, r := submit(t, insts[0], pTok, "key-"+runID+"-dispute-1", bet1)
		mu.Lock()
		code1, res1 = c, r
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		c, r := submit(t, insts[1], pTok, "key-"+runID+"-dispute-2", bet2)
		mu.Lock()
		code2, res2 = c, r
		mu.Unlock()
	}()
	wg.Wait()

	processed, rejected := res1, res2
	if code2 == http.StatusCreated {
		processed, rejected = res2, res1
	}
	if code1 != http.StatusCreated && code2 != http.StatusCreated {
		t.Fatalf("nenhuma aposta PROCESSED: %d %+v / %d %+v", code1, res1, code2, res2)
	}
	if processed.Status != "PROCESSED" || processed.Balance == nil || processed.Balance.Amount != "20.00" {
		t.Fatalf("aposta processada inválida: %d %+v", code1, processed)
	}
	if rejected.Status != "REJECTED" || rejected.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Fatalf("aposta rejeitada inválida: %d %+v", code2, rejected)
	}

	w := getWallet(t, insts[2], iTok, walID)
	assertSame(t, "20.00", w.Balance.Amount, "saldo final")

	entries := ledger(t, insts[0], iTok, walID)
	if len(entries) != 2 { // OPENING crédito + BET débito
		t.Fatalf("ledger = %d lançamentos, want 2 (1 crédito de abertura + 1 débito)", len(entries))
	}
	r := reconcile(t, insts[1], iTok, walID)
	if !r.Consistent || r.CheckedEntries != 2 {
		t.Fatalf("reconciliation = %+v, want consistent com 2 lançamentos", r)
	}

	// Reenvios (replays) com a mesma chave não alteram o resultado.
	t.Logf("replays com a mesma chave não alteram o resultado (saldo 20.00)")
	code, res := submit(t, insts[2], pTok, "key-"+runID+"-dispute-1", bet1)
	if code != http.StatusOK || !res.IdempotentReplay || res.Balance == nil || res.Balance.Amount != "20.00" {
		t.Fatalf("replay bet1 = %d %+v, want 200 idempotente 20.00", code, res)
	}
	code, res = submit(t, insts[0], pTok, "key-"+runID+"-dispute-2", bet2)
	if code != http.StatusUnprocessableEntity || !res.IdempotentReplay {
		t.Fatalf("replay bet2 = %d %+v, want 422 idempotente", code, res)
	}
	if b := getWallet(t, insts[1], iTok, walID).Balance.Amount; b != "20.00" {
		t.Fatalf("saldo após replays = %s, want 20.00", b)
	}
}

// TestFiftyDuplicatesAcrossInstances — RF-04/G2: a mesma aposta é enviada 50
// vezes em paralelo por 3 instâncias com a mesma Idempotency-Key: exatamente 1
// PROCESSED e 49 replays, um único débito no ledger e uma única transação.
func TestFiftyDuplicatesAcrossInstances(t *testing.T) {
	insts := startPool(t, 3)
	pTok, iTok := providerAToken(t), internalToken(t)
	player := runID + "-player-dup"

	walID := openWallet(t, insts[0], iTok, player, "1000.00")

	ext := runID + "-ext-dup"
	body := submitBody("provider-a", ext, player, walID, "r-dup", "g-1", "BET", "20.00", "")
	key := "key-" + runID + "-dup"

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		created  int
		replays  int
		rejected int
		others   []string
	)
	const total = 50
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func(i int) {
			defer wg.Done()
			c, res := submit(t, insts[i%3], pTok, key, body)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case c == http.StatusCreated && res.Status == "PROCESSED":
				created++
			case c == http.StatusOK && res.IdempotentReplay:
				replays++
			case c == http.StatusUnprocessableEntity:
				rejected++
				others = append(others, fmt.Sprintf("%d:%s", c, res.FailureCode))
			default:
				others = append(others, fmt.Sprintf("%d:%+v", c, res))
			}
		}(i)
	}
	wg.Wait()

	if created != 1 || replays != total-1 {
		t.Fatalf("desfechos: created=%d replays=%d rejected=%d others=%v, want 1/49/0", created, replays, rejected, others)
	}

	w := getWallet(t, insts[1], iTok, walID)
	assertSame(t, "980.00", w.Balance.Amount, "saldo após 50 envios")

	entries := ledger(t, insts[0], iTok, walID)
	if len(entries) != 2 {
		t.Fatalf("ledger = %d lançamentos, want 2 (1 débito)", len(entries))
	}
	if n := countWagers(t, "provider-a", ext); n != 1 {
		t.Fatalf("wager_transactions(provider,ext) = %d, want 1", n)
	}
	if r := reconcile(t, insts[2], iTok, walID); !r.Consistent {
		t.Fatalf("reconciliation após 50 enviaos = %+v", r)
	}
}

// TestCrashConsumerAfterCommit — RF-06: consumidor processa a mensagem, faz o
// COMMIT e morre antes de apagar a fila (FAULT_AFTER_COMMIT_BEFORE_DELETE).
// A mensagem retorna após a visibilidade expirar e OUTRA instância a conclui
// como replay (inbox completa) sem duplicar o débito.
func TestCrashConsumerAfterCommit(t *testing.T) {
	// Instância B: serve o HTTP e o outbox (sem consumidor) para abrir a carteira.
	api := startInstance(t, "http,outbox-publisher,reference-worker", 10)
	iTok := internalToken(t)
	player := runID + "-player-crash"
	walID := openWallet(t, api, iTok, player, "100.00")

	// Instância A: consumidor SOLO do papel de entrada com o ponto de falha
	// ativo; processa, commita e morre antes do delete (FAULT_..._BEFORE_DELETE).
	// A fila é DEDICADA (crashQueueURL): nenhuma outra instância em drenagem de
	// cenários anteriores disputa a mensagem do crash.
	crashInst := startInstance(t, "http,sqs-consumer", 11,
		"FAULT_AFTER_COMMIT_BEFORE_DELETE=1", "APP_SQS_QUEUE_URL="+crashQueueURL)

	body := map[string]any{
		"messageId":  "msg-" + runID + "-crash",
		"type":       "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339),
		"data": map[string]any{
			"providerId":            "provider-a",
			"externalTransactionId": runID + "-ext-crash",
			"idempotencyKey":        "key-" + runID + "-crash",
			"playerId":              player,
			"walletId":              walID,
			"roundId":               "r-crash",
			"gameId":                "g-1",
			"kind":                  "BET",
			"money":                 map[string]string{"amount": "25.00", "currency": "BRL"},
		},
	}
	raw, _ := json.Marshal(body)
	sendSQS(t, crashQueueURL, walID, "msg-"+runID+"-crash", string(raw))

	// A processa (débito 25.00 commitado), commita e CRASH antes do delete.
	err := crashInst.waitExit(60 * time.Second)
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 1 {
		t.Fatalf("consumidor com faultinject deveria sair com código 1, err=%v", err)
	}

	w := getWallet(t, api, iTok, walID)
	assertSame(t, "75.00", w.Balance.Amount, "saldo após commit antes do crash")

	// Efeito durável do commit (mesmo sem o delete): saldo, ledger e transação
	// consistentes apesar do crash pós-commit.
	if w := getWallet(t, api, iTok, walID); w.Balance.Amount != "75.00" {
		t.Fatalf("saldo após crash = %s, want 75.00", w.Balance.Amount)
	}
	if n := countWagers(t, "provider-a", runID+"-ext-crash"); n != 1 {
		t.Fatalf("wager_transactions após crash = %d, want 1", n)
	}
	if !inboxCompleted(t, "msg-"+runID+"-crash") {
		t.Fatal("inbox completa após crash (commit durável, delete perdido)")
	}

	// Reentrega: o LocalStack não requeueia mensagens cujo consumidor morreu
	// sem delete (at-least-once é garantia do SQS real, não do LocalStack).
	// Simulamos a reentrega com um retry do produtor: MESMO envelope e mesmo
	// messageId, apenas deduplication id novo — percorre exatamente o caminho
	// de replay (hash idêntico + inbox completa → apaga sem reaplicar).
	sendSQS(t, crashQueueURL, walID, "msg-"+runID+"-crash-retry", string(raw))

	// Instância C (completa) assume a reentrega como replay e apaga sem
	// reaplicar — a conclusão é observada pela inbox permanecer concluída e os
	// efeitos duraveis não mudarem.
	_ = startInstance(t, "http,sqs-consumer,outbox-publisher,reference-worker", 12,
		"APP_SQS_QUEUE_URL="+crashQueueURL)
	deadline := time.Now().Add(30 * time.Second)
	replayed := false
	for time.Now().Before(deadline) {
		if !replayed {
			// O replay consome e apaga a mensagem: o contador volta a zero.
			if visibleMessagesSafe(t, crashQueueURL) == 0 {
				replayed = true
			}
		}
		if replayed && visibleMessagesSafe(t, crashQueueURL) == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !replayed {
		t.Fatal("mensagem de reentrega não foi consumida (replay) pela instância C")
	}
	if !inboxCompleted(t, "msg-"+runID+"-crash") {
		t.Fatal("inbox da mensagem deixou de estar concluída após o replay")
	}
	if w := getWallet(t, api, iTok, walID); w.Balance.Amount != "75.00" {
		t.Fatalf("saldo após reentrega = %s, want 75.00 (sem duplicação)", w.Balance.Amount)
	}
	entries := ledger(t, api, iTok, walID)
	if len(entries) != 2 { // OPENING + 1 BET débito
		t.Fatalf("ledger = %d lançamentos após reentrega, want 2 (movimentação única)", len(entries))
	}
	if n := countWagers(t, "provider-a", runID+"-ext-crash"); n != 1 {
		t.Fatalf("wager_transactions após reentrega = %d, want 1", n)
	}
	if r := reconcile(t, api, iTok, walID); !r.Consistent {
		t.Fatalf("reconciliation após crash = %+v", r)
	}
}

// visibleMessagesSafe ignora falhas transitórias do contador aproximado do
// LocalStack e devolve -1 em erro para não abortar o teste.
func visibleMessagesSafe(t *testing.T, queue string) int64 {
	t.Helper()
	n, err := visibleMessagesErr(queue)
	if err != nil {
		return -1
	}
	return n
}

// TestFinalConferencia — G8 / RF-08: após uma sequência financeira mista
// (BET/REFUND/WIN/ROLLBACK/LOSS), o saldo armazenado iguala a soma de créditos
// menos débitos do ledger e a reconciliação HTTP confere.
func TestFinalConferencia(t *testing.T) {
	insts := startPool(t, 3)
	pTok, iTok := providerAToken(t), internalToken(t)
	player := runID + "-player-final"

	walID := openWallet(t, insts[0], iTok, player, "1000.00")

	type op struct{ kind, amount, ref string }
	var extID int
	submitOp := func(kind, amount, ref string) string {
		extID++
		ext := runID + fmt.Sprintf("-ext-%d", extID)
		code, res := submit(t, insts[extID%3], pTok, "key-"+runID+fmt.Sprintf("%d", extID),
			submitBody("provider-a", ext, player, walID, "r-f", "g-1", kind, amount, ref))
		if res.Status != "PROCESSED" {
			t.Fatalf("%s %s = %d %+v", kind, amount, code, res)
		}
		return ext
	}

	betExt := submitOp("BET", "30.00", "")
	submitOp("REFUND", "30.00", betExt)
	winExt := submitOp("WIN", "500.00", "")
	submitOp("ROLLBACK", "500.00", winExt)
	submitOp("LOSS", "0.00", "") // sem movimento

	w := getWallet(t, insts[0], iTok, walID)
	assertSame(t, "1000.00", w.Balance.Amount, "saldo final esperado")

	entries := ledger(t, insts[1], iTok, walID)
	if len(entries) != 5 { // abertura + BET + REFUND + WIN + ROLLBACK
		t.Fatalf("ledger = %d lançamentos, want 5 (LOSS não move)", len(entries))
	}

	tot := sumLedger(t, walID)
	expectedStart := int64(100000)
	credits := tot.credits
	debits := tot.debits
	// stored = Σ créditos − Σ débitos
	if credits-debits != int64(100000) {
		t.Fatalf("Σcréditos(%d) − Σdébitos(%d) = %d, want %d (saldo armazenado)",
			credits, debits, credits-debits, expectedStart)
	}

	r := reconcile(t, insts[2], iTok, walID)
	if !r.Consistent || r.CheckedEntries != 5 {
		t.Fatalf("reconciliation final = %+v, want consistent/5", r)
	}
	assertSame(t, w.Balance.Amount, r.StoredBalance.Amount, "storedBalance")
}
