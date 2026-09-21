#!/bin/bash
# Provisionamento das filas FIFO do LocalStack (executado no init/ready.d).
# LocalStack não autentica; o accountId padrão é 000000000000.
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"

echo "[sqs-init] criando filas em ${REGION}"

# DLQ das operações de entrada (wager-transactions.fifo).
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes '{
  "FifoQueue": "true",
  "ContentBasedDeduplication": "false",
  "VisibilityTimeout": "30"
}' >/dev/null

# Fila de entrada das operações (HTTP e SQS compartilham o mesmo caso de uso).
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes "{
  \"FifoQueue\": \"true\",
  \"ContentBasedDeduplication\": \"false\",
  \"VisibilityTimeout\": \"30\",
  \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions-dlq.fifo\\\",\\\"maxReceiveCount\\\":5}\"
}" >/dev/null

# Fila de saída dos eventos da outbox (wager-events.fifo).
awslocal sqs create-queue --queue-name wager-events.fifo --attributes '{
  "FifoQueue": "true",
  "ContentBasedDeduplication": "false",
  "VisibilityTimeout": "30"
}' >/dev/null

echo "[sqs-init] filas:"
awslocal sqs list-queues