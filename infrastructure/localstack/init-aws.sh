#!/bin/bash
# ============================================================
# LocalStack Initialization Script
# Runs automatically when LocalStack starts (ready.d hook)
# Creates all SQS queues and Kinesis streams needed by OrderStream
#
# CONCEPT: Infrastructure as Code
# We define our messaging infrastructure as code here.
# In production this would be Terraform or AWS CDK.
# ============================================================

set -e

echo "🚀 Initializing OrderStream AWS infrastructure in LocalStack..."

# ─────────────────────────────────────────────
# SQS Queues
# CONCEPT: Dead Letter Queue (DLQ)
# After maxReceiveCount failures, SQS moves the message to the DLQ.
# This prevents poison-pill messages from blocking the queue forever.
# Ops can inspect the DLQ to investigate root causes.
# ─────────────────────────────────────────────

echo "📬 Creating SQS Dead Letter Queues..."
awslocal sqs create-queue --queue-name order-created-dlq
awslocal sqs create-queue --queue-name inventory-reserved-dlq
awslocal sqs create-queue --queue-name inventory-failed-dlq
awslocal sqs create-queue --queue-name payment-processed-dlq
awslocal sqs create-queue --queue-name payment-failed-dlq

echo "📬 Creating SQS Main Queues with DLQ redrive policies..."

# order-created-queue: produced by Order Service, consumed by Inventory Service
ORDER_CREATED_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/order-created-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue \
  --queue-name order-created-queue \
  --attributes "{
    \"VisibilityTimeout\": \"30\",
    \"MessageRetentionPeriod\": \"345600\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${ORDER_CREATED_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }"

# inventory-reserved-queue: produced by Inventory Service, consumed by Payment Service
INV_RESERVED_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/inventory-reserved-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue \
  --queue-name inventory-reserved-queue \
  --attributes "{
    \"VisibilityTimeout\": \"30\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${INV_RESERVED_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }"

# inventory-failed-queue: produced by Inventory Service (out of stock), consumed by Notification Service
INV_FAILED_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/inventory-failed-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue \
  --queue-name inventory-failed-queue \
  --attributes "{
    \"VisibilityTimeout\": \"30\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${INV_FAILED_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }"

# payment-processed-queue: produced by Payment Service, consumed by Notification Service
PAY_PROCESSED_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/payment-processed-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue \
  --queue-name payment-processed-queue \
  --attributes "{
    \"VisibilityTimeout\": \"30\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${PAY_PROCESSED_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }"

# payment-failed-queue: produced by Payment Service, consumed by Inventory + Notification
PAY_FAILED_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/payment-failed-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue \
  --queue-name payment-failed-queue \
  --attributes "{
    \"VisibilityTimeout\": \"30\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${PAY_FAILED_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }"

echo "✅ All SQS queues created"

# ─────────────────────────────────────────────
# Kinesis Streams
# CONCEPT: Kinesis Sharding + Partition Key
# We use order_id as the partition key so all events for the same order
# land on the same shard → guaranteed ordering per order.
# 2 shards = parallel processing of 2 groups of orders simultaneously.
# ─────────────────────────────────────────────

echo "🌊 Creating Kinesis streams..."
awslocal kinesis create-stream \
  --stream-name ecommerce-events-stream \
  --shard-count 2

# Wait for stream to become ACTIVE
echo "⏳ Waiting for Kinesis stream to become ACTIVE..."
awslocal kinesis wait stream-exists --stream-name ecommerce-events-stream

echo "✅ Kinesis stream 'ecommerce-events-stream' is ACTIVE with 2 shards"

echo ""
echo "🎉 OrderStream infrastructure initialized successfully!"
echo "   SQS Queues: order-created | inventory-reserved | inventory-failed | payment-processed | payment-failed"
echo "   Kinesis:    ecommerce-events-stream (2 shards)"
