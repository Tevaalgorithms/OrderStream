# OrderStream — Developer Makefile
# Run 'make help' to see all commands

.PHONY: help up down logs migrate seed place-order check-order analytics test clean

help:
	@echo ""
	@echo "OrderStream Microservices — Available Commands"
	@echo "────────────────────────────────────────────────"
	@echo "  make up           Start all services (LocalStack, DBs, services)"
	@echo "  make down         Stop and remove all containers"
	@echo "  make logs         Tail logs from all services"
	@echo "  make migrate      Run DB migrations for all services"
	@echo "  make seed         Seed product catalog into Inventory Service"
	@echo "  make place-order  Place a test order (watch the saga unfold)"
	@echo "  make check-order  Check the status of ORDER_ID=<uuid>"
	@echo "  make analytics    Fetch analytics summary"
	@echo "  make test         Run integration tests"
	@echo "  make clean        Remove all volumes (fresh start)"
	@echo ""

up:
	docker compose up --build -d
	@echo ""
	@echo "✅ OrderStream is starting..."
	@echo "   API Gateway:          http://localhost:8000"
	@echo "   Order Service:        http://localhost:8001/docs"
	@echo "   Inventory Service:    http://localhost:8002/docs"
	@echo "   Payment Service:      http://localhost:8003/docs"
	@echo "   Notification Service: http://localhost:8004/docs"
	@echo "   Analytics Service:    http://localhost:8005/docs"
	@echo "   LocalStack:           http://localhost:4566"
	@echo ""
	@echo "⏳ Wait ~15s for LocalStack to initialize, then run: make seed"

down:
	docker compose down

logs:
	docker compose logs -f --tail=50

logs-order:
	docker compose logs -f order-service

logs-inventory:
	docker compose logs -f inventory-service

logs-payment:
	docker compose logs -f payment-service

logs-notification:
	docker compose logs -f notification-service

logs-analytics:
	docker compose logs -f analytics-service

seed:
	@echo "🌱 Seeding product catalog..."
	curl -s -X POST http://localhost:8000/products \
	  -H "Content-Type: application/json" \
	  -d @infrastructure/seeds/products.json | python3 -m json.tool
	@echo "✅ Products seeded"

place-order:
	@echo "📦 Placing test order..."
	@ORDER_RESPONSE=$$(curl -s -X POST http://localhost:8000/orders \
	  -H "Content-Type: application/json" \
	  -d @infrastructure/seeds/test_order.json); \
	echo "$$ORDER_RESPONSE" | python3 -m json.tool; \
	echo ""; \
	ORDER_ID=$$(echo "$$ORDER_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null); \
	echo "✅ Order placed: $$ORDER_ID"; \
	echo "👉 Watch logs: make logs"; \
	echo "👉 Check status: make check-order ORDER_ID=$$ORDER_ID"

check-order:
	@if [ -z "$(ORDER_ID)" ]; then echo "Usage: make check-order ORDER_ID=<uuid>"; exit 1; fi
	curl -s http://localhost:8000/orders/$(ORDER_ID) | python3 -m json.tool

analytics:
	@echo "📊 Analytics Summary..."
	curl -s http://localhost:8000/analytics/orders/summary | python3 -m json.tool

test:
	pytest tests/ -v

clean:
	docker compose down -v
	@echo "🗑️  All volumes removed — fresh start with 'make up'"
