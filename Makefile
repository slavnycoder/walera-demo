COMPOSE := docker compose

.PHONY: up down reset logs ps psql backend-logs walera-logs

up:
	$(COMPOSE) up -d --build
	@echo
	@echo "  Frontend: http://localhost:$${FRONTEND_HOST_PORT:-8081}"
	@echo "  Backend : http://localhost:$${BACKEND_HOST_PORT:-8000}"
	@echo "  Walera  : http://localhost:$${WALERA_HOST_PORT:-8080}"
	@echo "  Postgres: localhost:$${PG_HOST_PORT:-5432}  (user=walera pwd=walera)"
	@echo
	@echo "Open the frontend in TWO browser tabs and click checkboxes."

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

psql:
	$(COMPOSE) exec postgres psql -U walera -d walera

backend-logs:
	$(COMPOSE) logs -f --tail=200 backend

walera-logs:
	$(COMPOSE) logs -f --tail=200 walera
