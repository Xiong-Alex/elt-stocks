up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

smoke:
	powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1 -NoBuild

smoke-build:
	powershell -ExecutionPolicy Bypass -File scripts/smoke-test.ps1
