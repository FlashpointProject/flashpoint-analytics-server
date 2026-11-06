#!make
-include .env
export $(shell sed 's/=.*//' .env)

.PHONY: db db-up migrate run seed

db-up:
	docker-compose -p ${DB_CONTAINER_NAME} -f dc-db.yml up -d

db:
	docker-compose -p ${DB_CONTAINER_NAME} -f dc-db.yml down -v
	docker-compose -p ${DB_CONTAINER_NAME} -f dc-db.yml up -d

migrate:
	docker run --rm -v $(shell pwd)/migrations:/migrations --network host migrate/migrate -path=/migrations/ -database "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_IP}:${DB_PORT}/${DB_NAME}?sslmode=disable" up

run:
	export GIT_COMMIT=$(shell git rev-list -1 HEAD) && go run ./main/...

seed:
	go run ./cmd/seed
