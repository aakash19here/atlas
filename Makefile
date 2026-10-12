include .env
export

migrate-new:    ; goose create $(name) sql
migrate-up:     ; goose up
migrate-down:   ; goose down
migrate-status: ; goose status
sqlc:           ; sqlc generate
up: 			; docker compose up -d

simulator: 		; go run ./cmd/simulator
ingestor: 		; go run ./cmd/ingestor
alerter:		; go run ./cmd/alerter
