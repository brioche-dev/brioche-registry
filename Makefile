.PHONY: dev
dev:
	cargo watch -x 'run -- serve'

.PHONY: update-db-schema
update-db-schema:
# Run cargo check first to make sure `brioche` gets build. Otherwise, sqlx
# will try to check Brioche's queries and fail
	cargo check || true
	mkdir -p ./.sqlx
	DATABASE_URL=sqlite://$(CURDIR)/.sqlx/schema.db?mode=rwc cargo sqlx migrate run
	DATABASE_URL=sqlite://$(CURDIR)/.sqlx/schema.db cargo sqlx prepare
