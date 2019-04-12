.PHONY: prepare-test
prepare-test:
		pip install pyvows coverage tornado_pyvows

.PHONY: test
test:
		@echo "Restart MongoDB"
		@docker-compose down
		@docker-compose up -d
		@echo "Run Vows"
		@pyvows -c -l tc_mongodb
