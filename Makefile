.PHONY: default deps lint format test test_integration shell localstack deploy upload

default: deps lint
	$(info )
	$(info Ready to deploy.)
	$(info Please run 'make localstack' in a separate terminal. And 'make deploy' in another.)
	$(info After deploying, run 'make test && make test_integration' to run the tests.)
	$(info To process example Raw data files from /data directory with upload script, run 'make upload')

deps:
	pip install . ".[dev]"

lint:
	ruff check . 
	sam validate --lint

format:
	ruff format .

test:
	pytest -s -v

test_integration:
	pytest -s -v --run-integration -m integration

shell:
	python

localstack:
	localstack start

deploy:
	sam build
	sam deploy --config-env local

upload:
	python src/data_asset_uploader/raw_data_files_S3_uploader.py
