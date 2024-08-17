

run_tests:
	pytest test.py

lint:
	flake8

fmt:
	isort --profile black .
	black --line-length=120 .