lint:
	black src tests

lint-check:
	black src tests --check

test:
	coverage run -m pytest
	coverage report
	coverage html