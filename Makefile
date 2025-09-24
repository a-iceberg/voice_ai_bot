setup-js:
	npm install
	npm install libphonenumber-js winston winston-daily-rotate-file

setup-py:
	python3 -m venv venv && . venv/bin/activate && pip install -U pip requests

setup: setup-js setup-py