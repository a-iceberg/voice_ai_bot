setup-js:
	npm install

setup-py:
	python3 -m venv venv && . venv/bin/activate && pip install -U pip requests

setup: setup-js setup-py