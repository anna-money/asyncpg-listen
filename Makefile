all: deps lint test

deps:
	@python3 -m pip install --upgrade pip && pip3 install -r requirements-dev.txt

black:
	@black --line-length 120 asyncpg_listen tests

isort:
	@isort --line-length 120 --use-parentheses --multi-line 3 --combine-as --trailing-comma asyncpg_listen tests

pyright:
	pyright

flake8:
	@flake8 --max-line-length 120 --ignore C901,C812,E203 --extend-ignore W503 asyncpg_listen tests

lint: black isort flake8 pyright

test:
	@python3 -m pytest -vv --rootdir tests .

pyenv:
	echo asyncpg-listen > .python-version && pyenv install -s 3.11 && pyenv virtualenv -f 3.11 asyncpg-listen

pyenv-delete:
	pyenv virtualenv-delete -f asyncpg-listen
