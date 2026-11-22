# Local helpers. The scored build is the Dockerfile.
CXX ?= clang++
FIXTURES ?= ../starter-duckdb/fixtures
SCRIPTS ?= ../starter-duckdb/scripts
PY ?= ../.venv/bin/python
DEV ?= ../data/dev
ANSWER ?= ../dev-answer

ss: src/main.cpp
	$(CXX) -std=c++17 -O3 -mcpu=native -Wall -Wno-unused-function -pthread -o ss src/main.cpp

check: ss
	$(PY) $(SCRIPTS)/run_fixtures.py $(FIXTURES) -- ./ss
	rm -rf out && mkdir out && ./ss $(DEV) out && $(PY) $(SCRIPTS)/compare.py out $(ANSWER)

image:
	docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
