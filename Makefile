# Local helpers. The scored build is the Dockerfile.
# `CXX ?=` is a no-op (make predefines CXX=g++), so set it outright; override with `make CXX=clang++`.
CXX := g++
CXXFLAGS ?= -std=c++17 -O3 -march=native -Wall -Wno-unused-function -pthread

# Bench/verification home (outside the repo); see $(BENCH)/README.md.
BENCH ?= $(HOME)/ss-bench
FIXTURES ?= $(BENCH)/starter/fixtures
SCRIPTS ?= $(BENCH)/starter/scripts
PY ?= $(BENCH)/venv/bin/python
DEV ?= $(BENCH)/data/dev
ANSWER ?= $(BENCH)/dev-answer
DATASET ?= dev

ss: src/main.cpp
	$(CXX) $(CXXFLAGS) -o ss src/main.cpp

check: ss
	cd $(SCRIPTS) && $(PY) run_fixtures.py $(FIXTURES) -- $(CURDIR)/ss
	rm -rf out && mkdir out && ./ss $(DEV) out && $(PY) $(SCRIPTS)/compare.py out $(ANSWER)

# Full correctness gate (fixtures, dev, extra seeds, p20m, f60m). QUICK=1 skips 20M/60M.
gate: ss
	$(BENCH)/gate.sh $(CURDIR)/ss

# Timing: median of 5 (warm 8 cores, 4 cores, cold cache, docker). `make bench DATASET=p20m MODES=warm8`
bench: ss
	SRC=$(CURDIR)/src/main.cpp REPO=$(CURDIR) $(BENCH)/bench.sh $(CURDIR)/ss $(DATASET)

image:
	docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .

.PHONY: check gate bench image
