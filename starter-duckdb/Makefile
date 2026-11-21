IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
NATIVE_AMD64_ARCH ?= x86-64-v4
NATIVE_ROLLING_AVX512 ?= 1
DEV   ?= dev
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
export IMAGE CPUS MEM PLATFORM

.PHONY: build native fixtures dev run-dev check-dev

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --build-arg NATIVE_AMD64_ARCH=$(NATIVE_AMD64_ARCH) \
		--build-arg NATIVE_ROLLING_AVX512=$(NATIVE_ROLLING_AVX512) --load -t $(IMAGE) ..

## Build the native library for running pipeline.py directly on Linux
native: pipeline/shopping_native.so

pipeline/shopping_native.so: pipeline/native_pipeline.cpp $(wildcard pipeline/*.hpp)
	$(CXX) -std=c++20 -O3 -DNDEBUG -march=native -fPIC -shared -pthread $< -o $@

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer (dev-answer.zip, unzipped): make check-dev ANSWER=dev-answer
ANSWER ?= dev-answer
check-dev:
	python3 scripts/compare.py out $(ANSWER)
