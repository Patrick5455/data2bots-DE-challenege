.PHONY: build, run, run-marts, test, compile, acc-run, acc-test, lint, fix, local-dev, venv, requirements, precommit-install, refresh

#################################################################################
# GLOBALS                                                                       #
#################################################################################
DOCKER_REPO = patrick5455/patrojun6040_dbt
IMAGE = $(DOCKER_REPO):v1
ENVIRONMENT = dev
ENVS = -e ENV=$(ENVIRONMENT) \
-e POSTGRES_USER=${POSTGRES_USER} \
-e POSTGRESS_PASSWORD=${POSTGRESS_PASSWORD} \
-e POSTGRES_SCHEMA=${POSTGRES_SCHEMA}
TEST_ENVS= -e ENV=test \
-e POSTGRES_USER=${POSTGRES_USER} \
-e POSTGRESS_PASSWORD=${POSTGRESS_PASSWORD} \
VOLUMES = -v $(PWD)/data2bots-DE-challenge:/dbt --user 1000:1000
PROFILES = --profiles-dir /dbt/profile --profile postgres-dwh
VENV_DIR = env
PIP = $(VENV_DIR)/bin/pip
#################################################################################
# COMMANDS                                                                       #
#################################################################################

all: docker-build run

docker-build:
	docker build --no-cache -t $(IMAGE) .

push:
	docker push $(IMAGE)

build: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) build $(PROFILES)

run: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) run $(PROFILES)

run-staging: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) run $(PROFILES) --model staging.*

run-analysis: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) run $(PROFILES) --model analyis.*

clean: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) clean $(PROFILES)

deps: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) deps $(PROFILES)

seed: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) seed --no-version-check $(PROFILES)

test: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) test --no-version-check $(PROFILES)

compile: refresh
	docker run $(VOLUMES) $(TEST_ENVS) $(IMAGE) compile $(PROFILES)