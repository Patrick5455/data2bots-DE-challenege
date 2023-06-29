.PHONY: all, db, dp, build, run, run-analytics, clean, deps, test, compile, refresh

#################################################################################
# GLOBALS                                                                       #
#################################################################################
DOCKER_REPO=patrick5455/patrojun6040_dbt
IMAGE=$(DOCKER_REPO):dbt-dags-v1
ENVIRONMENT=dev
ENVS= -e ENV=$(ENVIRONMENT) \
-e DB_USER=${DB_USER} \
-e DB_PASSWORD=${DB_PASSWORD}
TEST_ENVS= -e ENV = test \
-e DB_USER = ${DB_USER} \
-e DB_PASSWORD = ${DB_PASSWORD}
VOLUMES=-v $(PWD)/patrojun6040:/dbt --user 1000:1000
PROFILES=--profiles-dir /dbt/profile --profile postgres-dwh
#################################################################################
# COMMANDS                                                                       #
#################################################################################

all: db dp run

db:
	docker build --no-cache -t $(IMAGE) .

dp:
	docker push $(IMAGE)

build: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) build $(PROFILES)

run: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) run $(PROFILES)

run-analytics: refresh
	docker run $(VOLUMES) $(ENVS) $(IMAGE) run $(PROFILES) --model analytics.*

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