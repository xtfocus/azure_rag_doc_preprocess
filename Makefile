.PHONY: build push deploy-create deploy-update clean
# Variables
DOCKER_IMAGE = tung-ingest
DOCKER_TAG=v1.0.1

DEV_ENV_FILE = .env.dev
PROD_ENV_FILE = .env.prod
HUB_ENV_FILE = .env.hub

# Azure image registry info
DEV_ACR_NAME = $(shell grep ACR_NAME .azure | cut -d '=' -f2)
DEV_IMAGE_NAME = $(shell grep IMAGE_NAME .azure | cut -d '=' -f2)
DEV_API_VERSION = $(shell grep API_VERSION .azure | cut -d '=' -f2)


PROD_ACR_NAME = $(shell grep ACR_NAME .azure.prod | cut -d '=' -f2)
PROD_IMAGE_NAME = $(shell grep IMAGE_NAME .azure.prod | cut -d '=' -f2)
PROD_API_VERSION = $(shell grep API_VERSION .azure.prod | cut -d '=' -f2)

HUB_ACR_NAME = $(shell grep ACR_NAME .azure.hub | cut -d '=' -f2)
HUB_IMAGE_NAME = $(shell grep IMAGE_NAME .azure.hub | cut -d '=' -f2)
HUB_API_VERSION = $(shell grep API_VERSION .azure.hub | cut -d '=' -f2)


build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

push: build
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(DEV_ACR_NAME).azurecr.io/$(DEV_IMAGE_NAME):$(DEV_API_VERSION)
	docker push $(DEV_ACR_NAME).azurecr.io/$(DEV_IMAGE_NAME):$(DEV_API_VERSION)

push-prod: build
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(PROD_ACR_NAME).azurecr.io/$(PROD_IMAGE_NAME):$(PROD_API_VERSION)
	docker push $(PROD_ACR_NAME).azurecr.io/$(PROD_IMAGE_NAME):$(PROD_API_VERSION)

push-hub: build
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(HUB_ACR_NAME).azurecr.io/$(HUB_IMAGE_NAME):$(HUB_API_VERSION)
	docker push $(HUB_ACR_NAME).azurecr.io/$(HUB_IMAGE_NAME):$(HUB_API_VERSION)


deploy-create: push
	./deploy.sh create -e $(DEV_ENV_FILE)
deploy-create-prod: push-prod
	./deploy_prod.sh create -e $(PROD_ENV_FILE)
deploy-create-hub: push-hub
	./deploy_hub.sh create -e $(HUB_ENV_FILE)


# Update existing deployment
deploy-update: push
	./deploy.sh update -e $(DEV_ENV_FILE)
deploy-update-prod: push-prod
	./deploy_prod.sh update -e $(PROD_ENV_FILE)
deploy-update-hub: push-hub
	./deploy_hub.sh update -e $(HUB_ENV_FILE)


clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.egg" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

