
#!/bin/bash

# Deployment script to deploy the app to QAI resource

# Function to display usage information
usage() {
    echo "Usage: $0 {create|update} -e <env_file>"
    exit 1
}

# Check if at least two arguments are provided
if [ $# -lt 2 ]; then
    usage
fi

# Parse command-line arguments
action=$1
shift

while getopts "e:" opt; do
    case $opt in
        e) env_file=$OPTARG ;;
        *) usage ;;
    esac
done

# Check if env_file is provided
if [ -z "$env_file" ]; then
    usage
fi

# Source Azure configuration
set -a
source ./.azure # Set API_NAME, RESOURCE_GROUP
set +a

# Process environment variables
envs=$(sed 's/[[:space:]]*#.*$//' "$env_file" | sed '/^[[:space:]]*$/d' | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//' | tr '\n' ' ' | sed 's/ $//')

# Perform action based on the command
case $action in
    create)
         az containerapp create \
          --name $API_NAME \
          --resource-group $RESOURCE_GROUP \
          --environment $CONTAINER_ENVIRONMENT \
          --image $ACR_NAME.azurecr.io/$IMAGE_NAME:$API_VERSION \
          --target-port 3100 \
          --ingres internal \
          --registry-server $ACR_NAME.azurecr.io \
          --query properties.configuration.ingress.fqdn \
          --tags ProjectID=$PROJECT_ID ApplicationName=$PROJECT_ID OwnerService=$OWNERSERVICE PIC=$PIC \
          --env-vars $envs \
          --cpu $ALLOCATED_CPU --memory $ALLOCATED_MEMORY \
          --min-replicas $MINREPLICAS --max-replicas $MAXREPLICAS \
          --subscription $SUBSCRIPTION
         
      
          echo "Container app created with name $API_NAME, in resource group $RESOURCE_GROUP, in container environment $CONTAINER_ENVIRONMENT, from registry $ACR_NAME"
          ;;

    update)
      
      az containerapp update -n $API_NAME --resource-group $RESOURCE_GROUP --set-env-vars $envs --subscription $SUBSCRIPTION
      echo "Container app updated with name $API_NAME in resource group $RESOURCE_GROUP"
      ;;

    *)
        usage
        ;;
esac
