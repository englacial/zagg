#!/bin/bash
# Deploy Lambda function to AWS
# Usage: ./deploy_lambda.sh

set -e

FUNCTION_NAME="process-morton-cell"
REGION="us-west-2"

echo "Creating deployment package..."
rm -f lambda_function.zip
zip lambda_function.zip lambda_handler.py

echo "Deploying to AWS Lambda..."
aws lambda update-function-code \
    --function-name $FUNCTION_NAME \
    --zip-file fileb://lambda_function.zip \
    --region $REGION

echo "Done! Function deployed: $FUNCTION_NAME"
