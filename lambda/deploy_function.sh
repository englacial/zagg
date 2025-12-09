#!/bin/bash
# Deploy Lambda function package
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUNCTION_NAME="process-morton-cell"
LAYER_ZIP="../lambda_layers/xagg-complete-layer.zip"

echo "======================================================================="
echo "Packaging Lambda Function: $FUNCTION_NAME"
echo "======================================================================="
echo ""

# Create deployment package
PACKAGE_DIR="$SCRIPT_DIR/package"
rm -rf "$PACKAGE_DIR"
mkdir -p "$PACKAGE_DIR"

echo "Copying function code..."
cp "$SCRIPT_DIR/lambda_handler.py" "$PACKAGE_DIR/"
cp "$SCRIPT_DIR/query_cmr_with_polygon.py" "$PACKAGE_DIR/"

echo ""
echo "Creating deployment zip..."
cd "$PACKAGE_DIR"
zip -r9 -q ../function.zip .
cd ..

echo "✓ Created function.zip ($(du -h function.zip | cut -f1))"
echo ""

# Clean up
rm -rf "$PACKAGE_DIR"

echo "======================================================================="
echo "Deployment Package Ready"
echo "======================================================================="
echo ""
echo "Files:"
echo "  - function.zip: Lambda function code"
echo "  - ../lambda_layers/xagg-complete-layer.zip: Lambda layer (dependencies)"
echo ""
echo "Next steps:"
echo ""
echo "1. Upload Lambda layer:"
echo "   aws lambda publish-layer-version \\"
echo "     --layer-name xagg-complete-stack \\"
echo "     --zip-file fileb://$SCRIPT_DIR/$LAYER_ZIP \\"
echo "     --compatible-runtimes python3.11 \\"
echo "     --description 'xagg complete stack: numpy, pandas, xarray, xdggs, h5coro, mortie'"
echo ""
echo "2. Create Lambda function (or update existing):"
echo "   aws lambda create-function \\"
echo "     --function-name $FUNCTION_NAME \\"
echo "     --runtime python3.11 \\"
echo "     --role arn:aws:iam::ACCOUNT_ID:role/lambda-execution-role \\"
echo "     --handler lambda_handler.lambda_handler \\"
echo "     --zip-file fileb://$SCRIPT_DIR/function.zip \\"
echo "     --timeout 720 \\"
echo "     --memory-size 2048 \\"
echo "     --environment Variables={NASA_EARTHDATA_SECRET=nasa-earthdata} \\"
echo "     --layers arn:aws:lambda:REGION:ACCOUNT_ID:layer:xagg-complete-stack:VERSION"
echo ""
echo "3. Or update existing function:"
echo "   aws lambda update-function-code \\"
echo "     --function-name $FUNCTION_NAME \\"
echo "     --zip-file fileb://$SCRIPT_DIR/function.zip"
echo ""
