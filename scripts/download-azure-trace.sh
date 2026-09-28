#!/usr/bin/env bash
# Download the Azure LLM inference conversation trace (about 1.1 GB).
# The file is gitignored. The benchmark replays testdata/azure_llm_2024_conv_sample.csv
# and does not need this download.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${1:-$ROOT/testdata/AzureLLMInferenceTrace_conv_1week.csv}"
URL="https://github.com/Azure/AzurePublicDataset/releases/download/dataset-llm-2024/AzureLLMInferenceTrace_conv_1week.csv"

mkdir -p "$(dirname "$DEST")"
echo "downloading conversation trace to $DEST"
curl -fL --retry 3 --retry-delay 2 -o "$DEST.partial" "$URL"
mv "$DEST.partial" "$DEST"
echo "wrote $DEST ($(wc -c < "$DEST") bytes)"
