# Azure LLM inference trace sample

This directory contains a short excerpt used by `cmd/replay`. It is not the full dataset.

- Source: [Azure Public Dataset, Azure LLM inference trace 2024](https://github.com/Azure/AzurePublicDataset/blob/master/AzureLLMInferenceDataset2024.md)
- File: `AzureLLMInferenceTrace_conv_1week.csv` (conversation service)
- License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The upstream repository license is Creative Commons Attribution 4.0 International.
- Collection: 2024-05-10 to 2024-05-19, described in Stojkovic et al., DynamoLLM, HPCA 2025.
- Excerpt: conversation requests from 2024-05-12 01:00:00Z through 01:03:00Z (three minutes). Columns are unchanged: `TIMESTAMP`, `ContextTokens`, `GeneratedTokens`.
- The full conversation CSV is about 1.1 GB and is not committed. Download it with `bash scripts/download-azure-trace.sh`. The filename is listed in `.gitignore`.
- This sample is only the slice the benchmark replays.

Please cite the DynamoLLM paper if you publish results that use this trace.
