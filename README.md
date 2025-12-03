# Braintrust vs LangSmith: Load Performance Comparison

A head-to-head benchmark comparing trace ingestion performance between **Braintrust** and **LangSmith** observability platforms.

## Getting Started

### 1. Installation & Setup

**Install dependencies:**

If using `uv`:
```bash
uv sync
```

If not using `uv`:
```bash
pip install .
```

**Configure API keys:**
```bash
cp .env.example .env
```

Edit `.env` and update the following:
- `BRAINTRUST_API_KEY` - Your Braintrust API key
- `LANGSMITH_API_KEY` - Your LangSmith API key

### 2. Prepare Test Data

**Before running any benchmark scripts**, download and prepare the trace data:

```bash
python prepare_data.py
```

This will:
- Download 768MB of compressed trace data from a public S3 bucket
- Extract to `data/big_traces.jsonl` (1.7GB uncompressed)
- Clean up temporary files

### 3. Run Benchmarks

Both scripts support the same command-line arguments for consistent testing:

**Braintrust:**
```bash
# Load all traces once
python load_braintrust.py

# Run 5 iterations for performance testing
python load_braintrust.py -n 5
```

**LangSmith:**
```bash
# Load all traces once
python load_langsmith.py

# Run 5 iterations for performance testing
python load_langsmith.py -n 5
```

**Common Options:**
- `-n, --iterations` - Number of times to replay traces (default: 1)
- `-l, --limit` - Limit number of trace rows to load

## What This Benchmark Tests

This benchmark loads 100 production-scale traces (1.7GB uncompressed) into both platforms and measures:

- **Query latency** - How long it takes to query the 100 traces after ingestion
- **UI responsiveness** - How quickly the web interface loads and displays traces
- **Trace rendering** - How well each platform handles displaying complex nested spans
- **Search/filter performance** - How fast you can find specific traces or spans

## Test Data: Why Size Matters

### The Dataset

**`data/big_traces.jsonl`** - 100 production-scale agentic AI traces

**Key characteristics:**
- **Size**: 1.7GB uncompressed, 768MB compressed
- **Structure**: Nested span hierarchies (parent-child relationships)
- **Realism**: Real-world metadata, inputs, outputs, and attributes
- **Complexity**: Multi-step agent workflows with tool calls, reasoning chains, and parallel execution

### Why These Traces Are Representative

Modern agentic AI applications generate increasingly large and complex traces:

**1. Multi-Step Reasoning**
- [Agentic workflows now span multiple steps](https://www.marktechpost.com/2025/08/09/9-agentic-ai-workflow-patterns-transforming-ai-agents-in-2025/), with each step creating new spans
- [Production monitoring shows 75.3% mean completion rate](https://digitaldefynd.com/IQ/agentic-ai-statistics/) for complex multi-step tasks
- More steps = more spans = larger traces

**2. Context Window Expansion**
- [Recent models expanded context to 128K-1M tokens](https://svitla.com/blog/agentic-ai-trends-2025/) (Llama 3.1, Claude Sonnet 4)
- Larger contexts mean more data captured in each span
- [End-to-end tracing](https://opentelemetry.io/blog/2024/llm-observability/) for entire agent workflows became standard in 2024-2025

### What This Benchmark Tests

**Real Production Scenarios:**
- ✅ **Agentic workflows** with 10+ spans per trace
- ✅ **Large payloads** from context-heavy LLM calls
- ✅ **Nested hierarchies** reflecting tool use and sub-agents
- ✅ **Batch ingestion** of 100 traces simultaneously
- ✅ **Network efficiency** under large payload stress

**Why 1.7GB Matters:**
- Most benchmarks test small, synthetic traces
- Real agent traces are **large and complex**
- This dataset stresses platforms in production-realistic ways
- Reveals timeout handling, batching efficiency, and scalability limits
