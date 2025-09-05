# mzcode - Universal ETL Analysis & Migration Intelligence Platform

Transform your ETL packages into intelligent, searchable knowledge graphs with AI-powered business insights. Supports SSIS, Informatica, and extensible to other ETL platforms.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+ 
- [uv package manager](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Docker (optional, for Memgraph backend)

### Platform Notes
**Windows Users**: Use PowerShell or CMD syntax for environment variables:
- PowerShell: `$env:VAR_NAME="value"`
- CMD: `set VAR_NAME=value`
- Linux/Mac: `export VAR_NAME=value` or `VAR_NAME=value command`

### Installation
```bash
git clone https://github.com/ai4data/mzcode.git
cd metazcode
uv sync
```

## 📊 Basic Usage

### Analyze ETL Project (Default)
```bash
# Complete analysis with NetworkX (in-memory)
uv run python -m metazcode full --path /path/to/etl/project

# With custom output
uv run python -m metazcode full --path /path/to/etl/project --output results.json
```

### Enable AI-Powered Business Summaries
```bash
# Set API key (OpenAI or OpenRouter)
export OPENAI_API_KEY=your-api-key
# OR
export OPENROUTER_API_KEY=your-api-key

# Run with AI enrichment (NetworkX backend)
uv run python -m metazcode full --path /path/to/etl/project --enable-llm

# Run with AI enrichment + Memgraph backend
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path /path/to/etl/project --enable-llm
```

### Use Persistent Database (Enterprise)
```bash
# Start Memgraph
docker-compose up -d

# Linux/Mac - Run with Memgraph backend
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path /path/to/etl/project

# Windows PowerShell - Run with Memgraph backend
$env:METAZCODE_DB_BACKEND="memgraph"
uv run python -m metazcode full --path /path/to/etl/project

# Windows CMD - Run with Memgraph backend
set METAZCODE_DB_BACKEND=memgraph
uv run python -m metazcode full --path /path/to/etl/project

# With AI enrichment (Windows PowerShell example)
$env:OPENAI_API_KEY="your-api-key"
$env:METAZCODE_DB_BACKEND="memgraph"
uv run python -m metazcode full --path /path/to/etl/project --enable-llm
```

## 🎯 Key Features

- **Multi-Platform Support**: SSIS, Informatica (more coming)
- **Dependency Analysis**: Cross-package dependencies, execution order
- **AI Enrichment**: Business-focused summaries via GPT-4o-mini
- **Search Capability**: BM25 indexed metadata for fast queries
- **Dual Backend**: NetworkX (in-memory) or Memgraph (persistent)
- **Migration Ready**: JSON output for migration tools

## 🔧 Available Commands

### Core Analysis Commands
```bash
# Full pipeline (ingestion + analysis + indexing)
uv run python -m metazcode full --path <project>

# Individual phases
uv run python -m metazcode ingest --path <project>    # Extract metadata
uv run python -m metazcode analyze --path <project>   # Dependency analysis
uv run python -m metazcode enrich --path <project>    # AI enrichment only

# Combined operations
uv run python -m metazcode ingest-n-index --path <project>  # Ingest + Index
```

### Export & Visualization
```bash
# Export graph to JSON
uv run python -m metazcode dump --path <project> --output graph.json

# Generate visualization (requires matplotlib)
uv run python -m metazcode visualize --path <project>
```

## 🤖 LLM Providers & Models

### OpenAI (Default)
```bash
export OPENAI_API_KEY=your-key

# Default: GPT-4o-mini (fast and cost-effective) with NetworkX
uv run python -m metazcode full --path <project> --enable-llm

# Explicit OpenAI with different models (NetworkX)
uv run python -m metazcode full --path <project> --enable-llm --provider openai --model gpt-4o-mini
uv run python -m metazcode full --path <project> --enable-llm --provider openai --model gpt-4o
uv run python -m metazcode full --path <project> --enable-llm --provider openai --model gpt-4

# With Memgraph backend (default model)
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path <project> --enable-llm

# With Memgraph backend and specific models
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path <project> --enable-llm --provider openai --model gpt-4o
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path <project> --enable-llm --provider openai --model gpt-4

# Using environment variables for configuration
export OPENAI_API_KEY=your_openai_key
export METAZCODE_LLM_PROVIDER=openai
export METAZCODE_LLM_MODEL=gpt-4o-mini
export METAZCODE_DB_BACKEND=memgraph  # Optional
uv run python -m metazcode full --path <project> --enable-llm
```

### OpenRouter (Multi-Model)
```bash
export OPENROUTER_API_KEY=your-key

# DeepSeek (very cost-effective, high quality) with NetworkX
uv run python -m metazcode full --path <project> --enable-llm \
  --provider openrouter --model "deepseek/deepseek-chat"

# Anthropic Claude (excellent reasoning) with NetworkX
uv run python -m metazcode full --path <project> --enable-llm \
  --provider openrouter --model "anthropic/claude-3.5-sonnet"

# Meta Llama (open source) with NetworkX
uv run python -m metazcode full --path <project> --enable-llm \
  --provider openrouter --model "meta-llama/llama-3.1-8b-instruct"

# Google Gemini (competitive performance) with NetworkX
uv run python -m metazcode full --path <project> --enable-llm \
  --provider openrouter --model "google/gemini-pro-1.5"

# With Memgraph backend - DeepSeek (Linux/Mac)
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path <project> --enable-llm \
  --provider openrouter --model "deepseek/deepseek-chat"

# With Memgraph backend - DeepSeek (Windows PowerShell)
$env:METAZCODE_DB_BACKEND="memgraph"
uv run python -m metazcode full --path <project> --enable-llm `
  --provider openrouter --model "deepseek/deepseek-chat"

# With Memgraph backend - Claude (Linux/Mac)
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path <project> --enable-llm \
  --provider openrouter --model "anthropic/claude-3.5-sonnet"

# With Memgraph backend - Claude (Windows PowerShell)
$env:METAZCODE_DB_BACKEND="memgraph"
uv run python -m metazcode full --path <project> --enable-llm `
  --provider openrouter --model "anthropic/claude-3.5-sonnet"

# Using environment variables for OpenRouter
export OPENROUTER_API_KEY=your_openrouter_key
export OPENROUTER_SITE_URL=https://yoursite.com  # Optional: for rankings
export OPENROUTER_SITE_NAME="YourProject"        # Optional: for rankings
export METAZCODE_LLM_PROVIDER=openrouter
export METAZCODE_LLM_MODEL="deepseek/deepseek-chat"
export METAZCODE_DB_BACKEND=memgraph  # Optional
uv run python -m metazcode full --path <project> --enable-llm
```

### Model Comparison
| Model | Cost | Speed | Quality | Use Case |
|-------|------|-------|---------|----------|
| gpt-4o-mini | $$ | Fast | Good | Default, balanced |
| deepseek/deepseek-chat | $ | Fast | Excellent | Best value |
| claude-3.5-sonnet | $$$$ | Moderate | Best | Complex logic |

## 🗄️ Backend Options

### NetworkX (Default)
- **Use When**: Small/medium projects (<100 packages)
- **Pros**: No setup, fast, lightweight
- **Cons**: No persistence between runs

### Memgraph (Enterprise)
- **Use When**: Large projects, team collaboration, persistent analysis
- **Pros**: Persistent storage, analytics views, Cypher queries
- **Setup**:
```bash
docker-compose up -d
export METAZCODE_DB_BACKEND=memgraph
```

## 📁 Supported ETL Technologies

### Currently Supported
- **SSIS**: .dtsx, .conmgr, .params, .dtproj files
- **Informatica**: .xml workflow/mapping files

### File Discovery
The tool automatically discovers and processes:
- Package definitions and workflows
- Data flow components and transformations
- SQL operations and stored procedures
- Connection managers and parameters
- Cross-package dependencies

## 🔍 Output Structure

### Analysis Results (JSON)
```json
{
  "nodes": [...],           // All ETL components
  "links": [...],           // Relationships/dependencies
  "execution_order": [...], // Parallel execution groups
  "shared_resources": {...},// Bottleneck analysis
  "llm_summaries": {...}   // AI-generated insights (if enabled)
}
```

### Node Properties
- `id`: Unique identifier
- `node_type`: pipeline, operation, data_asset, connection
- `technology`: SSIS, Informatica, etc.
- `llm_summary`: Business description (with AI enrichment)
- `source_file_path`: Traceability to source

## 🧪 Testing with Sample Data

```bash
# Test with included SSIS samples (NetworkX)
uv run python -m metazcode full --path data/ssis/dataWH_ssis

# With AI enrichment (NetworkX)
uv run python -m metazcode full --path data/ssis/dataWH_ssis --enable-llm

# With Memgraph backend only
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path data/ssis/dataWH_ssis

# With Memgraph + AI enrichment (best combination)
export OPENAI_API_KEY=your-key
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path data/ssis/dataWH_ssis --enable-llm
```

## ⚙️ Configuration

### Environment Variables
```bash
# Database backend
METAZCODE_DB_BACKEND=networkx|memgraph  # Default: networkx

# Memgraph connection (if using)
MEMGRAPH_HOST=localhost
MEMGRAPH_PORT=7687
MEMGRAPH_USERNAME=admin  # Optional
MEMGRAPH_PASSWORD=admin  # Optional

# LLM configuration
OPENAI_API_KEY=your-key
OPENROUTER_API_KEY=your-key
METAZCODE_LLM_PROVIDER=openai|openrouter
METAZCODE_LLM_MODEL=gpt-4o-mini
METAZCODE_LLM_BATCH_SIZE=10
```

### Configuration File (.env)
```env
METAZCODE_DB_BACKEND=memgraph
OPENAI_API_KEY=your-openai-key
METAZCODE_LLM_PROVIDER=openai
METAZCODE_LLM_MODEL=gpt-4o-mini
```

## 🚨 Troubleshooting

| Issue | Solution |
|-------|----------|
| "No .dtsx files found" | Verify path contains ETL files: `ls path/*.dtsx` |
| "Connection refused" (Memgraph) | Start Docker: `docker-compose up -d` |
| "Import error" | Reinstall dependencies: `uv sync` |
| "API key error" | Set environment variable: `export OPENAI_API_KEY=...` |
| Unicode errors on Windows | Normal - doesn't affect functionality |

## 🏗️ Architecture

- **Technology-Agnostic Core**: Canonical graph model works with any ETL
- **Plugin Architecture**: Easy to add new ETL platforms
- **Dual Backend**: Choose between speed (NetworkX) or persistence (Memgraph)
- **AI Integration**: Optional LLM enrichment for business insights
- **Full Traceability**: Every element traced to source file location

## 📝 License

MIT License - See LICENSE file

## 🤝 Contributing

Contributions welcome! The tool is designed for extensibility:
1. New ETL platforms: Extend `IngestionTool` base class
2. New analysis: Add to `CrossPackageAnalyzer`
3. New LLM providers: Implement `BaseLLMClient`

## 📧 Support

- Issues: [GitHub Issues](https://github.com/ai4data/metazensecode/issues)
- Documentation: See `/docs` folder
- Examples: See `/data/ssis` for sample projects

---

**Transform your ETL chaos into organized intelligence!** 🎯