# MetaZenseCode

**Transform your ETL packages into intelligent business insights**

MetaZenseCode automatically analyzes your ETL projects and creates comprehensive dependency maps enhanced with AI-generated business summaries. Perfect for migration planning, compliance reporting, and business process optimization.

## 🌟 AI-Powered Business Intelligence

**NEW**: Transform technical ETL metadata into clear business insights with our optional LLM enrichment pipeline.

- 🤖 **AI Data Architect Analysis** - GPT-4o-mini generates business-focused summaries
- 📊 **Smart Operation Understanding** - "Aggregates daily sales by territory for revenue tracking"
- 🎯 **Migration-Ready Context** - Perfect input for AI migration tools and human planners
- ⚡ **Technology Agnostic** - Works with any ETL platform metadata
- 🔒 **Privacy First** - Only metadata sent to AI, never your actual data

## 🚀 Quick Start

### 1. Install
```bash
git clone <repository-url>
cd metazensecode
uv sync
```

### 2. Run Analysis
```bash
# Analyze your SSIS project (that's it!)
uv run python -m metazcode full --path "path/to/your/ssis/project"
```

### 3. Optional: Enable AI-Powered Insights

#### OpenAI (Default Provider)
```bash
# Add your OpenAI API key for intelligent business summaries
echo "OPENAI_API_KEY=your_api_key_here" >> .env

# Run analysis with AI enrichment (uses OpenAI by default)
uv run python -m metazcode full --path "path/to/your/project" --enable-llm
```

#### OpenRouter (Multi-Model Access)
```bash
# Add your OpenRouter API key (often more cost-effective)
echo "OPENROUTER_API_KEY=your_api_key_here" >> .env

# Use OpenRouter with DeepSeek (fast and cost-effective)
uv run python -m metazcode full --path "path/to/your/project" --enable-llm --provider openrouter --model "deepseek/deepseek-chat"

# Use OpenRouter with Claude (high quality)
uv run python -m metazcode full --path "path/to/your/project" --enable-llm --provider openrouter --model "anthropic/claude-3.5-sonnet"
```

### 4. See Results
Your analysis creates a comprehensive report showing:
- 📊 All ETL packages and their operations
- 🔗 Dependencies between packages
- ⚠️ Potential risks and bottlenecks
- 📋 Execution order recommendations
- 🤖 **AI-generated business summaries** (when LLM enrichment enabled)

## ✨ What You Get

### Beautiful Analysis Report
```
🚀 Starting Complete ETL Analysis
📁 Project path: /your/etl/project

✅ Found 15 packages, extracted 67 operations
✅ Discovered 8 cross-package dependencies
✅ Identified 3 shared resources
🤖 Generated 67 AI business summaries (LLM enrichment enabled)

📋 Execution Order:
   Level 1: CustomerETL, ProductETL (can run in parallel)
   Level 2: SalesETL (waits for Level 1)
   Level 3: ReportingETL (waits for Level 2)

⚠️  Potential Issues:
   🔌 MainDB_Connection: used by 4 packages (potential bottleneck)

🎉 Analysis Complete!
💾 Results saved to: enhanced_graph_full_analysis.json
```

### Rich Output Files
- **enhanced_graph_full_analysis.json** - Complete analysis with all metadata
- **AI-enriched summaries** - Business-focused operation descriptions (with LLM)
- **Terminal report** - Immediate insights and recommendations

### Sample AI-Enhanced Output
```json
{
  "operation_12345": {
    "name": "Load Customer Dimension",
    "type": "Data Flow Task",
    "llm_summary": "Loads customer master data into the data warehouse dimension table, supporting customer analytics and segmentation reporting.",
    "sources": ["CRM_Database.dbo.Customers"],
    "destinations": ["DataWarehouse.dbo.DimCustomer"]
  }
}
```

## 🎯 Perfect For

- **🔄 ETL Migration Planning** - Map your current state before moving to cloud
- **📊 Impact Analysis** - Understand what changes when you modify a package
- **🚨 Risk Assessment** - Identify bottlenecks and failure points
- **📋 Documentation** - Auto-generate comprehensive ETL documentation with AI insights
- **🛡️ Compliance** - Track data flows for audit and regulatory requirements
- **🤖 AI-Assisted Analysis** - Get business-focused summaries of complex operations

## 🗄️ Storage Options

### Option 1: Quick Analysis (Default)
```bash
# Fast in-memory analysis - perfect for most users
uv run python -m metazcode full --path "your/ssis/project"
```
**Best for:** Small to medium projects, one-time analysis, development

### Option 2: Enterprise Database Storage
```bash
# Start database
docker-compose up -d

# Run analysis with persistent storage
METAZCODE_DB_BACKEND=memgraph uv run python -m metazcode full --path "your/ssis/project"
```
**Best for:** Large enterprises, production use, multiple tools accessing the same data

**💡 When to use database storage:**
- You have 50+ SSIS packages
- Multiple people need to access the analysis
- You're building migration tools that need the data
- You want to query the results over time

## 📁 Try It Out

### Test with Sample Data
```bash
# Quick test with included sample
uv run python -m metazcode full --path data/ssis/dataWH_ssis

# Check the results
cat enhanced_graph_full_analysis.json
```

### Analyze Your Own ETL Project
```bash
# Point to your ETL project folder
uv run python -m metazcode full --path "C:\YourETLProjects\MyProject"

# With AI enrichment for business insights
uv run python -m metazcode full --path "C:\YourETLProjects\MyProject" --enable-llm

# Or just a single package
uv run python -m metazcode full --path "C:\MyPackage.dtsx"
```

## 🔧 Advanced Options

### Standard Analysis
```bash
# Save analysis to custom file
uv run python -m metazcode full --path "your/project" --output "my_analysis.json"

# See detailed progress
uv run python -m metazcode full --path "your/project" --verbose

# Use database storage with custom connection
uv run python -m metazcode full --path "your/project" \
  --database memgraph --memgraph-host localhost --memgraph-port 7687
```

### AI-Powered Analysis (Multi-Provider Support)

#### OpenAI Models
```bash
# Default: GPT-4o-mini (fast and cost-effective)
uv run python -m metazcode full --path "your/project" --enable-llm

# Explicit OpenAI with different models
uv run python -m metazcode full --path "your/project" --enable-llm --provider openai --model gpt-4o-mini
uv run python -m metazcode full --path "your/project" --enable-llm --provider openai --model gpt-4o
uv run python -m metazcode full --path "your/project" --enable-llm --provider openai --model gpt-4

# Using environment variables
export OPENAI_API_KEY=your_openai_key
export METAZCODE_LLM_PROVIDER=openai
export METAZCODE_LLM_MODEL=gpt-4o-mini
uv run python -m metazcode full --path "your/project" --enable-llm
```

#### OpenRouter (Access to Multiple Providers)
```bash
# DeepSeek (very cost-effective, high quality)
uv run python -m metazcode full --path "your/project" --enable-llm \
  --provider openrouter --model "deepseek/deepseek-chat"

# Anthropic Claude (excellent reasoning)
uv run python -m metazcode full --path "your/project" --enable-llm \
  --provider openrouter --model "anthropic/claude-3.5-sonnet"

# Meta Llama (open source)
uv run python -m metazcode full --path "your/project" --enable-llm \
  --provider openrouter --model "meta-llama/llama-3.1-8b-instruct"

# Google Gemini (competitive performance)
uv run python -m metazcode full --path "your/project" --enable-llm \
  --provider openrouter --model "google/gemini-pro-1.5"

# Using environment variables for OpenRouter
export OPENROUTER_API_KEY=your_openrouter_key
export OPENROUTER_SITE_URL=https://yoursite.com  # Optional: for OpenRouter rankings
export OPENROUTER_SITE_NAME="YourProject"        # Optional: for OpenRouter rankings
uv run python -m metazcode full --path "your/project" --enable-llm --provider openrouter
```

#### Advanced LLM Configuration
```bash
# Combine with database storage and custom output
uv run python -m metazcode full --path "your/project" \
  --enable-llm --provider openrouter --model "deepseek/deepseek-chat" \
  --output "ai_enhanced_analysis.json" --database memgraph

# Environment variables for fine-tuning
export METAZCODE_LLM_BATCH_SIZE=5      # Process 5 nodes at once (default: 10)
export METAZCODE_LLM_MAX_RETRIES=3     # Retry failed requests 3 times
export METAZCODE_LLM_TIMEOUT=30        # 30 second timeout per request
```

#### 💰 Cost & Performance Comparison

| Provider | Model | Cost* | Speed | Quality | Best For |
|----------|-------|-------|-------|---------|-----------|
| OpenAI | gpt-4o-mini | $$ | ⚡⚡⚡ | ⭐⭐⭐⭐ | Default choice, balanced |
| OpenRouter | deepseek/deepseek-chat | $ | ⚡⚡⚡ | ⭐⭐⭐⭐⭐ | Cost-effective, high quality |
| OpenRouter | anthropic/claude-3.5-sonnet | $$$$ | ⚡⚡ | ⭐⭐⭐⭐⭐ | Premium quality |
| OpenRouter | meta-llama/llama-3.1-8b | $ | ⚡⚡⚡ | ⭐⭐⭐⭐ | Open source, budget |

*Relative cost for ~100 operations: $ = <$0.50, $$ = $0.50-$2.00, $$$$ = $5.00+

#### 🎯 Model Recommendations

**For Development/Testing:**
```bash
# DeepSeek - Excellent quality at lowest cost
uv run python -m metazcode full --path "your/project" --enable-llm \
  --provider openrouter --model "deepseek/deepseek-chat"
```

**For Production:**
```bash
# OpenAI GPT-4o-mini - Reliable, well-tested
uv run python -m metazcode full --path "your/project" --enable-llm \
  --provider openai --model gpt-4o-mini
```

**For Premium Quality:**
```bash
# Claude 3.5 Sonnet - Best reasoning for complex ETL logic
uv run python -m metazcode full --path "your/project" --enable-llm \
  --provider openrouter --model "anthropic/claude-3.5-sonnet"
```

## ❓ FAQ

**Q: What ETL files does it read?**  
A: Currently supports SSIS: .dtsx packages, .conmgr connections, .params parameters, .dtproj projects

**Q: Does it modify my ETL files?**  
A: No! It only reads and analyzes - never changes your files

**Q: How long does analysis take?**  
A: Usually under 30 seconds for most projects, plus 1-2 minutes for AI enrichment

**Q: What if I have connection errors in my packages?**  
A: No problem! The tool analyzes file structure, not live connections

**Q: Do I need an OpenAI API key?**  
A: Only for AI enrichment features. Standard analysis works without any API keys

**Q: Is my data sent to AI services?**  
A: Only metadata (table names, operation names, SQL) - never actual data content

**Q: Can I use this for other ETL tools?**  
A: Currently SSIS only, but the architecture supports other platforms

## 🚨 Troubleshooting

**Problem: "No .dtsx files found"**
```bash
# Check your path has SSIS files
ls /path/to/your/ssis/project/*.dtsx
```

**Problem: "Connection refused" (when using database)**
```bash
# Make sure database is running
docker-compose up -d
docker ps | grep memgraph
```

**Problem: "Import errors"**
```bash
# Reinstall dependencies
uv sync
```

## 🎉 What's Next?

After running your analysis:

1. **Review the JSON output** - Contains complete metadata about your ETL environment
2. **Read AI business summaries** - Understand what each operation accomplishes (with LLM)
3. **Check execution order** - Use for migration planning and optimization  
4. **Identify risks** - Address bottlenecks before they cause problems
5. **Share insights** - Use AI-enhanced documentation for team discussions

## 🤝 Contributing

MZCode is designed for extensibility. We welcome contributions that help organizations better understand and optimize their data infrastructure.

---

**Transform your ETL environment from mystery to mastery with AI-powered insights!** 🎯