# MetaZenseCode

**Transform your SSIS packages into intelligent, actionable business insights**

MetaZenseCode extracts deep business logic from SQL Server Integration Services (SSIS) packages and creates comprehensive dependency graphs that power data-driven decisions across your organization. From migration planning to compliance automation, from cost optimization to business process discovery - MetaZenseCode turns your ETL code into strategic business intelligence.

## ✨ What It Does

- **📊 Extracts Business Logic** - Automatically parses .dtsx files and creates comprehensive metadata graphs
- **🔍 Maps Dependencies** - Visualizes complex data flows and package relationships
- **⚠️ Identifies Risks** - Spots bottlenecks, conflicts, and compliance issues
- **📋 Enables Intelligence** - Powers migration, optimization, and business process insights
- **🗄️ Dual Backend Support** - Choose between NetworkX (fast) or Memgraph (scalable) storage

## 🚀 Quick Start

### Install

```bash
# Get the code
git clone <repository-url>
cd metazensecode

# Install with uv (fast Python package manager)
uv sync

# For Memgraph database support (optional, for large-scale analysis)
uv sync --extra memgraph
```

### Database Setup

**Option 1: NetworkX (Default) - For small to medium projects**
```bash
# No additional setup needed - uses in-memory NetworkX graphs
```

**Option 2: Memgraph Database - For large-scale enterprise analysis**

1. **Start Memgraph Database**:
```bash
# Start Memgraph and Lab interface using Docker
docker-compose up -d

# This starts:
# - Memgraph database on port 7687
# - Memgraph Lab (web interface) on port 3000
```

2. **Configure Environment**:
```bash
# Copy and configure environment variables
cp .env.example .env

# Edit .env and set:
# METAZCODE_DB_BACKEND=memgraph
# MEMGRAPH_USERNAME=admin
# MEMGRAPH_PASSWORD=admin
```

3. **Test Connection**:
```bash
# Test if Memgraph is running
docker ps  # Should show memgraph containers

# Access Memgraph Lab in browser
open http://localhost:3000
```

### Run Analysis

**Option 1: Everything in one command (recommended)**
```bash
# NetworkX backend (default)
uv run python -m metazcode full --path "path/to/your/ssis/project"

# Memgraph backend for large-scale analysis
uv run python -m metazcode full --path "path/to/your/ssis/project" \
  --database memgraph --memgraph-username admin --memgraph-password admin
```

**Option 2: Step by step**
```bash
# Step 1: Extract business logic
uv run python -m metazcode ingest --path "path/to/your/ssis/project" --database memgraph

# Step 2: Find dependencies
uv run python -m metazcode analyze --path "path/to/your/ssis/project" --database memgraph
```

**Option 3: Using environment variables**
```bash
# Set environment variables
export METAZCODE_DB_BACKEND=memgraph
export MEMGRAPH_USERNAME=admin
export MEMGRAPH_PASSWORD=admin

# Run analysis
uv run python -m metazcode full --path "path/to/your/ssis/project"
```

### Try the Example

```bash
# Test with included sample data (NetworkX)
uv run python -m metazcode full --path data/ssis/dataWH_ssis

# Test with Memgraph backend
uv run python -m metazcode full --path data/ssis/dataWH_ssis \
  --database memgraph --memgraph-username admin --memgraph-password admin
```

### Verify Data in Memgraph

After running analysis with Memgraph, you can verify the data was stored:

1. **Check via Memgraph Lab**:
   - Open http://localhost:3000
   - Run query: `MATCH (n) RETURN count(n) as node_count`
   - Run query: `MATCH ()-[r]->() RETURN count(r) as edge_count`

2. **Check via Command Line**:
```bash
# Quick verification script
uv run python -c "
from metazcode.sdk.models.config import DatabaseConfig
from metazcode.sdk.graph.client_memgraph import MemgraphClient

config = DatabaseConfig(backend='memgraph', username='admin', password='admin')
client = MemgraphClient(config)
print(f'Nodes: {client.get_node_count()}')
print(f'Edges: {client.get_edge_count()}')
client.close()
"
```

## 📊 What You Get

### Beautiful Progress Report
```
🚀 Starting Complete SSIS Analysis
📁 Project path: /your/ssis/project

📊 Phase 1: Reading SSIS packages...
✅ Found 10 packages, extracted 48 operations

🕸️ Phase 2: Finding dependencies...
✅ Discovered 8 cross-package dependencies
📋 Execution Order:
   Level 1: CustomerETL, ProductETL (can run in parallel)
   Level 2: SalesETL (waits for Level 1)
   Level 3: ReportingETL (waits for Level 2)

⚠️ High-Risk Resources:
   🔌 MainDB_Connection: used by 4 packages (bottleneck!)

🎉 Analysis Complete!
💾 Results saved to: enhanced_graph_full_analysis.json
```

### Rich Metadata Files
- **Enhanced graph** - Complete business logic and dependencies
- **Analysis report** - Execution order and risk assessment
- **Search index** - Fast search through your SSIS metadata

## 🎯 Transformative Use Cases

### 🔄 **Platform Migration & Modernization**
- **SSIS to Cloud**: Accelerate migration to Azure Data Factory, AWS Glue, or Google Dataflow
- **Legacy Modernization**: Transform monolithic ETL to microservices architecture
- **Platform Independence**: Abstract business logic for vendor-agnostic implementations

### 🚨 **Risk Management & Compliance**
- **Data Breach Response**: Instantly map impact of compromised data sources
- **Regulatory Compliance**: Automated GDPR, SOX, HIPAA audit trail generation
- **Change Impact Analysis**: Predict downstream effects before making changes

### 💰 **Cost Optimization & Performance**
- **Resource Allocation**: Identify expensive, low-value data processing
- **Bottleneck Detection**: Find performance constraints in ETL pipelines
- **Cloud Cost Management**: Optimize resource usage and reduce operational costs

### 🔍 **Business Intelligence & Discovery**
- **Process Mining**: Reverse-engineer business processes from data flows
- **Data Lineage**: Understand complete data journey from source to report
- **Business Impact Analysis**: Connect technical components to business outcomes

### 🛡️ **Quality & Governance**
- **Data Quality Root Cause**: Trace quality issues back to their source
- **Governance Automation**: Self-maintaining data catalogs and documentation
- **Testing Strategy**: Auto-generate test cases from ETL metadata

### 🤖 **AI & Machine Learning Enhancement**
- **Predictive Analytics**: Forecast ETL failures and business disruptions
- **Anomaly Detection**: Identify unusual patterns in data flows
- **Intelligent Recommendations**: AI-powered optimization suggestions

## 🗄️ Database Backends

MetaZenseCode supports dual backend architecture for maximum flexibility:

### NetworkX (Default)
- **Best for**: Small to medium SSIS projects (< 50 packages), prototyping, algorithm-heavy analysis
- **Pros**: Fast startup, no setup required, rich graph algorithms (400+), excellent for development
- **Cons**: In-memory only, limited scalability, no persistence

### Memgraph Database
- **Best for**: Large enterprise SSIS environments (50+ packages), production deployments, persistent storage
- **Pros**: Persistent storage, concurrent access, scalable, real-time querying, web-based interface
- **Cons**: Requires Docker setup, network overhead for small operations

### Seamless Backend Switching

```bash
# NetworkX backend (default)
uv run python -m metazcode full --path "your/ssis/project"

# Memgraph backend via CLI flags
uv run python -m metazcode full --database memgraph \
  --memgraph-username admin --memgraph-password admin \
  --path "your/ssis/project"

# Memgraph backend via environment variables
export METAZCODE_DB_BACKEND=memgraph
export MEMGRAPH_USERNAME=admin
export MEMGRAPH_PASSWORD=admin
uv run python -m metazcode full --path "your/ssis/project"
```

### Backend Selection Guide

| Use Case | NetworkX | Memgraph | Reason |
|----------|----------|----------|---------|
| **Development & Prototyping** | ✅ | ❌ | Fast iteration, no setup |
| **Algorithm-heavy Analysis** | ✅ | ❌ | Rich algorithm library |
| **Production Deployment** | ❌ | ✅ | Persistence, reliability |
| **Large Enterprise (>100K nodes)** | ❌ | ✅ | Scalability, performance |
| **Multi-user Environment** | ❌ | ✅ | Concurrent access |
| **Real-time Querying** | ❌ | ✅ | Live data exploration |

## 📁 Commands & Database Options

### Core Commands

| Command | What It Does | When To Use |
|---------|-------------|-------------|
| `full` | **Everything** - Complete analysis | **First time users** |
| `complete` | Same as `full` (shorter name) | **Quick analysis** |
| `ingest` | Extract business logic only | **Basic parsing** |
| `analyze` | Add dependency analysis | **After ingest** |
| `dump` | Export results to JSON | **Save results** |

### Database Backend Options

All commands support these database options:

| Flag | Description | Example |
|------|-------------|---------|
| `--database` | Backend choice (networkx/memgraph) | `--database memgraph` |
| `--memgraph-host` | Memgraph server host | `--memgraph-host localhost` |
| `--memgraph-port` | Memgraph server port | `--memgraph-port 7687` |
| `--memgraph-username` | Memgraph username | `--memgraph-username admin` |
| `--memgraph-password` | Memgraph password | `--memgraph-password admin` |

### Command Examples

```bash
# Basic analysis with NetworkX
uv run python -m metazcode full --path "your/ssis/project"

# Enterprise analysis with Memgraph
uv run python -m metazcode full --path "your/ssis/project" \
  --database memgraph --memgraph-username admin --memgraph-password admin

# Step-by-step with different backends
uv run python -m metazcode ingest --path "your/ssis/project" --database networkx
uv run python -m metazcode analyze --path "your/ssis/project" --database memgraph
```

## 💡 Real-World Examples

### Enterprise Migration Planning
```bash
# Analyze entire SSIS environment with Memgraph for persistence
uv run python -m metazcode full --path "C:\SSIS\Projects" \
  --database memgraph --memgraph-username admin --memgraph-password admin \
  --output "migration_plan.json"

# Results show:
# - 150+ packages analyzed across 12 projects
# - 45 shared databases and connections identified
# - 8-level execution pipeline with parallel opportunities
# - 15 high-risk bottlenecks requiring immediate attention
```

### Compliance Audit Preparation
```bash
# Generate comprehensive audit trail for compliance
uv run python -m metazcode full --path "Production_ETL" \
  --database memgraph --output "compliance_audit.json"

# Use Memgraph Lab to query specific compliance questions:
# - Which packages handle PII data?
# - What's the complete lineage of customer data?
# - Are there any unencrypted data flows?
```

### Cost Optimization Analysis
```bash
# Identify expensive, low-value processing
uv run python -m metazcode analyze --path "CloudETL_Environment" \
  --database memgraph --verbose

# Then query Memgraph for optimization insights:
# - Which transformations consume the most resources?
# - What data flows can be consolidated?
# - Are there duplicate processing patterns?
```

### Business Process Discovery
```bash
# Reverse-engineer business processes from ETL flows
uv run python -m metazcode full --path "BusinessETL" \
  --database memgraph --project-id "OrderToCash"

# Explore in Memgraph Lab:
# - Map the complete Order-to-Cash process
# - Identify process bottlenecks and delays
# - Understand data dependencies across business units
```

### Quick Development Check
```bash
# Fast analysis for development (NetworkX)
uv run python -m metazcode complete --path "MyETL_Project"

# Perfect for:
# - Quick dependency checking
# - Development validation
# - Local testing
```

## 🔧 Options

### Basic Options
- `--path` - Your SSIS project folder
- `--output` - Save detailed analysis to file
- `--verbose` - See detailed progress

### Advanced Options  
- `--index-output` - Save search index
- `--project-id` - Custom project name

## ❓ FAQ

**Q: What SSIS files does it read?**  
A: .dtsx packages, .conmgr connections, .params parameters, .dtproj projects

**Q: Does it modify my SSIS files?**  
A: No! It only reads and analyzes - never changes your files

**Q: How long does analysis take?**  
A: Typically under 30 seconds for most projects (tested on 50+ package environments)

**Q: What if I have connection errors?**  
A: The tool works offline - it analyzes file structure, not live connections

## 🚀 Ready to Start?

### Quick Start (NetworkX)
```bash
# Install and test with NetworkX backend
git clone <repository-url>
cd metazensecode  
uv sync

# Analyze your SSIS project
uv run python -m metazcode full --path "your/ssis/project"

# Check the results!
# - enhanced_graph_full_analysis.json (complete metadata)
# - Your terminal shows execution plan and risks
```

### Enterprise Setup (Memgraph)
```bash
# Install with Memgraph support
git clone <repository-url>
cd metazensecode  
uv sync --extra memgraph

# Start Memgraph database
docker-compose up -d

# Configure environment
cp .env.example .env
# Edit .env: set METAZCODE_DB_BACKEND=memgraph, MEMGRAPH_USERNAME=admin, MEMGRAPH_PASSWORD=admin

# Run enterprise analysis
uv run python -m metazcode full --path "your/ssis/project" --database memgraph

# Explore results in Memgraph Lab
open http://localhost:3000
```

### Next Steps

1. **Explore the Documentation**: Check out `docs/` for detailed use cases and backend comparisons
2. **Try Different Backends**: Compare NetworkX vs Memgraph for your use case
3. **Integrate with Your Workflow**: Use the JSON output for migration planning, compliance, or optimization
4. **Scale Up**: Use Memgraph for production environments with large SSIS deployments

**Transform your SSIS environment from mystery to mastery!** 🎯

---

## 📚 Documentation

- **[Backend Comparison](docs/backend-comparison.md)** - Detailed comparison of NetworkX vs Memgraph
- **[Concrete Use Cases](docs/concrete-use-cases.md)** - Real-world applications and business value
- **[CLAUDE.md](CLAUDE.md)** - Developer guide and architecture overview

## 🤝 Contributing

MetaZenseCode is designed for extensibility. Whether you're adding new ETL platform support, enhancing analysis capabilities, or integrating with other tools, we welcome contributions that help organizations better understand and optimize their data infrastructure.