# MetaZenseCode

**Analyze your SSIS packages and understand how they work together**

MetaZenseCode reads your SQL Server Integration Services (SSIS) packages and creates a smart analysis showing you business logic, dependencies, and execution order - perfect for migration planning.

## ✨ What It Does

- **📊 Reads SSIS packages** - Automatically parses .dtsx files and extracts business logic
- **🔍 Finds dependencies** - Shows which packages must run before others
- **⚠️ Spots problems** - Identifies resource conflicts and bottlenecks  
- **📋 Creates roadmap** - Gives you execution order for coordinated migration

## 🚀 Quick Start

### Install

```bash
# Get the code
git clone <repository-url>
cd metazensecode

# Install with uv (fast Python package manager)
uv sync
```

### Run Analysis

**Option 1: Everything in one command (recommended)**
```bash
# Complete analysis: business logic + dependencies + search index
uv run python -m metazcode full --path "path/to/your/ssis/project"
```

**Option 2: Step by step**
```bash
# Step 1: Extract business logic
uv run python -m metazcode ingest --path "path/to/your/ssis/project"

# Step 2: Find dependencies
uv run python -m metazcode analyze --path "path/to/your/ssis/project"
```

### Try the Example

```bash
# Test with included sample
uv run python -m metazcode full --path "examples/sample_ssis_project"
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

## 🎯 Perfect For

### 🔄 **SSIS Migration**
- **Before**: "We have 50 SSIS packages, not sure how they're connected"
- **After**: "Package A must run before B, here's the 5-level execution plan"

### 🤖 **AI Migration Planning**  
- Feed rich metadata to AI tools for intelligent migration recommendations
- Preserve business logic across platform migrations

### 📋 **Documentation**
- Auto-generate system documentation
- Understand data lineage and transformations

## 📁 Commands

| Command | What It Does | When To Use |
|---------|-------------|-------------|
| `full` | **Everything** - Complete analysis | **First time users** |
| `complete` | Same as `full` (shorter name) | **Quick analysis** |
| `ingest` | Extract business logic only | **Basic parsing** |
| `analyze` | Add dependency analysis | **After ingest** |
| `dump` | Export results to JSON | **Save results** |

## 💡 Examples

### Enterprise Migration
```bash
# Analyze entire SSIS environment
uv run python -m metazcode full --path "C:\SSIS\Projects" --output "migration_plan.json"

# Results show:
# - 25 packages analyzed  
# - 12 shared databases identified
# - 6-level execution pipeline created
# - 3 high-risk connection bottlenecks found
```

### Quick Check
```bash
# Fast analysis of single project
uv run python -m metazcode complete --path "MyETL_Project"
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

```bash
# Install and test
git clone <repository-url>
cd metazensecode  
uv sync

# Analyze your SSIS project
uv run python -m metazcode full --path "your/ssis/project"

# Check the results!
# - enhanced_graph_full_analysis.json (complete metadata)
# - Your terminal shows execution plan and risks
```

**Transform your SSIS migration from guesswork to strategic planning!** 🎯