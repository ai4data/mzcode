# MetaZenseCode

**Transform your SSIS packages into intelligent business insights**

MetaZenseCode automatically analyzes your SSIS projects and creates comprehensive dependency maps that power migration planning, compliance reporting, and business process optimization.

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

### 3. See Results
Your analysis creates a comprehensive report showing:
- 📊 All SSIS packages and their operations
- 🔗 Dependencies between packages
- ⚠️ Potential risks and bottlenecks
- 📋 Execution order recommendations

## ✨ What You Get

### Beautiful Analysis Report
```
🚀 Starting Complete SSIS Analysis
📁 Project path: /your/ssis/project

✅ Found 15 packages, extracted 67 operations
✅ Discovered 8 cross-package dependencies
✅ Identified 3 shared resources

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
- **Terminal report** - Immediate insights and recommendations

## 🎯 Perfect For

- **🔄 SSIS Migration Planning** - Map your current state before moving to cloud
- **📊 Impact Analysis** - Understand what changes when you modify a package
- **🚨 Risk Assessment** - Identify bottlenecks and failure points
- **📋 Documentation** - Auto-generate comprehensive SSIS documentation
- **🛡️ Compliance** - Track data flows for audit and regulatory requirements

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

### Analyze Your Own SSIS Project
```bash
# Point to your SSIS project folder
uv run python -m metazcode full --path "C:\YourSSISProjects\MyProject"

# Or just a single package
uv run python -m metazcode full --path "C:\MyPackage.dtsx"
```

## 🔧 Advanced Options

```bash
# Save analysis to custom file
uv run python -m metazcode full --path "your/project" --output "my_analysis.json"

# See detailed progress
uv run python -m metazcode full --path "your/project" --verbose

# Use database storage with custom connection
uv run python -m metazcode full --path "your/project" \
  --database memgraph --memgraph-host localhost --memgraph-port 7687
```

## ❓ FAQ

**Q: What SSIS files does it read?**  
A: .dtsx packages, .conmgr connections, .params parameters, .dtproj projects

**Q: Does it modify my SSIS files?**  
A: No! It only reads and analyzes - never changes your files

**Q: How long does analysis take?**  
A: Usually under 30 seconds for most projects

**Q: What if I have connection errors in my packages?**  
A: No problem! The tool analyzes file structure, not live connections

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

1. **Review the JSON output** - Contains complete metadata about your SSIS environment
2. **Check execution order** - Use for migration planning and optimization  
3. **Identify risks** - Address bottlenecks before they cause problems
4. **Share insights** - Use for team discussions and documentation

## 🤝 Contributing

MZCode is designed for extensibility. We welcome contributions that help organizations better understand and optimize their data infrastructure.

---

**Transform your SSIS environment from mystery to mastery!** 🎯