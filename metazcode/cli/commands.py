import click
import os
from pathlib import Path
from typing import Optional
import json
import networkx as nx
import logging

from metazcode.sdk.graph.graph_constructor import GraphClientBuilder
from metazcode.cli.orchestrator import Orchestrator
from metazcode.sdk.models.canonical_types import NodeType
from metazcode.sdk.integration.index_integration import IndexIntegration
from metazcode.sdk.models.config import DatabaseConfig, MetaZenseConfig
from metazcode.sdk.enrichment import EnrichmentPipeline


def database_option(f):
    """Decorator to add database configuration options to CLI commands."""
    f = click.option(
        "--database",
        type=click.Choice(["networkx", "memgraph"]),
        default=None,
        help="Graph database backend to use (overrides environment variable)",
    )(f)
    f = click.option(
        "--memgraph-host",
        default=None,
        help="Memgraph server host (default: localhost)",
    )(f)
    f = click.option(
        "--memgraph-port",
        type=int,
        default=None,
        help="Memgraph server port (default: 7687)",
    )(f)
    f = click.option(
        "--memgraph-username",
        default=None,
        help="Memgraph username (default: from environment)",
    )(f)
    f = click.option(
        "--memgraph-password",
        default=None,
        help="Memgraph password (default: from environment)",
    )(f)
    return f


def get_database_config(
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
) -> DatabaseConfig:
    """Create database configuration from CLI options and environment."""
    config = DatabaseConfig.from_environment()

    # Override with CLI options if provided
    if database:
        config.backend = database
    if memgraph_host:
        config.host = memgraph_host
    if memgraph_port:
        config.port = memgraph_port
    if memgraph_username:
        config.username = memgraph_username
    if memgraph_password:
        config.password = memgraph_password

    return config


@click.group()
def cli():
    """A command-line tool for Metazcode."""
    pass


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory or a specific file to ingest.",
)
@database_option
def ingest(
    path: str,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Discovers and runs all available ingestion tools on the specified path.
    Can target a whole directory or a single supported file.
    """
    click.echo(f"Starting ingestion for: {os.path.abspath(path)}")

    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(
            f"Error: The path '{path}' does not exist or is not a valid file or directory.",
            err=True,
        )
        return

    # 1. Build the graph client with configuration
    db_config = get_database_config(
        database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
    )

    # Test connection and show status
    if not GraphClientBuilder.validate_connection(db_config):
        click.echo(
            f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
            err=True,
        )

    graph_client = GraphClientBuilder.get_client(db_config)

    # 2. Initialize and run the orchestrator
    orchestrator = Orchestrator(
        graph_client=graph_client, root_path=root_path, target_file=target_file
    )
    orchestrator.run()

    click.echo("[SUCCESS] Ingestion finished successfully.")


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory to ingest for visualization.",
)
@click.option(
    "--output-path", default="graph.png", help="Path to save the visualization."
)
@database_option
def visualize(
    path: str,
    output_path: str,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Generates a visual representation of the graph.
    NOTE: This is a basic visualization and may be unreadable for large graphs.
    """
    click.echo(
        f"Generating graph visualization for '{path}' and saving to '{output_path}'..."
    )

    # Build the graph client with configuration
    db_config = get_database_config(
        database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
    )

    # Test connection and show status
    if not GraphClientBuilder.validate_connection(db_config):
        click.echo(
            f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
            err=True,
        )

    graph_client = GraphClientBuilder.get_client(db_config)

    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(
            f"Error: The path '{path}' does not exist or is not a valid file or directory.",
            err=True,
        )
        return

    orchestrator = Orchestrator(
        graph_client=graph_client, root_path=root_path, target_file=target_file
    )
    orchestrator.run()  # This will populate the in-memory graph

    try:
        from metazcode.sdk.graph.visualizer import visualize_graph

        graph = graph_client.get_graph()
        visualize_graph(graph, output_path)
        click.echo(f"[SUCCESS] Visualization saved to {output_path}")
    except ImportError:
        click.echo(
            "Please install matplotlib and pygraphviz to use this command.", err=True
        )
    except Exception as e:
        click.echo(f"Error during visualization: {e}", err=True)


@cli.command()
@click.option(
    "--path", default=".", help="The path to the project directory to ingest."
)
@click.option(
    "--output",
    default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Path to save the output JSON file.",
)
@database_option
def dump(
    path: str,
    output: Optional[str],
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Ingests the project and prints the nodes and edges to the console or saves to a JSON file.
    """
    click.echo("==================== Starting Ingestion ====================")

    # Build the graph client with configuration
    db_config = get_database_config(
        database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
    )

    # Test connection and show status
    if not GraphClientBuilder.validate_connection(db_config):
        click.echo(
            f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
            err=True,
        )

    graph_client = GraphClientBuilder.get_client(db_config)

    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(f"Error: Path '{path}' not found.", err=True)
        return

    orchestrator = Orchestrator(
        graph_client=graph_client, root_path=root_path, target_file=target_file
    )
    orchestrator.run()
    click.echo("====================== Ingestion Complete ======================")
    click.echo("")

    graph = graph_client.get_graph()

    if output:
        # Serialize and write to JSON file
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert graph to a JSON-serializable format
        graph_data = nx.node_link_data(graph, edges="links")

        # Custom JSON serialization for complex types if needed
        def default_serializer(o):
            if isinstance(o, (set, tuple)):
                return list(o)
            if hasattr(o, "to_dict"):
                return o.to_dict()
            try:
                return str(o)  # Fallback to string representation
            except TypeError:
                return repr(o)

        with open(output_path, "w") as f:
            json.dump(graph_data, f, indent=2, default=default_serializer)

        click.echo(f"[SUCCESS] Graph data saved to {output_path.resolve()}")
    else:
        # Print to console (existing behavior)
        click.echo("--- NODES ---")
        for node_id, data in graph.nodes(data=True):
            click.echo(f"ID: {node_id}")
            for key, value in data.items():
                click.echo(f"  {key}: {value}")
            click.echo("----------")

        click.echo("\n--- EDGES ---")
        for source, target, data in graph.edges(data=True):
            click.echo(f"FROM: {source}")
            click.echo(f"  TO: {target}")
            click.echo(f"  RELATION: {data.get('relation', 'N/A')}")
            click.echo("----------")


@cli.command()
@click.option(
    "--path", default=".", help="The path to the project directory to analyze."
)
@click.option(
    "--output",
    default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Path to save the cross-package analysis results JSON file.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging to see detailed progress.",
)
@database_option
def analyze(
    path: str,
    output: Optional[str],
    verbose: bool,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Perform cross-package dependency analysis on an existing graph.

    This command:
    1. Loads the existing graph from the specified path
    2. Identifies shared resources (tables, connections, parameters)
    3. Maps data flow dependencies between packages
    4. Determines execution order requirements
    5. Adds cross-package edges to the graph
    6. Updates package properties with dependency information

    Example: metazcode analyze --path ./data/ssis/project --output analysis.json
    """
    # Setup logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    click.echo(
        f"[START] Starting cross-package dependency analysis for: {os.path.abspath(path)}"
    )

    # Validate path
    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(f"[ERROR] Error: Path '{path}' not found.", err=True)
        return

    try:
        # 1. Build the graph client with configuration
        db_config = get_database_config(
            database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
        )

        # Test connection and show status
        if not GraphClientBuilder.validate_connection(db_config):
            click.echo(
                f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
                err=True,
            )

        graph_client = GraphClientBuilder.get_client(db_config)

        # 2. Load the graph by running ingestion first
        click.echo("[INFO] Loading existing graph...")
        orchestrator = Orchestrator(
            graph_client=graph_client, root_path=root_path, target_file=target_file
        )
        orchestrator.run()
        click.echo("[INFO] Graph loaded successfully")

        # 3. Initialize cross-package analyzer
        from metazcode.sdk.analysis.cross_package_analyzer import CrossPackageAnalyzer

        analyzer = CrossPackageAnalyzer(graph_client)

        # 4. Perform the analysis
        click.echo("[ANALYZE] Performing cross-package dependency analysis...")
        analysis_results = analyzer.analyze()

        # 5. Display results
        click.echo(f"[SUCCESS] Cross-package analysis completed!")
        click.echo(f"   Packages analyzed: {analysis_results['packages_analyzed']}")
        click.echo(
            f"   Shared tables: {analysis_results['shared_resources']['tables']}"
        )
        click.echo(
            f"   Shared connections: {analysis_results['shared_resources']['connections']}"
        )
        click.echo(f"   Data dependencies: {analysis_results['data_dependencies']}")
        click.echo(
            f"   Cross-package edges added: {analysis_results['cross_package_edges_added']}"
        )

        # Show execution chains
        execution_order = analysis_results["detailed_analysis"]["execution_order"]
        if len(execution_order) > 1:
            click.echo(f"\n[INFO] Execution order (sequential levels):")
            for level, packages in enumerate(execution_order, 1):
                package_names = [
                    pkg.split(":")[1] if ":" in pkg else pkg for pkg in packages
                ]
                click.echo(f"   Level {level}: {', '.join(package_names)}")
        else:
            click.echo(
                f"\n[INFO] All packages can execute in parallel (no dependencies)"
            )

        # Show high-risk resources
        contention_risks = analysis_results["contention_risks"]
        if contention_risks["high_risk_connections"]:
            click.echo(f"\n[WARNING] High-risk connections (resource contention):")
            for risk in contention_risks["high_risk_connections"]:
                click.echo(f"   {risk['connection']}: {risk['package_count']} packages")

        # 6. Save analysis results if output specified
        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w") as f:
                json.dump(analysis_results, f, indent=2, default=str)

            click.echo(f"[SUCCESS] Analysis results saved to {output_path.resolve()}")

        # 7. Optionally save enhanced graph
        enhanced_graph_path = Path("enhanced_graph_with_dependencies.json")
        graph = graph_client.get_graph()
        graph_data = nx.node_link_data(graph, edges="links")

        def default_serializer(o):
            if isinstance(o, (set, tuple)):
                return list(o)
            if hasattr(o, "to_dict"):
                return o.to_dict()
            try:
                return str(o)
            except TypeError:
                return repr(o)

        with open(enhanced_graph_path, "w") as f:
            json.dump(graph_data, f, indent=2, default=default_serializer)

        click.echo(
            f"[SUCCESS] Enhanced graph with cross-package dependencies saved to {enhanced_graph_path.resolve()}"
        )

    except Exception as e:
        click.echo(f"[ERROR] Error during cross-package analysis: {e}", err=True)
        if verbose:
            import traceback

            click.echo(traceback.format_exc(), err=True)


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory or a specific file to ingest and index.",
)
@click.option(
    "--index-output",
    default=None,
    help="Path to save the index file (optional).",
)
@click.option(
    "--project-id",
    default=None,
    help="Project identifier for the index (optional).",
)
@database_option
def ingest_n_index(
    path: str,
    index_output: Optional[str],
    project_id: Optional[str],
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Run both ingestion and indexing together.

    This command performs the complete workflow of data ingestion followed by
    enhanced SSIS indexing for fast search and analysis capabilities.
    """
    click.echo(
        f"Starting integrated ingestion and indexing for: {os.path.abspath(path)}"
    )

    try:
        # Initialize integration component
        integration = IndexIntegration()

        # Run integrated workflow
        results = integration.ingest_and_index(
            path=path, index_output=index_output, project_id=project_id
        )

        # Display results
        ingestion_stats = results["ingestion_results"]
        index_stats = results["index_results"]

        click.echo(f"\n[SUCCESS] Integrated workflow completed successfully!")
        click.echo(f"")
        click.echo(f"Ingestion Results:")
        click.echo(f"  - Nodes: {ingestion_stats['node_count']}")
        click.echo(f"  - Edges: {ingestion_stats['edge_count']}")
        click.echo(f"")
        click.echo(f"Index Results:")
        click.echo(f"  - Indexed nodes: {index_stats['node_count']}")
        click.echo(f"  - Unique names: {index_stats['unique_names']}")
        click.echo(f"  - BM25 metadata ready: {index_stats['bm25_metadata_ready']}")
        click.echo(f"  - BM25 content ready: {index_stats['bm25_content_ready']}")

        # Show SSIS enhancements if available
        if "ssis_enhancements" in index_stats:
            ssis_stats = index_stats["ssis_enhancements"]
            click.echo(f"")
            click.echo(f"SSIS Business Logic Indexing:")
            click.echo(
                f"  - SQL operations indexed: {ssis_stats['sql_operations_indexed']}"
            )
            click.echo(
                f"  - Shared tables indexed: {ssis_stats['shared_tables_indexed']}"
            )
            click.echo(
                f"  - Parameterized connections indexed: {ssis_stats['parameterized_connections_indexed']}"
            )
            click.echo(
                f"  - Cross-package pipelines indexed: {ssis_stats['cross_package_pipelines_indexed']}"
            )

        # Show file outputs
        files_created = results["files_created"]
        if files_created["index_file"]:
            click.echo(f"")
            click.echo(f"Files Created:")
            click.echo(f"  - Index file: {files_created['index_file']}")
            click.echo(f"  - Metadata file: {files_created['metadata_file']}")

        click.echo(f"")
        click.echo(
            f"[SUCCESS] Enhanced SSIS indexing complete. Graph is now searchable!"
        )

    except Exception as e:
        click.echo(
            f"[ERROR] Error during integrated ingestion and indexing: {e}", err=True
        )
        import traceback

        click.echo(traceback.format_exc(), err=True)


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory or a specific file to process.",
)
@click.option(
    "--output",
    default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Path to save the analysis results JSON file.",
)
@click.option(
    "--index-output",
    default=None,
    help="Path to save the index file (optional).",
)
@click.option(
    "--project-id",
    default=None,
    help="Project identifier for the index (optional).",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging to see detailed progress.",
)
@click.option(
    "--enable-llm",
    is_flag=True,
    help="Enable LLM enrichment of nodes with business summaries.",
)
@database_option
def full(
    path: str,
    output: Optional[str],
    index_output: Optional[str],
    project_id: Optional[str],
    verbose: bool,
    enable_llm: bool,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Complete SSIS analysis: ingest + analyze + index in one command.

    This command performs the full MetaZenseCode workflow:
    1. Ingests SSIS packages and extracts business logic
    2. Performs cross-package dependency analysis
    3. Creates enhanced searchable index
    4. Exports rich graph with enterprise intelligence

    Perfect for users who want complete SSIS analysis without running separate commands.

    Example: metazcode full --path ./data/ssis/project --output analysis.json
    """
    # Setup logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    click.echo("Starting Complete SSIS Analysis (Ingest + Analyze + Index)")
    click.echo("=" * 70)
    click.echo(f"Project path: {os.path.abspath(path)}")

    # Validate path
    p = Path(path)
    root_path: str
    target_file: Optional[str] = None

    if p.is_dir():
        root_path = str(p.resolve())
    elif p.is_file():
        root_path = str(p.parent.resolve())
        target_file = str(p.resolve())
    else:
        click.echo(f"[ERROR] Path '{path}' not found.", err=True)
        return

    try:
        # Phase 1: Ingestion
        click.echo("")
        click.echo("Phase 1: SSIS Ingestion (Building Graph)")
        click.echo("-" * 50)

        # Build the graph client with configuration
        db_config = get_database_config(
            database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
        )

        # Test connection and show status
        if not GraphClientBuilder.validate_connection(db_config):
            click.echo(
                f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
                err=True,
            )

        graph_client = GraphClientBuilder.get_client(db_config)
        orchestrator = Orchestrator(
            graph_client=graph_client, root_path=root_path, target_file=target_file
        )
        orchestrator.run()

        initial_nodes = graph_client.get_node_count()
        initial_edges = graph_client.get_edge_count()
        click.echo(f"Ingestion complete: {initial_nodes} nodes, {initial_edges} edges")

        # Phase 2: Cross-Package Analysis
        click.echo("")
        click.echo("Phase 2: Cross-Package Analysis (Enterprise Intelligence)")
        click.echo("-" * 50)

        from metazcode.sdk.analysis.cross_package_analyzer import CrossPackageAnalyzer

        analyzer = CrossPackageAnalyzer(graph_client)
        analysis_results = analyzer.analyze()

        final_nodes = graph_client.get_node_count()
        final_edges = graph_client.get_edge_count()
        new_edges = final_edges - initial_edges

        click.echo(f"Analysis complete:")
        click.echo(f"   Packages analyzed: {analysis_results['packages_analyzed']}")
        click.echo(f"   Cross-package edges added: {new_edges}")
        click.echo(
            f"   Shared tables: {analysis_results['shared_resources']['tables']}"
        )
        click.echo(
            f"   Shared connections: {analysis_results['shared_resources']['connections']}"
        )

        # Show execution order
        execution_order = analysis_results["detailed_analysis"]["execution_order"]
        if len(execution_order) > 1:
            click.echo(f"")
            click.echo(f"Execution Order:")
            for level, packages in enumerate(execution_order, 1):
                package_names = [
                    pkg.split(":")[1] if ":" in pkg else pkg for pkg in packages
                ]
                click.echo(f"   Level {level}: {', '.join(package_names)}")
        else:
            click.echo(f"All packages can execute in parallel")

        # Show warnings
        contention_risks = analysis_results["contention_risks"]
        if contention_risks["high_risk_connections"]:
            click.echo(f"")
            click.echo(f"High-Risk Resources:")
            for risk in contention_risks["high_risk_connections"]:
                click.echo(f"   {risk['connection']}: {risk['package_count']} packages")

        # Phase 3: LLM Enrichment (Optional)
        if enable_llm or os.getenv("METAZCODE_ENABLE_LLM_ENRICHMENT", "false").lower() == "true":
            click.echo("")
            click.echo("Phase 3: LLM Enrichment (AI-Generated Summaries)")
            click.echo("-" * 50)

            # Create configuration with LLM settings
            config = MetaZenseConfig.from_environment()
            config.enable_llm_enrichment = True
            
            # Initialize and run enrichment pipeline
            enrichment_pipeline = EnrichmentPipeline(graph_client)
            
            # Validate configuration before running
            validation = enrichment_pipeline.validate_configuration()
            if not validation["ready"]:
                click.echo("LLM Enrichment configuration issues:", err=True)
                if not validation["api_key_present"]:
                    click.echo(f"   ❌ No API key found: set OPENAI_API_KEY environment variable", err=True)
                if not validation["api_key_format"]:
                    click.echo(f"   ❌ Invalid API key format", err=True)
                if not validation["model_supported"]:
                    click.echo(f"   ❌ Unsupported model", err=True)
                if not validation["graph_client_connected"]:
                    click.echo(f"   ❌ Graph client not connected", err=True)
                click.echo("Skipping LLM enrichment phase")
            else:
                # Run enrichment
                enrichment_results = enrichment_pipeline.enrich_graph()
                
                if enrichment_results["status"] == "completed":
                    summary = enrichment_results["summary"]
                    click.echo(f"LLM Enrichment complete:")
                    click.echo(f"   Nodes processed: {summary['total_nodes']}")
                    click.echo(f"   Successfully enriched: {summary['successfully_enriched']}")
                    click.echo(f"   Failed: {summary['failed']}")
                    click.echo(f"   Success rate: {summary['success_rate']:.1f}%")
                    
                    if "token_usage" in enrichment_results:
                        tokens = enrichment_results["token_usage"]
                        if tokens.get("total_tokens", 0) > 0:
                            click.echo(f"   Tokens used: {tokens['total_tokens']}")
                else:
                    click.echo(f"LLM Enrichment failed: {enrichment_results.get('reason', 'Unknown error')}")

        # Phase 4: Enhanced Indexing
        click.echo("")
        phase_num = "Phase 4" if enable_llm or os.getenv("METAZCODE_ENABLE_LLM_ENRICHMENT", "false").lower() == "true" else "Phase 3"
        click.echo(f"{phase_num}: Enhanced Indexing (Search Capabilities)")
        click.echo("-" * 50)

        integration = IndexIntegration()

        # Run indexing on the already enhanced graph
        index = integration._build_enhanced_index(graph_client, project_id)

        # Save index if requested
        index_path = None
        metadata_path = None
        if index_output:
            index_path, metadata_path = integration._save_index(index, index_output)

        # Generate index results summary
        integration_results = integration._generate_results_summary(
            graph_client, index, index_path, metadata_path
        )
        index_results = integration_results["index_results"]

        click.echo(f"Indexing complete:")
        click.echo(f"   Indexed nodes: {index_results['node_count']}")
        click.echo(f"   Unique names: {index_results['unique_names']}")
        click.echo(f"   BM25 metadata ready: {index_results['bm25_metadata_ready']}")
        click.echo(f"   BM25 content ready: {index_results['bm25_content_ready']}")

        # Check if SSIS enhanced index has additional stats
        if hasattr(index, "get_ssis_enhancement_stats"):
            ssis_stats = index.get_ssis_enhancement_stats()
            if ssis_stats:
                click.echo(f"")
                click.echo(f"SSIS Business Logic Indexing:")
                click.echo(
                    f"   SQL operations: {ssis_stats.get('sql_operations_indexed', 0)}"
                )
                click.echo(
                    f"   Shared tables: {ssis_stats.get('shared_tables_indexed', 0)}"
                )
                click.echo(
                    f"   Parameterized connections: {ssis_stats.get('parameterized_connections_indexed', 0)}"
                )
                click.echo(
                    f"   Cross-package pipelines: {ssis_stats.get('cross_package_pipelines_indexed', 0)}"
                )

        # Final Phase: Analytics Preparation (Application Readiness)
        if db_config.backend == "memgraph":
            click.echo("")
            final_phase_num = "Phase 5" if enable_llm or os.getenv("METAZCODE_ENABLE_LLM_ENRICHMENT", "false").lower() == "true" else "Phase 4"
            click.echo(f"{final_phase_num}: Analytics Preparation (Application Readiness)")
            click.echo("-" * 50)
            
            # Trigger analytics preparation for downstream applications
            try:
                from metazcode.sdk.graph.analytics_ready_client import AnalyticsReadyMemgraphClient
                if isinstance(graph_client, AnalyticsReadyMemgraphClient):
                    graph_client.prepare_for_applications()
                    
                    click.echo("Analytics preparation complete:")
                    click.echo("   ✅ Performance indexes created")
                    click.echo("   ✅ Materialized views for applications created")
                    click.echo("   ✅ Graph metadata for readiness verification added")
                    click.echo("   ✅ Ready for downstream applications:")
                    click.echo("      • Migration planning tools")
                    click.echo("      • Compliance analysis tools") 
                    click.echo("      • Governance reporting tools")
                    click.echo("      • Data lineage tracking tools")
                else:
                    click.echo("Analytics preparation skipped (client type not supported)")
            except Exception as e:
                click.echo(f"Warning: Analytics preparation failed: {e}")

        # Save outputs
        click.echo("")
        click.echo("Saving Results")
        click.echo("-" * 50)

        # Save analysis results if requested
        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(analysis_results, f, indent=2, default=str)
            click.echo(f"Analysis results: {output_path.resolve()}")

        # Always save enhanced graph
        enhanced_graph_path = Path("enhanced_graph_full_analysis.json")
        
        # Handle different graph client types
        try:
            graph = graph_client.get_graph()
            # Check if it's a NetworkX graph
            if hasattr(graph, 'is_multigraph'):
                graph_data = nx.node_link_data(graph, edges="links")
            else:
                # For Memgraph, create a custom export
                all_nodes = graph_client.get_all_nodes()
                nodes_data = [node.to_dict() for node in all_nodes]
                
                # Get edges via custom query
                connection = graph_client.get_graph()
                cursor = connection.cursor()
                cursor.execute("""
                    MATCH (source)-[r]->(target)
                    RETURN source.id as source_id, target.id as target_id, 
                           type(r) as relation_type, properties(r) as properties
                """)
                edge_results = cursor.fetchall()
                
                edges_data = []
                for source_id, target_id, relation_type, properties in edge_results:
                    edge_dict = {
                        "source": source_id,
                        "target": target_id,
                        "relation": relation_type,
                        "properties": properties if properties else {}
                    }
                    edges_data.append(edge_dict)
                
                graph_data = {
                    "nodes": nodes_data,
                    "links": edges_data,
                    "metadata": {
                        "node_count": len(nodes_data),
                        "edge_count": len(edges_data),
                        "graph_type": "memgraph"
                    }
                }
        except Exception as e:
            click.echo(f"Warning: Could not export graph data: {e}")
            graph_data = {"error": str(e), "nodes": [], "links": []}

        def default_serializer(o):
            if isinstance(o, (set, tuple)):
                return list(o)
            if hasattr(o, "to_dict"):
                return o.to_dict()
            try:
                return str(o)
            except TypeError:
                return repr(o)

        with open(enhanced_graph_path, "w") as f:
            json.dump(graph_data, f, indent=2, default=default_serializer)

        click.echo(f"Enhanced graph: {enhanced_graph_path.resolve()}")

        # Show index files if created
        if index_path:
            click.echo(f"Index file: {index_path}")
        if metadata_path:
            click.echo(f"Metadata file: {metadata_path}")

        # Final summary
        click.echo("")
        click.echo("COMPLETE ANALYSIS FINISHED!")
        click.echo("=" * 70)
        click.echo(
            f"Graph: {final_nodes} nodes, {final_edges} edges ({new_edges} cross-package)"
        )
        click.echo(f"Intelligence: Business logic + Dependencies + Search ready")
        click.echo(f"Ready for: Migration planning, AI agents, Enterprise coordination")
        click.echo("")
        click.echo("Next steps:")
        click.echo("  Review analysis results in the generated JSON files")
        click.echo("  Use the enhanced graph for migration planning")
        click.echo("  Feed the rich metadata to AI migration agents")

    except Exception as e:
        click.echo(f"[ERROR] Error during complete analysis: {e}", err=True)
        if verbose:
            import traceback

            click.echo(traceback.format_exc(), err=True)


@cli.command()
@click.option(
    "--path",
    default=".",
    help="The path to the project directory to enrich.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force re-enrichment of already enriched nodes.",
)
@click.option(
    "--node-types",
    default="operation,pipeline",
    help="Comma-separated list of node types to enrich (operation, pipeline).",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging to see detailed progress.",
)
@database_option
def enrich(
    path: str,
    force: bool,
    node_types: str,
    verbose: bool,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Run LLM enrichment on an existing graph.
    
    This command adds AI-generated business summaries to SSIS operations and
    pipelines, enriching the graph with human-readable context for migration
    planning and documentation.
    
    Example: metazcode enrich --path ./data/ssis/project
    """
    # Setup logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    click.echo(f"🤖 Starting LLM enrichment for: {os.path.abspath(path)}")
    
    # Parse node types
    target_types = []
    type_mapping = {
        'operation': NodeType.OPERATION,
        'pipeline': NodeType.PIPELINE
    }
    
    for type_str in node_types.lower().split(','):
        type_str = type_str.strip()
        if type_str in type_mapping:
            target_types.append(type_mapping[type_str])
        else:
            click.echo(f"Warning: Unknown node type '{type_str}', ignoring")
    
    if not target_types:
        click.echo("Error: No valid node types specified", err=True)
        return

    try:
        # Build the graph client
        db_config = get_database_config(
            database, memgraph_host, memgraph_port, memgraph_username, memgraph_password
        )
        
        if not GraphClientBuilder.validate_connection(db_config):
            click.echo(
                f"Warning: Could not connect to {db_config.backend} backend. Falling back to NetworkX.",
                err=True,
            )
        
        graph_client = GraphClientBuilder.get_client(db_config)
        
        # Validate path and load graph if needed
        p = Path(path)
        if p.is_dir():
            root_path = str(p.resolve())
            target_file = None
        elif p.is_file():
            root_path = str(p.parent.resolve())
            target_file = str(p.resolve())
        else:
            click.echo(f"Error: Path '{path}' not found.", err=True)
            return
        
        # Load graph if empty
        node_count = graph_client.get_node_count()
        if node_count == 0:
            click.echo("Graph is empty, loading from path...")
            orchestrator = Orchestrator(
                graph_client=graph_client, 
                root_path=root_path, 
                target_file=target_file
            )
            orchestrator.run()
            node_count = graph_client.get_node_count()
            click.echo(f"Loaded {node_count} nodes")
        
        # Configure enrichment
        config = MetaZenseConfig.from_environment()
        config.enable_llm_enrichment = True
        
        # Initialize enrichment pipeline
        enrichment_pipeline = EnrichmentPipeline(graph_client)
        
        # Validate configuration
        validation = enrichment_pipeline.validate_configuration()
        if not validation["ready"]:
            click.echo("❌ LLM Enrichment configuration issues:")
            if not validation["api_key_present"]:
                click.echo(f"   No API key found: set OPENAI_API_KEY environment variable")
            if not validation["api_key_format"]:
                click.echo(f"   Invalid API key format")
            if not validation["model_supported"]:
                click.echo(f"   Unsupported model")
            if not validation["graph_client_connected"]:
                click.echo(f"   Graph client not connected")
            return
        
        # Run enrichment
        enrichment_results = enrichment_pipeline.enrich_graph(node_types=target_types)
        
        # Display results
        if enrichment_results["status"] == "completed":
            summary = enrichment_results["summary"]
            click.echo("")
            click.echo("✅ LLM Enrichment completed successfully!")
            click.echo(f"   Nodes processed: {summary['total_nodes']}")
            click.echo(f"   Successfully enriched: {summary['successfully_enriched']}")
            click.echo(f"   Failed: {summary['failed']}")
            click.echo(f"   Skipped: {summary['skipped']}")
            click.echo(f"   Success rate: {summary['success_rate']:.1f}%")
            
            if "token_usage" in enrichment_results:
                tokens = enrichment_results["token_usage"]
                if tokens.get("total_tokens", 0) > 0:
                    click.echo(f"   API tokens used: {tokens['total_tokens']}")
        else:
            click.echo(f"❌ Enrichment failed: {enrichment_results.get('reason', 'Unknown error')}")
    
    except Exception as e:
        click.echo(f"Error during enrichment: {e}", err=True)
        if verbose:
            import traceback
            click.echo(traceback.format_exc(), err=True)


# Alias command for shorter usage
@cli.command()
@click.option("--path", default=".", help="The path to the project directory.")
@click.option("--output", default=None, help="Path to save analysis results.")
@click.option("--enable-llm", is_flag=True, help="Enable LLM enrichment.")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging.")
@database_option
def complete(
    path: str,
    output: Optional[str],
    enable_llm: bool,
    verbose: bool,
    database: Optional[str],
    memgraph_host: Optional[str],
    memgraph_port: Optional[int],
    memgraph_username: Optional[str],
    memgraph_password: Optional[str],
):
    """
    Alias for 'full' command - complete SSIS analysis in one shot.

    Shorter command for: ingest + analyze + index

    Example: metazcode complete --path ./data/ssis/project
    """
    # Call the full command with default parameters
    from click import Context

    ctx = Context(full)
    ctx.invoke(
        full,
        path=path,
        output=output,
        index_output=None,
        project_id=None,
        verbose=verbose,
        enable_llm=enable_llm,
        database=database,
        memgraph_host=memgraph_host,
        memgraph_port=memgraph_port,
        memgraph_username=memgraph_username,
        memgraph_password=memgraph_password,
    )


if __name__ == "__main__":
    cli()
