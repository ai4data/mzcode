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
def ingest(path: str):
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

    # 1. Build the graph client
    graph_client = GraphClientBuilder.get_client()

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
def visualize(path: str, output_path: str):
    """
    Generates a visual representation of the graph.
    NOTE: This is a basic visualization and may be unreadable for large graphs.
    """
    click.echo(
        f"Generating graph visualization for '{path}' and saving to '{output_path}'..."
    )

    graph_client = GraphClientBuilder.get_client()

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
def dump(path: str, output: Optional[str]):
    """
    Ingests the project and prints the nodes and edges to the console or saves to a JSON file.
    """
    click.echo("==================== Starting Ingestion ====================")
    graph_client = GraphClientBuilder.get_client()

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
def analyze(path: str, output: Optional[str], verbose: bool):
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
    
    click.echo(f"[START] Starting cross-package dependency analysis for: {os.path.abspath(path)}")
    
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
        # 1. Build the graph client and load existing graph
        graph_client = GraphClientBuilder.get_client()
        
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
        click.echo(f"   Shared tables: {analysis_results['shared_resources']['tables']}")
        click.echo(f"   Shared connections: {analysis_results['shared_resources']['connections']}")
        click.echo(f"   Data dependencies: {analysis_results['data_dependencies']}")
        click.echo(f"   Cross-package edges added: {analysis_results['cross_package_edges_added']}")
        
        # Show execution chains
        execution_order = analysis_results['detailed_analysis']['execution_order']
        if len(execution_order) > 1:
            click.echo(f"\n[INFO] Execution order (sequential levels):")
            for level, packages in enumerate(execution_order, 1):
                package_names = [pkg.split(':')[1] if ':' in pkg else pkg for pkg in packages]
                click.echo(f"   Level {level}: {', '.join(package_names)}")
        else:
            click.echo(f"\n[INFO] All packages can execute in parallel (no dependencies)")
        
        # Show high-risk resources
        contention_risks = analysis_results['contention_risks']
        if contention_risks['high_risk_connections']:
            click.echo(f"\n[WARNING] High-risk connections (resource contention):")
            for risk in contention_risks['high_risk_connections']:
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
        
        click.echo(f"[SUCCESS] Enhanced graph with cross-package dependencies saved to {enhanced_graph_path.resolve()}")
        
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
def ingest_n_index(path: str, index_output: Optional[str], project_id: Optional[str]):
    """
    Run both ingestion and indexing together.
    
    This command performs the complete workflow of data ingestion followed by
    enhanced SSIS indexing for fast search and analysis capabilities.
    """
    click.echo(f"Starting integrated ingestion and indexing for: {os.path.abspath(path)}")
    
    try:
        # Initialize integration component
        integration = IndexIntegration()
        
        # Run integrated workflow
        results = integration.ingest_and_index(
            path=path,
            index_output=index_output,
            project_id=project_id
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
            click.echo(f"  - SQL operations indexed: {ssis_stats['sql_operations_indexed']}")
            click.echo(f"  - Shared tables indexed: {ssis_stats['shared_tables_indexed']}")
            click.echo(f"  - Parameterized connections indexed: {ssis_stats['parameterized_connections_indexed']}")
            click.echo(f"  - Cross-package pipelines indexed: {ssis_stats['cross_package_pipelines_indexed']}")
        
        # Show file outputs
        files_created = results["files_created"]
        if files_created["index_file"]:
            click.echo(f"")
            click.echo(f"Files Created:")
            click.echo(f"  - Index file: {files_created['index_file']}")
            click.echo(f"  - Metadata file: {files_created['metadata_file']}")
        
        click.echo(f"")
        click.echo(f"[SUCCESS] Enhanced SSIS indexing complete. Graph is now searchable!")
        
    except Exception as e:
        click.echo(f"[ERROR] Error during integrated ingestion and indexing: {e}", err=True)
        import traceback
        click.echo(traceback.format_exc(), err=True)


if __name__ == "__main__":
    cli()
