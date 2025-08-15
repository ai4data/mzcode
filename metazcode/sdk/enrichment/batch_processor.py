"""
Batch Processor for Efficient LLM Enrichment

This module handles batch processing of nodes for efficient API utilization
and provides progress tracking for the enrichment process.
"""

import logging
import time
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

from .node_enricher import NodeEnricher

logger = logging.getLogger(__name__)


class BatchProcessor:
    """
    Processes nodes in batches for efficient LLM enrichment.
    
    This class manages concurrent processing, progress tracking,
    and error handling for batch enrichment operations.
    """
    
    def __init__(self, node_enricher: NodeEnricher, batch_size: int = 10):
        """
        Initialize the batch processor.
        
        Args:
            node_enricher: The node enricher instance to use
            batch_size: Number of nodes to process concurrently
        """
        self.node_enricher = node_enricher
        self.batch_size = batch_size
        
        # Progress tracking
        self.progress = {
            "total": 0,
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "start_time": None,
            "last_update": None
        }
    
    def process_nodes(self, node_ids: List[str]) -> Dict[str, Any]:
        """
        Process a list of nodes in batches.
        
        Args:
            node_ids: List of node IDs to process
            
        Returns:
            Summary of processing results
        """
        self.progress["total"] = len(node_ids)
        self.progress["start_time"] = time.time()
        self.progress["processed"] = 0
        self.progress["successful"] = 0
        self.progress["failed"] = 0
        
        logger.info(f"Starting batch processing of {len(node_ids)} nodes")
        
        # Process nodes in parallel batches
        with ThreadPoolExecutor(max_workers=min(self.batch_size, len(node_ids))) as executor:
            # Submit all tasks
            future_to_node = {
                executor.submit(self.node_enricher.enrich_node, node_id): node_id 
                for node_id in node_ids
            }
            
            # Process completed tasks and show progress
            for future in as_completed(future_to_node):
                node_id = future_to_node[future]
                try:
                    success = future.result()
                    if success:
                        self.progress["successful"] += 1
                    else:
                        self.progress["failed"] += 1
                except Exception as e:
                    logger.error(f"Exception processing node {node_id}: {e}")
                    self.progress["failed"] += 1
                
                self.progress["processed"] += 1
                self._update_progress()
        
        # Final progress update
        self._print_final_summary()
        
        return {
            "total_nodes": self.progress["total"],
            "processed": self.progress["processed"],
            "successful": self.progress["successful"],
            "failed": self.progress["failed"],
            "success_rate": (self.progress["successful"] / max(self.progress["processed"], 1)) * 100
        }
    
    def _update_progress(self):
        """Update and display progress."""
        current_time = time.time()
        
        # Only update display every 0.5 seconds to avoid spam
        if self.progress["last_update"] is None or (current_time - self.progress["last_update"]) > 0.5:
            self.progress["last_update"] = current_time
            self._print_progress()
    
    def _print_progress(self):
        """Print progress bar to console."""
        processed = self.progress["processed"]
        total = self.progress["total"]
        
        if total == 0:
            return
        
        # Calculate progress
        percentage = (processed / total) * 100
        
        # Calculate rate
        elapsed = time.time() - self.progress["start_time"]
        rate = processed / elapsed if elapsed > 0 else 0
        
        # Estimate time remaining
        if rate > 0:
            remaining = (total - processed) / rate
            eta_str = f"{int(remaining)}s"
        else:
            eta_str = "calculating..."
        
        # Build progress message
        progress_msg = (
            f"\rProgress: {processed}/{total} ({percentage:.1f}%) | "
            f"Rate: {rate:.1f} nodes/s | ETA: {eta_str}"
        )
        
        # Print progress (using \r to overwrite the line)
        print(progress_msg, end='', flush=True)
    
    def _print_final_summary(self):
        """Print final summary after processing."""
        # Clear the progress line
        print("\r" + " " * 80 + "\r", end='')
        
        # Calculate final statistics
        total_time = time.time() - self.progress["start_time"]
        
        print(f"Completed in {total_time:.1f} seconds")
        
        # Log detailed summary
        summary = {
            "total_nodes": self.progress["total"],
            "processed": self.progress["processed"],
            "successful": self.progress["successful"],
            "failed": self.progress["failed"],
            "success_rate": (self.progress["successful"] / max(self.progress["processed"], 1)) * 100
        }
        
        logger.info(f"Batch processing complete: {summary}")
    
    def process_in_chunks(self, node_ids: List[str], chunk_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Process nodes in smaller chunks to manage memory and API rate limits.
        
        Args:
            node_ids: List of node IDs to process
            chunk_size: Size of each chunk (defaults to batch_size * 5)
            
        Returns:
            Combined summary of all chunks
        """
        if chunk_size is None:
            chunk_size = self.batch_size * 5
        
        overall_stats = {
            "total_nodes": len(node_ids),
            "processed": 0,
            "successful": 0,
            "failed": 0
        }
        
        # Process in chunks
        for i in range(0, len(node_ids), chunk_size):
            chunk = node_ids[i:i + chunk_size]
            logger.info(f"Processing chunk {i//chunk_size + 1} ({len(chunk)} nodes)")
            
            chunk_results = self.process_nodes(chunk)
            
            # Aggregate results
            overall_stats["processed"] += chunk_results["processed"]
            overall_stats["successful"] += chunk_results["successful"]
            overall_stats["failed"] += chunk_results["failed"]
            
            # Add a small delay between chunks to avoid rate limits
            if i + chunk_size < len(node_ids):
                time.sleep(1)
        
        overall_stats["success_rate"] = (
            overall_stats["successful"] / max(overall_stats["processed"], 1)
        ) * 100
        
        return overall_stats