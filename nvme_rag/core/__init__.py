from .chunker import NVMeChunker, NVMeChunk, NVMePDFParser

# llama_index는 RAG 파이프라인에만 필요하므로 optional import
try:
    from .pipeline import NVMeRAGPipeline, nvme_chunks_to_nodes
    _PIPELINE_AVAILABLE = True
except ImportError:
    _PIPELINE_AVAILABLE = False

# Qdrant 파이프라인 (qdrant-client / openai / fastembed 필요)
try:
    from .qdrant_pipeline import (
        NVMeQdrantPipeline,
        MetadataEnricher,
        NVMePointPayload,
        RetrievedChunk,
        NVMeAnswer,
    )
    _QDRANT_PIPELINE_AVAILABLE = True
except ImportError:
    _QDRANT_PIPELINE_AVAILABLE = False

__all__ = [
    # chunker
    "NVMeChunker",
    "NVMeChunk",
    "NVMePDFParser",
    # LlamaIndex pipeline (optional)
    "NVMeRAGPipeline",
    "nvme_chunks_to_nodes",
    # Qdrant pipeline (optional)
    "NVMeQdrantPipeline",
    "MetadataEnricher",
    "NVMePointPayload",
    "RetrievedChunk",
    "NVMeAnswer",
]
