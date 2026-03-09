"""
NVMe RAG Pipeline (LlamaIndex)
-------------------------------
NVMeChunk -> LlamaIndex TextNode -> VectorStoreIndex 흐름을 담당합니다.

사용 예:
    pipeline = NVMeRAGPipeline(openai_api_key="...")
    pipeline.build_index("NVM_Express_Base_Specification.pdf")
    result = pipeline.query("What is the maximum queue depth for NVMe?")
    print(result.response)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from llama_index.core import (
    Settings,
    StorageContext,
    VectorStoreIndex,
    load_index_from_storage,
)
from llama_index.core.schema import TextNode
from llama_index.core.node_parser import SimpleNodeParser

from .chunker import NVMeChunker, NVMeChunk


# ---------------------------------------------------------------------------
# 청크 -> LlamaIndex TextNode 변환
# ---------------------------------------------------------------------------

def nvme_chunks_to_nodes(chunks: list[NVMeChunk]) -> list[TextNode]:
    """NVMeChunk 목록을 LlamaIndex TextNode 목록으로 변환합니다."""
    nodes = []
    for chunk in chunks:
        node = TextNode(
            text=chunk.text,
            metadata=chunk.to_metadata(),
            # 메타데이터 중 검색 시 함께 임베딩할 필드 지정
            metadata_template="{key}: {value}",
            text_template=(
                "Section {section_number} - {section_title}\n"
                "(Page {page_start})\n\n"
                "{content}"
            ),
        )
        # 섹션 제목을 excluded_embed_metadata_keys에서 제외해 임베딩에 포함
        node.excluded_embed_metadata_keys = [
            "chunk_index",
            "page_end",
        ]
        node.excluded_llm_metadata_keys = [
            "chunk_index",
        ]
        nodes.append(node)
    return nodes


# ---------------------------------------------------------------------------
# 파이프라인
# ---------------------------------------------------------------------------

class NVMeRAGPipeline:
    """
    NVMe 스펙 PDF를 인덱싱하고 쿼리하는 RAG 파이프라인입니다.

    Parameters
    ----------
    embed_model :
        LlamaIndex 임베딩 모델. None이면 Settings.embed_model 사용.
    llm :
        LlamaIndex LLM. None이면 Settings.llm 사용.
    chunker_kwargs :
        NVMeChunker 생성자에 전달할 키워드 인자.
        예: {"max_chunk_size": 1200, "overlap_size": 150}
    """

    def __init__(
        self,
        embed_model=None,
        llm=None,
        chunker_kwargs: Optional[dict] = None,
    ) -> None:
        if embed_model:
            Settings.embed_model = embed_model
        if llm:
            Settings.llm = llm

        self._chunker = NVMeChunker(**(chunker_kwargs or {}))
        self._index: Optional[VectorStoreIndex] = None

    # ------------------------------------------------------------------
    # 인덱스 빌드
    # ------------------------------------------------------------------

    def build_index(
        self,
        pdf_path: str | Path,
        persist_dir: Optional[str | Path] = None,
    ) -> VectorStoreIndex:
        """
        PDF를 청킹하고 VectorStoreIndex를 생성합니다.

        Parameters
        ----------
        pdf_path : PDF 파일 경로
        persist_dir : 인덱스를 저장할 디렉토리 (None이면 저장 안 함)
        """
        print(f"[NVMeRAG] 청킹 시작: {pdf_path}")
        chunks = self._chunker.chunk_pdf(pdf_path)
        print(f"[NVMeRAG] 청크 생성 완료: {len(chunks)}개")

        nodes = nvme_chunks_to_nodes(chunks)
        print(f"[NVMeRAG] LlamaIndex 노드 변환 완료: {len(nodes)}개")

        self._index = VectorStoreIndex(nodes, show_progress=True)

        if persist_dir:
            persist_dir = Path(persist_dir)
            persist_dir.mkdir(parents=True, exist_ok=True)
            self._index.storage_context.persist(persist_dir=str(persist_dir))
            print(f"[NVMeRAG] 인덱스 저장 완료: {persist_dir}")

        return self._index

    def load_index(self, persist_dir: str | Path) -> VectorStoreIndex:
        """저장된 인덱스를 불러옵니다."""
        storage_context = StorageContext.from_defaults(
            persist_dir=str(persist_dir)
        )
        self._index = load_index_from_storage(storage_context)
        print(f"[NVMeRAG] 인덱스 로드 완료: {persist_dir}")
        return self._index

    # ------------------------------------------------------------------
    # 쿼리
    # ------------------------------------------------------------------

    def query(
        self,
        question: str,
        similarity_top_k: int = 5,
        response_mode: str = "compact",
    ):
        """
        질문에 대한 답변을 생성합니다.

        Parameters
        ----------
        question : 질문 문자열
        similarity_top_k : 검색할 상위 청크 수
        response_mode : "compact" | "refine" | "tree_summarize"
        """
        if self._index is None:
            raise RuntimeError("인덱스가 없습니다. build_index() 또는 load_index()를 먼저 호출하세요.")

        query_engine = self._index.as_query_engine(
            similarity_top_k=similarity_top_k,
            response_mode=response_mode,
        )
        return query_engine.query(question)

    def retrieve(
        self,
        question: str,
        similarity_top_k: int = 5,
    ) -> list:
        """
        질문과 유사한 청크를 검색합니다 (LLM 생성 없이 검색만).

        Returns
        -------
        list[NodeWithScore]
        """
        if self._index is None:
            raise RuntimeError("인덱스가 없습니다.")

        retriever = self._index.as_retriever(
            similarity_top_k=similarity_top_k,
        )
        return retriever.retrieve(question)

    # ------------------------------------------------------------------
    # 청킹 결과만 확인하는 유틸
    # ------------------------------------------------------------------

    def inspect_chunks(
        self,
        pdf_path: str | Path,
        max_display: int = 10,
    ) -> list[NVMeChunk]:
        """청킹 결과를 출력하고 반환합니다 (인덱스 빌드 없이)."""
        chunks = self._chunker.chunk_pdf(pdf_path)
        print(f"\n총 {len(chunks)}개 청크 생성\n")
        print(f"{'#':<6} {'타입':<10} {'섹션':<12} {'제목':<35} {'페이지':<8} {'길이'}")
        print("-" * 90)
        for c in chunks[:max_display]:
            print(
                f"{c.chunk_index:<6} {c.chunk_type:<10} {c.section_number:<12} "
                f"{c.section_title[:33]:<35} {c.page_start:<8} {len(c.text)}"
            )
        if len(chunks) > max_display:
            print(f"... 외 {len(chunks) - max_display}개")
        return chunks
