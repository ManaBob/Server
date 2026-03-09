"""
NVMe RAG Pipeline (LlamaIndex)
-------------------------------
NVMeChunk -> LlamaIndex TextNode -> VectorStoreIndex 흐름을 담당합니다.

지원 벡터 스토어:
  - 기본 (in-memory)  : ChromaDB 설정 없이 사용
  - ChromaDB          : chroma_* 파라미터 전달 시 자동 사용

지원 그래프 스토어:
  - Neo4j             : build_graph_index() 호출 시 사용

사용 예:
    # 기본 (in-memory)
    pipeline = NVMeRAGPipeline()
    pipeline.build_index("nvme_spec.pdf")

    # ChromaDB
    pipeline = NVMeRAGPipeline(
        chroma_host="localhost", chroma_port=8000, chroma_collection="nvme"
    )
    pipeline.build_index("nvme_spec.pdf")

    # Neo4j 그래프
    pipeline = NVMeRAGPipeline(
        neo4j_url="bolt://localhost:7687",
        neo4j_username="neo4j",
        neo4j_password="password",
    )
    pipeline.build_graph_index("nvme_spec.pdf")
    result = pipeline.query_graph("NVMe command set란?")
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from llama_index.core import (
    Settings,
    StorageContext,
    VectorStoreIndex,
    KnowledgeGraphIndex,
    load_index_from_storage,
)
from llama_index.core.schema import TextNode

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
            metadata_template="{key}: {value}",
            text_template=(
                "Section {section_number} - {section_title}\n"
                "(Page {page_start})\n\n"
                "{content}"
            ),
        )
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

    ChromaDB 파라미터 (셋 중 하나를 선택)
    ----------------------------------------
    chroma_host, chroma_port :
        원격 ChromaDB 서버 주소 (HttpClient). 예: "localhost", 8000
    chroma_path :
        로컬 ChromaDB 경로 (PersistentClient). 예: "./chroma_db"
    chroma_collection :
        사용할 컬렉션 이름 (기본값: "nvme_docs")

    Neo4j 파라미터
    ----------------------------------------
    neo4j_url :
        Neo4j Bolt URL. 예: "bolt://localhost:7687"
    neo4j_username :
        Neo4j 사용자 이름 (기본값: "neo4j")
    neo4j_password :
        Neo4j 비밀번호
    neo4j_database :
        데이터베이스 이름 (기본값: "neo4j")
    """

    def __init__(
        self,
        embed_model=None,
        llm=None,
        chunker_kwargs: Optional[dict] = None,
        # ChromaDB
        chroma_host: Optional[str] = None,
        chroma_port: int = 8000,
        chroma_path: Optional[str] = None,
        chroma_collection: str = "nvme_docs",
        # Neo4j
        neo4j_url: Optional[str] = None,
        neo4j_username: str = "neo4j",
        neo4j_password: Optional[str] = None,
        neo4j_database: str = "neo4j",
    ) -> None:
        if embed_model:
            Settings.embed_model = embed_model
        if llm:
            Settings.llm = llm

        self._chunker = NVMeChunker(**(chunker_kwargs or {}))
        self._index: Optional[VectorStoreIndex] = None
        self._graph_index: Optional[KnowledgeGraphIndex] = None

        # ChromaDB 설정 저장
        self._chroma_host = chroma_host
        self._chroma_port = chroma_port
        self._chroma_path = chroma_path
        self._chroma_collection = chroma_collection

        # Neo4j 설정 저장
        self._neo4j_url = neo4j_url
        self._neo4j_username = neo4j_username
        self._neo4j_password = neo4j_password
        self._neo4j_database = neo4j_database

    # ------------------------------------------------------------------
    # ChromaDB 헬퍼
    # ------------------------------------------------------------------

    def _make_chroma_vector_store(self):
        """ChromaDB 설정에 따라 VectorStore를 반환합니다."""
        import chromadb
        from llama_index.vector_stores.chroma import ChromaVectorStore

        if self._chroma_host:
            client = chromadb.HttpClient(
                host=self._chroma_host,
                port=self._chroma_port,
            )
            print(f"[ChromaDB] HTTP 클라이언트 연결: {self._chroma_host}:{self._chroma_port}")
        elif self._chroma_path:
            client = chromadb.PersistentClient(path=self._chroma_path)
            print(f"[ChromaDB] 로컬 저장소 연결: {self._chroma_path}")
        else:
            raise ValueError(
                "ChromaDB를 사용하려면 chroma_host 또는 chroma_path를 지정하세요."
            )

        collection = client.get_or_create_collection(self._chroma_collection)
        print(f"[ChromaDB] 컬렉션: '{self._chroma_collection}' (기존 문서 수: {collection.count()})")
        return ChromaVectorStore(chroma_collection=collection)

    # ------------------------------------------------------------------
    # Neo4j 헬퍼
    # ------------------------------------------------------------------

    def _make_neo4j_graph_store(self):
        """Neo4j GraphStore를 반환합니다."""
        from llama_index.graph_stores.neo4j import Neo4jGraphStore

        if not self._neo4j_url or not self._neo4j_password:
            raise ValueError(
                "Neo4j를 사용하려면 neo4j_url과 neo4j_password를 지정하세요."
            )

        graph_store = Neo4jGraphStore(
            username=self._neo4j_username,
            password=self._neo4j_password,
            url=self._neo4j_url,
            database=self._neo4j_database,
        )
        print(f"[Neo4j] 연결: {self._neo4j_url} (DB: {self._neo4j_database})")
        return graph_store

    # ------------------------------------------------------------------
    # 벡터 인덱스 빌드
    # ------------------------------------------------------------------

    def build_index(
        self,
        pdf_path: str | Path,
        persist_dir: Optional[str | Path] = None,
    ) -> VectorStoreIndex:
        """
        PDF를 청킹하고 VectorStoreIndex를 생성합니다.

        ChromaDB 파라미터가 설정된 경우 ChromaDB에 저장합니다.
        설정되지 않은 경우 기본 in-memory 스토어를 사용합니다.

        Parameters
        ----------
        pdf_path : PDF 파일 경로
        persist_dir : 인덱스를 저장할 디렉토리 (in-memory 모드에서만 사용)
        """
        print(f"[NVMeRAG] 청킹 시작: {pdf_path}")
        chunks = self._chunker.chunk_pdf(pdf_path)
        print(f"[NVMeRAG] 청크 생성 완료: {len(chunks)}개")

        nodes = nvme_chunks_to_nodes(chunks)
        print(f"[NVMeRAG] LlamaIndex 노드 변환 완료: {len(nodes)}개")

        use_chroma = self._chroma_host or self._chroma_path
        if use_chroma:
            vector_store = self._make_chroma_vector_store()
            storage_context = StorageContext.from_defaults(vector_store=vector_store)
            self._index = VectorStoreIndex(
                nodes,
                storage_context=storage_context,
                show_progress=True,
            )
            print("[NVMeRAG] ChromaDB 인덱스 빌드 완료 (자동 영구 저장)")
        else:
            self._index = VectorStoreIndex(nodes, show_progress=True)
            if persist_dir:
                persist_dir = Path(persist_dir)
                persist_dir.mkdir(parents=True, exist_ok=True)
                self._index.storage_context.persist(persist_dir=str(persist_dir))
                print(f"[NVMeRAG] 인덱스 저장 완료: {persist_dir}")

        return self._index

    def load_index(self, persist_dir: str | Path) -> VectorStoreIndex:
        """
        저장된 인덱스를 불러옵니다.

        ChromaDB 파라미터가 설정된 경우 ChromaDB에서 로드합니다.
        """
        use_chroma = self._chroma_host or self._chroma_path
        if use_chroma:
            vector_store = self._make_chroma_vector_store()
            storage_context = StorageContext.from_defaults(vector_store=vector_store)
            self._index = VectorStoreIndex.from_vector_store(
                vector_store,
                storage_context=storage_context,
            )
            print("[NVMeRAG] ChromaDB에서 인덱스 로드 완료")
        else:
            storage_context = StorageContext.from_defaults(
                persist_dir=str(persist_dir)
            )
            self._index = load_index_from_storage(storage_context)
            print(f"[NVMeRAG] 인덱스 로드 완료: {persist_dir}")

        return self._index

    # ------------------------------------------------------------------
    # Neo4j 그래프 인덱스 빌드
    # ------------------------------------------------------------------

    def build_graph_index(
        self,
        pdf_path: str | Path,
        max_triplets_per_chunk: int = 2,
        include_embeddings: bool = True,
    ) -> KnowledgeGraphIndex:
        """
        PDF를 청킹하고 Neo4j Knowledge Graph Index를 생성합니다.

        LLM이 각 청크에서 (주어, 관계, 목적어) 트리플을 추출해 Neo4j에 저장합니다.

        Parameters
        ----------
        pdf_path : PDF 파일 경로
        max_triplets_per_chunk : 청크당 최대 추출 트리플 수 (기본값: 2)
        include_embeddings : 노드에 임베딩을 포함할지 여부 (기본값: True)
        """
        print(f"[NVMeRAG/Graph] 청킹 시작: {pdf_path}")
        chunks = self._chunker.chunk_pdf(pdf_path)
        print(f"[NVMeRAG/Graph] 청크 생성 완료: {len(chunks)}개")

        nodes = nvme_chunks_to_nodes(chunks)

        graph_store = self._make_neo4j_graph_store()

        storage_context = StorageContext.from_defaults(graph_store=graph_store)

        print(f"[NVMeRAG/Graph] Knowledge Graph 구축 중 (청크당 최대 {max_triplets_per_chunk}개 트리플)...")
        self._graph_index = KnowledgeGraphIndex(
            nodes,
            storage_context=storage_context,
            max_triplets_per_chunk=max_triplets_per_chunk,
            include_embeddings=include_embeddings,
            show_progress=True,
        )
        print("[NVMeRAG/Graph] Neo4j Knowledge Graph 구축 완료")
        return self._graph_index

    def load_graph_index(self) -> KnowledgeGraphIndex:
        """Neo4j에서 기존 그래프 인덱스를 로드합니다."""
        graph_store = self._make_neo4j_graph_store()
        storage_context = StorageContext.from_defaults(graph_store=graph_store)
        self._graph_index = KnowledgeGraphIndex.from_existing(
            storage_context=storage_context,
        )
        print("[NVMeRAG/Graph] Neo4j 그래프 인덱스 로드 완료")
        return self._graph_index

    # ------------------------------------------------------------------
    # 쿼리 (벡터)
    # ------------------------------------------------------------------

    def query(
        self,
        question: str,
        similarity_top_k: int = 5,
        response_mode: str = "compact",
    ):
        """
        벡터 인덱스로 질문에 대한 답변을 생성합니다.

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
    # 쿼리 (그래프)
    # ------------------------------------------------------------------

    def query_graph(
        self,
        question: str,
        similarity_top_k: int = 5,
        response_mode: str = "compact",
        use_keyword: bool = True,
        use_global: bool = False,
    ):
        """
        Neo4j Knowledge Graph 인덱스로 질문에 대한 답변을 생성합니다.

        Parameters
        ----------
        question : 질문 문자열
        similarity_top_k : 검색할 상위 노드 수
        response_mode : "compact" | "refine" | "tree_summarize"
        use_keyword : 키워드 기반 그래프 탐색 활성화 (기본값: True)
        use_global : 전체 그래프 컨텍스트 포함 여부 (느리지만 포괄적)
        """
        if self._graph_index is None:
            raise RuntimeError(
                "그래프 인덱스가 없습니다. build_graph_index() 또는 load_graph_index()를 먼저 호출하세요."
            )

        query_engine = self._graph_index.as_query_engine(
            include_text=True,
            retriever_mode="keyword" if use_keyword else "embedding",
            response_mode=response_mode,
            similarity_top_k=similarity_top_k,
            graph_store_query_depth=2,
        )
        return query_engine.query(question)

    def retrieve_from_graph(
        self,
        question: str,
        similarity_top_k: int = 5,
    ) -> list:
        """그래프에서 관련 트리플을 검색합니다 (LLM 없이)."""
        if self._graph_index is None:
            raise RuntimeError("그래프 인덱스가 없습니다.")

        retriever = self._graph_index.as_retriever(
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
