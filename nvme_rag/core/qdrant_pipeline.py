"""
NVMe Spec RAG Pipeline (Qdrant + Hybrid Search)
------------------------------------------------
Qdrant를 사용한 NVMe 스펙 RAG 파이프라인입니다.

주요 기능:
  - Hybrid retrieval: dense (의미론적) + sparse BM25 (키워드/약어) → RRF 융합
  - 계층적 청킹: section / table / field / note
  - 풍부한 메타데이터: doc_id, version, spec_family, path, keywords, field_aliases 등
  - 컨텍스트 prefix: [Document] / [Section] / [Table] / [Field] / [Note/Requirement]
  - 부모-자식 컨텍스트 재구성: field → 부모 section, table → 부모 section
  - 소스 인용: 문서 제목, 섹션 번호, 페이지 범위
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Optional

# --- Optional imports (각 기능에만 필요) ---
try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Distance, VectorParams, SparseVectorParams, SparseIndexParams,
        PointStruct, NamedVector, NamedSparseVector, SparseVector,
        ScoredPoint, Filter, FieldCondition, MatchValue,
        PayloadSchemaType, HnswConfigDiff,
    )
    _QDRANT_AVAILABLE = True
except ImportError:
    _QDRANT_AVAILABLE = False

try:
    from openai import OpenAI as _OpenAI
    _OPENAI_AVAILABLE = True
except ImportError:
    _OPENAI_AVAILABLE = False

try:
    from fastembed import SparseTextEmbedding as _SparseTextEmbedding
    _FASTEMBED_AVAILABLE = True
except ImportError:
    _FASTEMBED_AVAILABLE = False

from .chunker import NVMeChunk, NVMeChunker


# ---------------------------------------------------------------------------
# Regex 패턴
# ---------------------------------------------------------------------------

# 커맨드 이름: "Copy Command", "Get Log Page Command" (섹션 제목 첫 단어 그룹)
_COMMAND_RE = re.compile(
    r"^((?:[A-Z][A-Za-z]+\s+){1,5}Command)\b"
)

# 괄호 약어: (FUA), (LR), (MPTR), (PRP1), (OACS)
_ALIAS_RE = re.compile(r"\(([A-Z][A-Z0-9_]{1,9})\)")

# 필드 정의 첫 줄: "31 LR: ...", "31:30 Description:"
_FIRST_FIELD_RE = re.compile(
    r"^(?:\d+:\d+|\d+)\s+([\w][^\n:]{2,50})(?:\s*\([A-Z]{2,10}\))?:",
    re.MULTILINE,
)


# ---------------------------------------------------------------------------
# 데이터 모델
# ---------------------------------------------------------------------------

@dataclass
class NVMePointPayload:
    """Qdrant 포인트 페이로드 — 모든 청크 메타데이터를 담습니다."""

    # 문서 식별
    doc_id: str
    doc_title: str
    version: str           # e.g. "1.1", "2.0"
    spec_family: str       # e.g. "NVM Command Set", "Base"

    # 청크 식별
    chunk_id: str
    chunk_type: str        # section / table / field / note
    chunk_index: int

    # 섹션 계층
    section_number: str    # e.g. "6.16.1"
    section_title: str
    section_depth: int
    subsection_number: str # 직전 상위 섹션 번호 (e.g. "6.16")
    subsection_title: str
    path: str              # "6 NVM Cmd Set > 6.16 Copy Command > 6.16.1 ..."

    # 내용 특화
    command_name: str      # "Copy Command", "Get Log Page Command", ...
    table_id: Optional[int]
    table_title: str
    field_name: str
    field_aliases: list[str]  # ["FUA", "LR", "PRINFOW", ...]

    # 페이지
    page_start: int
    page_end: int

    # 부모-자식 관계
    parent_chunk_id: str

    # 검색 보강
    keywords: list[str]

    # 텍스트
    text: str              # 원본 청크 텍스트
    context_text: str      # [Document]/[Section]/[Field] prefix 붙은 임베딩용 텍스트

    def to_dict(self) -> dict:
        return {
            "doc_id": self.doc_id,
            "doc_title": self.doc_title,
            "version": self.version,
            "spec_family": self.spec_family,
            "chunk_id": self.chunk_id,
            "chunk_type": self.chunk_type,
            "chunk_index": self.chunk_index,
            "section_number": self.section_number,
            "section_title": self.section_title,
            "section_depth": self.section_depth,
            "subsection_number": self.subsection_number,
            "subsection_title": self.subsection_title,
            "path": self.path,
            "command_name": self.command_name,
            "table_id": self.table_id,
            "table_title": self.table_title,
            "field_name": self.field_name,
            "field_aliases": self.field_aliases,
            "page_start": self.page_start,
            "page_end": self.page_end,
            "parent_chunk_id": self.parent_chunk_id,
            "keywords": self.keywords,
            "text": self.text,
            "context_text": self.context_text,
        }


@dataclass
class RetrievedChunk:
    """검색된 청크 + 부모 컨텍스트."""
    payload: NVMePointPayload
    score: float
    parent_payload: Optional[NVMePointPayload] = None

    def citation(self) -> str:
        """소스 인용 문자열 (문서 제목, 섹션 번호, 페이지 범위)."""
        p = self.payload
        page = (
            f"pp.{p.page_start}–{p.page_end}"
            if p.page_end and p.page_end != p.page_start
            else f"p.{p.page_start}"
        )
        return f"{p.doc_title}, §{p.section_number} {p.section_title} ({page})"

    def context_for_llm(self) -> str:
        """LLM 프롬프트에 전달할 컨텍스트 텍스트 (부모 포함)."""
        parts = []
        if self.parent_payload and self.parent_payload.chunk_type == "section":
            pp = self.parent_payload
            preview = pp.text[:400]
            parts.append(
                f"[Parent: §{pp.section_number} {pp.section_title}]\n{preview}"
            )
        parts.append(self.payload.context_text)
        return "\n\n".join(parts)


@dataclass
class NVMeAnswer:
    """RAG 생성 답변."""
    question: str
    answer: str
    citations: list[str]
    retrieved_chunks: list[RetrievedChunk]

    def formatted(self) -> str:
        """인용 포함 최종 답변 텍스트."""
        lines = [self.answer, "", "Sources:"]
        for i, cit in enumerate(self.citations, 1):
            lines.append(f"  [{i}] {cit}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# 순수 함수 헬퍼 (Qdrant/OpenAI 없이 테스트 가능)
# ---------------------------------------------------------------------------

def _chunk_id(doc_id: str, chunk_index: int) -> str:
    """결정론적 UUID 생성 — 재인덱싱 시 동일 ID 보장."""
    key = f"{doc_id}::{chunk_index}"
    return str(uuid.UUID(hashlib.md5(key.encode()).hexdigest()))


def _extract_command_name(section_title: str) -> str:
    """섹션 제목에서 커맨드 이름을 추출합니다.

    "Copy Command"             → "Copy Command"
    "Get Log Page Command"     → "Get Log Page Command"
    "Copy – Command Dword 10"  → ""  (서브섹션, 커맨드 아님)
    """
    m = _COMMAND_RE.match(section_title.strip())
    return m.group(1) if m else ""


def _extract_field_info(text: str, table_caption: str = "") -> tuple[str, list[str]]:
    """필드 이름(field_name)과 약어 목록(field_aliases)을 추출합니다.

    - table_caption이 있으면 field_name으로 사용 (Figure/Table 청크)
    - 없으면 첫 번째 비트 필드 정의 줄에서 추출
    - 약어: 텍스트 내 (UPPERCASE) 형식의 모든 약어
    """
    field_name = table_caption or ""
    if not field_name:
        m = _FIRST_FIELD_RE.search(text)
        field_name = m.group(1).strip() if m else ""

    aliases = sorted(set(_ALIAS_RE.findall(text)))
    return field_name, aliases


def _extract_keywords(
    text: str,
    section_title: str,
    command_name: str,
    field_aliases: list[str],
) -> list[str]:
    """BM25 sparse 검색 보강을 위한 키워드 목록을 추출합니다."""
    keywords: set[str] = set(field_aliases)

    # 섹션 제목의 주요 단어 (대문자 시작, 3자 이상)
    for word in re.split(r"\W+", section_title):
        if len(word) >= 3 and word[0].isupper():
            keywords.add(word)

    # 텍스트 내 괄호 약어
    keywords.update(_ALIAS_RE.findall(text))

    if command_name:
        keywords.add(command_name)

    return sorted(keywords)


def _build_path(section_number: str, section_map: dict[str, str]) -> str:
    """섹션 번호로 계층 경로 문자열을 구성합니다.

    "6.16.1" → "6 NVM Command Set > 6.16 Copy Command > 6.16.1 Copy – Dword 10"
    """
    if section_number.startswith("Annex"):
        title = section_map.get(section_number, "")
        return f"{section_number} {title}".strip()

    parts = section_number.split(".")
    segments = []
    for i in range(1, len(parts) + 1):
        sec = ".".join(parts[:i])
        title = section_map.get(sec, "")
        segments.append(f"{sec} {title}".strip() if title else sec)
    return " > ".join(segments)


def _build_context_text(
    chunk: NVMeChunk,
    doc_title: str,
    path: str,
    keywords: list[str],
) -> str:
    """임베딩 + LLM 프롬프트용 컨텍스트 prefix 텍스트를 생성합니다.

    형식 예시 (field 청크):
        [Document] NVM Express NVM Command Set Specification Rev 1.1
        [Keywords] FUA, LR, PRINFOW
        [Section] 6 NVM Command Set > 6.16 Copy Command > 6.16.3 Copy – Dword 12
        [Field] Figure 34: Copy – Command Dword 12
        <원본 텍스트>
    """
    lines = [f"[Document] {doc_title}"]

    if keywords:
        lines.append(f"[Keywords] {', '.join(keywords)}")

    lines.append(f"[Section] {path}")

    if chunk.chunk_type == "table":
        tbl_type = chunk.metadata.get("table_type", "Table")
        tbl_num  = chunk.metadata.get("table_number", "")
        caption  = chunk.metadata.get("table_caption", "")
        lines.append(f"[Table] {tbl_type} {tbl_num}: {caption}")

    elif chunk.chunk_type == "field":
        tbl_type = chunk.metadata.get("table_type", "Figure")
        tbl_num  = chunk.metadata.get("table_number", "")
        caption  = chunk.metadata.get("table_caption", "")
        label = f"{tbl_type} {tbl_num}: {caption}" if tbl_num else (caption or chunk.section_title)
        lines.append(f"[Field] {label}")

    elif chunk.chunk_type == "note":
        lines.append("[Note/Requirement]")

    # section 청크는 [Section] 라인만으로 충분

    lines.append("")
    lines.append(chunk.text)
    return "\n".join(lines)


def _rrf(
    results_list: list[list],
    k: int = 60,
    top_k: int = 10,
) -> list:
    """Reciprocal Rank Fusion으로 여러 검색 결과를 합산합니다.

    score(d) = Σ 1/(k + rank(d))  for each result list
    """
    scores: dict[str, dict] = {}
    for results in results_list:
        for rank, hit in enumerate(results):
            hit_id = str(hit.id)
            if hit_id not in scores:
                scores[hit_id] = {"score": 0.0, "hit": hit}
            scores[hit_id]["score"] += 1.0 / (k + rank + 1)

    sorted_items = sorted(scores.values(), key=lambda x: x["score"], reverse=True)
    return [item["hit"] for item in sorted_items[:top_k]]


# ---------------------------------------------------------------------------
# 메타데이터 보강기 (Qdrant 불필요, 독립적으로 테스트 가능)
# ---------------------------------------------------------------------------

class MetadataEnricher:
    """NVMeChunk 목록을 NVMePointPayload 목록으로 변환합니다.

    Parameters
    ----------
    doc_id      : 문서 고유 ID (재현 가능한 인덱싱을 위한 결정론적 값 권장)
    doc_title   : 문서 제목 (e.g. "NVM Express NVM Command Set Specification Rev 1.1")
    version     : 스펙 버전 (e.g. "1.1")
    spec_family : 스펙 계열 (e.g. "NVM Command Set", "Base")
    """

    def __init__(
        self,
        doc_id: str,
        doc_title: str,
        version: str = "",
        spec_family: str = "",
    ) -> None:
        self.doc_id = doc_id
        self.doc_title = doc_title
        self.version = version
        self.spec_family = spec_family

    def enrich(self, chunks: list[NVMeChunk]) -> list[NVMePointPayload]:
        """청크 목록 전체를 일괄 변환합니다."""
        # 1. 섹션 제목 맵 구성
        section_map: dict[str, str] = {
            c.section_number: c.section_title
            for c in chunks
            if c.section_number not in {}
        }

        # 2. 청크 ID 일괄 할당
        chunk_ids = [_chunk_id(self.doc_id, c.chunk_index) for c in chunks]

        # 3. 섹션 번호 → 첫 번째 section 청크 ID (부모 참조용)
        section_first_id: dict[str, str] = {}
        for c, cid in zip(chunks, chunk_ids):
            if c.chunk_type == "section" and c.section_number not in section_first_id:
                section_first_id[c.section_number] = cid

        # 4. 페이로드 생성
        return [
            self._make_payload(c, cid, section_map, section_first_id)
            for c, cid in zip(chunks, chunk_ids)
        ]

    def _make_payload(
        self,
        c: NVMeChunk,
        chunk_id: str,
        section_map: dict[str, str],
        section_first_id: dict[str, str],
    ) -> NVMePointPayload:
        # 섹션 depth
        depth = (
            1 if c.section_number.startswith("Annex")
            else len(c.section_number.split("."))
        )

        # 상위 섹션 (subsection = direct parent)
        subsection_num   = c.parent_section
        subsection_title = section_map.get(subsection_num, "")

        # 계층 경로
        path = _build_path(c.section_number, section_map)

        # 커맨드 이름 (현재 → 상위 섹션 순으로 탐색)
        command_name = _extract_command_name(c.section_title)
        if not command_name and subsection_num:
            command_name = _extract_command_name(subsection_title)

        # 테이블/필드 정보
        table_id    = c.metadata.get("table_number")
        table_title = c.metadata.get("table_caption", "")
        field_name, field_aliases = _extract_field_info(
            c.text,
            table_caption=table_title if c.chunk_type == "field" else "",
        )

        # 키워드
        keywords = _extract_keywords(c.text, c.section_title, command_name, field_aliases)

        # 컨텍스트 텍스트 (임베딩 + LLM용)
        context_text = _build_context_text(c, self.doc_title, path, keywords)

        # 부모 chunk ID
        #   - section 청크 → 상위 section 청크
        #   - table/field/note 청크 → 같은 섹션의 첫 section 청크
        if c.chunk_type == "section":
            parent_chunk_id = section_first_id.get(c.parent_section, "")
        else:
            parent_chunk_id = section_first_id.get(c.section_number, "")

        return NVMePointPayload(
            doc_id=self.doc_id,
            doc_title=self.doc_title,
            version=self.version,
            spec_family=self.spec_family,
            chunk_id=chunk_id,
            chunk_type=c.chunk_type,
            chunk_index=c.chunk_index,
            section_number=c.section_number,
            section_title=c.section_title,
            section_depth=depth,
            subsection_number=subsection_num,
            subsection_title=subsection_title,
            path=path,
            command_name=command_name,
            table_id=table_id,
            table_title=table_title,
            field_name=field_name,
            field_aliases=field_aliases,
            page_start=c.page_start,
            page_end=c.page_end,
            parent_chunk_id=parent_chunk_id,
            keywords=keywords,
            text=c.text,
            context_text=context_text,
        )


# ---------------------------------------------------------------------------
# Qdrant 인덱서
# ---------------------------------------------------------------------------

class NVMeQdrantIndexer:
    """NVMePointPayload 목록을 Qdrant 컬렉션에 업로드합니다.

    Parameters
    ----------
    client          : QdrantClient 인스턴스
    collection_name : 컬렉션 이름
    dense_dim       : dense 벡터 차원 수
                      text-embedding-3-small → 1536
                      text-embedding-3-large → 3072
    """

    def __init__(
        self,
        client: "QdrantClient",
        collection_name: str = "nvme_spec",
        dense_dim: int = 1536,
    ) -> None:
        if not _QDRANT_AVAILABLE:
            raise ImportError("pip install qdrant-client")
        self.client = client
        self.collection_name = collection_name
        self.dense_dim = dense_dim

    def create_collection(self, recreate: bool = False) -> None:
        """dense + sparse 벡터 컬렉션을 생성합니다."""
        exists = self.client.collection_exists(self.collection_name)
        if exists and not recreate:
            return
        if exists:
            self.client.delete_collection(self.collection_name)

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config={
                "dense": VectorParams(
                    size=self.dense_dim,
                    distance=Distance.COSINE,
                    hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
                ),
            },
            sparse_vectors_config={
                "sparse": SparseVectorParams(
                    index=SparseIndexParams(on_disk=False),
                ),
            },
        )

        # 페이로드 필터링 인덱스
        payload_indexes = [
            ("chunk_type",    PayloadSchemaType.KEYWORD),
            ("section_number", PayloadSchemaType.KEYWORD),
            ("doc_id",        PayloadSchemaType.KEYWORD),
            ("command_name",  PayloadSchemaType.KEYWORD),
            ("page_start",    PayloadSchemaType.INTEGER),
        ]
        for field_name, schema_type in payload_indexes:
            self.client.create_payload_index(
                collection_name=self.collection_name,
                field_name=field_name,
                field_schema=schema_type,
            )

    def index(
        self,
        payloads: list[NVMePointPayload],
        dense_vectors: list[list[float]],
        sparse_vectors: list[tuple[list[int], list[float]]],
        batch_size: int = 100,
    ) -> None:
        """페이로드와 벡터를 Qdrant에 업서트합니다."""
        assert len(payloads) == len(dense_vectors) == len(sparse_vectors), \
            "payloads, dense_vectors, sparse_vectors 길이가 일치해야 합니다"

        points = [
            PointStruct(
                id=p.chunk_id,
                vector={
                    "dense": dv,
                    "sparse": SparseVector(indices=sv[0], values=sv[1]),
                },
                payload=p.to_dict(),
            )
            for p, dv, sv in zip(payloads, dense_vectors, sparse_vectors)
        ]

        for i in range(0, len(points), batch_size):
            self.client.upsert(
                collection_name=self.collection_name,
                points=points[i : i + batch_size],
            )


# ---------------------------------------------------------------------------
# Qdrant 검색기
# ---------------------------------------------------------------------------

class NVMeQdrantRetriever:
    """Qdrant 하이브리드 검색 (dense + sparse → RRF) + 부모 컨텍스트 재구성.

    Parameters
    ----------
    client          : QdrantClient 인스턴스
    collection_name : 컬렉션 이름
    """

    def __init__(
        self,
        client: "QdrantClient",
        collection_name: str = "nvme_spec",
    ) -> None:
        if not _QDRANT_AVAILABLE:
            raise ImportError("pip install qdrant-client")
        self.client = client
        self.collection_name = collection_name

    def search(
        self,
        dense_query: list[float],
        sparse_query: tuple[list[int], list[float]],
        top_k: int = 5,
        chunk_type: Optional[str] = None,
        doc_id: Optional[str] = None,
    ) -> list[RetrievedChunk]:
        """하이브리드 검색 수행 후 부모 컨텍스트를 붙여 반환합니다."""
        filter_ = self._build_filter(chunk_type=chunk_type, doc_id=doc_id)

        # Dense 시맨틱 검색
        dense_hits = self.client.search(
            collection_name=self.collection_name,
            query_vector=NamedVector(name="dense", vector=dense_query),
            query_filter=filter_,
            limit=top_k * 2,
            with_payload=True,
        )

        # Sparse BM25 키워드 검색
        sp_indices, sp_values = sparse_query
        sparse_hits = self.client.search(
            collection_name=self.collection_name,
            query_vector=NamedSparseVector(
                name="sparse",
                vector=SparseVector(indices=sp_indices, values=sp_values),
            ),
            query_filter=filter_,
            limit=top_k * 2,
            with_payload=True,
        )

        # RRF 융합
        fused = _rrf([dense_hits, sparse_hits], top_k=top_k)

        # 부모-자식 컨텍스트 재구성
        return self._reconstruct_context(fused)

    def _build_filter(
        self,
        chunk_type: Optional[str],
        doc_id: Optional[str],
    ) -> Optional["Filter"]:
        conditions = []
        if chunk_type:
            conditions.append(
                FieldCondition(key="chunk_type", match=MatchValue(value=chunk_type))
            )
        if doc_id:
            conditions.append(
                FieldCondition(key="doc_id", match=MatchValue(value=doc_id))
            )
        return Filter(must=conditions) if conditions else None

    def _reconstruct_context(self, hits: list["ScoredPoint"]) -> list[RetrievedChunk]:
        """검색된 청크의 부모 섹션 청크를 일괄 조회하여 붙입니다."""
        parent_ids = {
            hit.payload.get("parent_chunk_id")
            for hit in hits
            if hit.payload.get("parent_chunk_id")
        }

        parents: dict[str, NVMePointPayload] = {}
        if parent_ids:
            parent_points = self.client.retrieve(
                collection_name=self.collection_name,
                ids=list(parent_ids),
                with_payload=True,
            )
            for pt in parent_points:
                parents[pt.payload["chunk_id"]] = NVMePointPayload(**pt.payload)

        result = []
        for hit in hits:
            payload = NVMePointPayload(**hit.payload)
            parent_id = hit.payload.get("parent_chunk_id", "")
            result.append(RetrievedChunk(
                payload=payload,
                score=hit.score,
                parent_payload=parents.get(parent_id),
            ))
        return result


# ---------------------------------------------------------------------------
# 통합 파이프라인
# ---------------------------------------------------------------------------

class NVMeQdrantPipeline:
    """NVMe 스펙 RAG 파이프라인 — 인덱싱부터 답변 생성까지.

    Parameters
    ----------
    qdrant_url      : Qdrant URL (":memory:" = 인메모리, 기본값)
    qdrant_api_key  : Qdrant Cloud API 키 (로컬은 불필요)
    openai_api_key  : OpenAI API 키
    dense_model     : 임베딩 모델 (기본: "text-embedding-3-small")
    sparse_model    : Sparse 임베딩 모델 (기본: "Qdrant/bm25")
    collection_name : Qdrant 컬렉션 이름 (기본: "nvme_spec")
    chunker_kwargs  : NVMeChunker 생성자 인자
    """

    DENSE_DIM_MAP = {
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
        "text-embedding-ada-002": 1536,
    }

    def __init__(
        self,
        qdrant_url: str = ":memory:",
        qdrant_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        dense_model: str = "text-embedding-3-small",
        sparse_model: str = "Qdrant/bm25",
        collection_name: str = "nvme_spec",
        chunker_kwargs: Optional[dict] = None,
    ) -> None:
        for pkg, flag, name in [
            (_QDRANT_AVAILABLE,   True, "pip install qdrant-client"),
            (_OPENAI_AVAILABLE,   True, "pip install openai"),
            (_FASTEMBED_AVAILABLE, True, "pip install fastembed"),
        ]:
            if not pkg:
                raise ImportError(name)

        # Qdrant 클라이언트
        self._qdrant = (
            QdrantClient(":memory:")
            if qdrant_url == ":memory:"
            else QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
        )

        # 임베딩 모델
        self._openai = _OpenAI(api_key=openai_api_key)
        self._dense_model = dense_model
        self._sparse_embedder = _SparseTextEmbedding(model_name=sparse_model)

        # 파이프라인 컴포넌트
        dense_dim = self.DENSE_DIM_MAP.get(dense_model, 1536)
        self._indexer   = NVMeQdrantIndexer(self._qdrant, collection_name, dense_dim)
        self._retriever = NVMeQdrantRetriever(self._qdrant, collection_name)
        self._chunker   = NVMeChunker(**(chunker_kwargs or {}))

    # ------------------------------------------------------------------
    # 인덱싱
    # ------------------------------------------------------------------

    def build_index(
        self,
        pdf_path: str | Path,
        doc_title: str = "",
        version: str = "",
        spec_family: str = "",
        recreate: bool = False,
    ) -> int:
        """PDF를 청킹 → 임베딩 → Qdrant 인덱싱합니다.

        Returns: 생성된 청크(포인트) 수
        """
        pdf_path = Path(pdf_path)
        if not doc_title:
            doc_title = pdf_path.stem.replace("_", " ")

        doc_id = hashlib.md5(str(pdf_path.resolve()).encode()).hexdigest()[:16]

        print(f"[NVMeQdrant] 청킹: {pdf_path.name}")
        chunks = self._chunker.chunk_pdf(pdf_path)
        print(f"[NVMeQdrant] {len(chunks)}개 청크 생성")

        enricher = MetadataEnricher(doc_id, doc_title, version, spec_family)
        payloads = enricher.enrich(chunks)

        print(f"[NVMeQdrant] 임베딩 중 (dense={self._dense_model}, sparse=BM25)...")
        texts = [p.context_text for p in payloads]
        dense_vecs  = self._embed_dense_batch(texts)
        sparse_vecs = self._embed_sparse_batch(texts)

        self._indexer.create_collection(recreate=recreate)
        self._indexer.index(payloads, dense_vecs, sparse_vecs)
        print(f"[NVMeQdrant] 인덱싱 완료: {len(payloads)}개 포인트")
        return len(payloads)

    # ------------------------------------------------------------------
    # 검색
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        top_k: int = 5,
        chunk_type: Optional[str] = None,
        doc_id: Optional[str] = None,
    ) -> list[RetrievedChunk]:
        """하이브리드 검색 (dense + sparse → RRF) 수행."""
        dense_q  = self._embed_dense(query)
        sparse_q = self._embed_sparse(query)
        return self._retriever.search(
            dense_query=dense_q,
            sparse_query=sparse_q,
            top_k=top_k,
            chunk_type=chunk_type,
            doc_id=doc_id,
        )

    def answer(
        self,
        question: str,
        top_k: int = 5,
        llm_model: str = "gpt-4o",
    ) -> NVMeAnswer:
        """하이브리드 검색 + LLM으로 답변을 생성합니다."""
        retrieved = self.search(question, top_k=top_k)

        context = "\n\n---\n\n".join(
            f"[Context {i}]\n{r.context_for_llm()}"
            for i, r in enumerate(retrieved, 1)
        )

        system_prompt = (
            "You are an expert on NVMe (NVM Express) specifications. "
            "Answer questions based solely on the provided specification excerpts. "
            "Be precise about technical details: field names, bit definitions, "
            "command parameters, and normative requirements (shall/should/may). "
            "Always cite the source with section number and page range."
        )

        response = self._openai.chat.completions.create(
            model=llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": f"Question: {question}\n\nContext:\n{context}"},
            ],
        )

        return NVMeAnswer(
            question=question,
            answer=response.choices[0].message.content,
            citations=[r.citation() for r in retrieved],
            retrieved_chunks=retrieved,
        )

    # ------------------------------------------------------------------
    # 임베딩 헬퍼
    # ------------------------------------------------------------------

    def _embed_dense(self, text: str) -> list[float]:
        resp = self._openai.embeddings.create(model=self._dense_model, input=text)
        return resp.data[0].embedding

    def _embed_dense_batch(
        self, texts: list[str], batch_size: int = 100
    ) -> list[list[float]]:
        result = []
        for i in range(0, len(texts), batch_size):
            resp = self._openai.embeddings.create(
                model=self._dense_model, input=texts[i : i + batch_size]
            )
            result.extend(item.embedding for item in resp.data)
        return result

    def _embed_sparse(self, text: str) -> tuple[list[int], list[float]]:
        emb = next(self._sparse_embedder.embed([text]))
        return emb.indices.tolist(), emb.values.tolist()

    def _embed_sparse_batch(
        self, texts: list[str]
    ) -> list[tuple[list[int], list[float]]]:
        return [
            (emb.indices.tolist(), emb.values.tolist())
            for emb in self._sparse_embedder.embed(texts)
        ]
