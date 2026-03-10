# NVMe Spec RAG System — PR 변경 내용

## 개요

NVMe 스펙 문서를 대상으로 한 RAG(Retrieval-Augmented Generation) 시스템을 구축했습니다.
`MPTR`, `PRP1`, `OACS`, `CNS`, `LBAF`, `Get Log Page` 등 NVMe 고유 용어의 exact-match 검색이
핵심 요구사항이며, 이를 위해 dense + sparse 하이브리드 검색과 스펙 구조 중심의 계층적 청킹을 채택했습니다.

---

## 변경 파일 목록

```
nvme_rag/
├── core/
│   ├── __init__.py           # optional import 구조로 변경 + Qdrant 파이프라인 exports
│   ├── chunker.py            # 청크 타입 재정의, 메타데이터 확장, NOTE 감지
│   ├── pipeline.py           # LlamaIndex 파이프라인 (기존 유지, 메타데이터 수정)
│   └── qdrant_pipeline.py    # [신규] Qdrant 하이브리드 RAG 파이프라인
├── tests/
│   ├── test_chunker.py       # 테스트 전면 강화 (12개)
│   ├── test_real_format.py   # pytest 스타일로 전환 (8개)
│   └── test_qdrant_pipeline.py  # [신규] Qdrant 파이프라인 단위 테스트 (30개)
├── main.py                   # CLI 진입점
└── requirements.txt          # qdrant-client, fastembed, openai 추가
```

---

## 1. 청커 (`chunker.py`) — 스펙 중심 강화

### 청크 타입 재정의 (4종)

NVMe 문서 구조를 반영하여 청크 타입을 4종으로 명확히 정의했습니다.

| 타입 | 설명 | 용도 |
|------|------|------|
| `section` | 설명 문단 (command overview, 개념 설명) | 개념 질문 대응 |
| `table` | 구조적 표 (Identify Controller Data Structure, SMART log 등) | 구조형 데이터 검색 |
| `field` | 필드/레지스터 정의 (MPTR, PRP1, Bits XX:YY 형태) | exact question, 약어 정의 대응 |
| `note` | NOTE: / Warning: / Caution: 블록 | 제약조건·예외사항 질의 대응 |

기존 `register` 타입은 `field`로 통합되었으며, `Figure X:` 형태의 Bits/Description 컬럼 테이블도 `field`로 분류합니다.

### 신규 패턴

```python
# Bit X (단일 비트, 범위 없음)도 감지
_REGISTER_RE = re.compile(r"^Bits?\s+\d+(?::\d+)?\b", re.MULTILINE | re.IGNORECASE)

# "Bits  Description" 컬럼 헤더 형식 (NVMe Figure 테이블)
_FIELD_TABLE_HEADER_RE = re.compile(r"^Bits\s{2,}(?:Description|Type|Value)\s*$", ...)

# RFC 2119 규범적 언어 (shall/should/may/must)
_NORMATIVE_RE = re.compile(r"\b(shall(?: not)?|should(?: not)?|...)\b", re.IGNORECASE)

# NOTE: / Warning: / Caution: 블록
_NOTE_BLOCK_RE = re.compile(r"^(NOTE|Warning|Caution|IMPORTANT)\s*[:\-–—]", re.MULTILINE)
```

### 메타데이터 확장

모든 청크에 아래 필드가 추가되었습니다.

| 필드 | 설명 |
|------|------|
| `section_depth` | 섹션 계층 깊이 (정수, `"6.16.1"` → `3`) |
| `has_normative_language` | shall/should/may 포함 여부 플래그 |
| `table_number` | `str` → `int` 타입 변환 |

### Figure → Field 분류 로직

```
"Table X:" 캡션  → table 청크
"Figure X:" + "Bits  Description" 컬럼 헤더 → field 청크   ← NVMe 필드 정의
"Figure X:" + Bits 컬럼 없음                → table 청크
```

### NOTE 블록 독립 추출

기존에는 NOTE/Warning 문단이 section 텍스트에 묻혔습니다.
이제 단락 단위로 분리하여 `note` 청크로 독립 저장합니다.

```
1 Introduction
...
NOTE: If the host submits to a Full SQ, behavior is undefined.   ← note 청크
...
The controller shall process the command.                         ← section 청크 (has_normative_language=True)
```

> **설계 결정**: `shall/must` 문장을 모두 `note`로 분리하지 않는 이유 —
> "The controller **shall** process the command." 같은 문장은 section 문맥과 함께 이해해야 의미가 완전합니다.
> 명시적으로 레이블된 `NOTE:` / `Warning:` 블록만 독립 청크로 분리하고,
> 나머지는 `has_normative_language=True` 플래그로 필터 검색을 지원합니다.

---

## 2. Qdrant 파이프라인 (`qdrant_pipeline.py`) — [신규]

### 전체 데이터 흐름

```
PDF
 └─ NVMeChunker.chunk_pdf()          ← 계층적 청킹 (section/table/field/note)
     └─ MetadataEnricher.enrich()    ← 메타데이터 보강 + context_text 생성
         └─ embed_dense_batch()      ← OpenAI text-embedding-3-small
         └─ embed_sparse_batch()     ← FastEmbed BM25
             └─ NVMeQdrantIndexer    ← Qdrant 업서트

Query
 └─ embed_dense() + embed_sparse()
     └─ NVMeQdrantRetriever.search()
         ├─ dense search (cosine)
         ├─ sparse search (BM25)
         └─ RRF 융합
             └─ 부모 청크 일괄 조회
                 └─ [RetrievedChunk, ...] → LLM → NVMeAnswer
```

### Qdrant 컬렉션 스키마

```
Collection: nvme_spec
├── vectors
│   ├── dense:  VectorParams(size=1536, distance=COSINE, hnsw m=16)
│   └── sparse: SparseVectorParams(BM25, on_disk=False)
└── payload indexes
    ├── chunk_type    (KEYWORD)
    ├── section_number (KEYWORD)
    ├── doc_id        (KEYWORD)
    ├── command_name  (KEYWORD)
    └── page_start    (INTEGER)
```

### 메타데이터 페이로드 (23개 필드)

```python
NVMePointPayload(
    # 문서 식별
    doc_id, doc_title, version, spec_family,

    # 청크 식별
    chunk_id,          # 결정론적 UUID (재인덱싱 시 동일 ID 보장)
    chunk_type,        # section / table / field / note
    chunk_index,

    # 섹션 계층
    section_number,    # "6.16.1"
    section_title,     # "Copy – Command Dword 10"
    section_depth,     # 3
    subsection_number, # "6.16"  (직전 상위 섹션)
    subsection_title,  # "Copy Command"
    path,              # "6 NVM Command Set > 6.16 Copy Command > 6.16.1 ..."

    # 내용 특화
    command_name,      # "Copy Command"  (섹션 제목에서 자동 추출)
    table_id,          # 34
    table_title,       # "Copy – Command Dword 12"
    field_name,        # "Copy – Command Dword 12"
    field_aliases,     # ["FUA", "LR", "PRINFOW", "STCR"]  ← NVMe 약어

    # 위치
    page_start, page_end,

    # 계층 관계
    parent_chunk_id,   # 부모 section 청크의 UUID

    # 검색 보강
    keywords,          # ["FUA", "LR", "Copy", "Copy Command", ...]

    # 텍스트
    text,              # 원본 청크 텍스트
    context_text,      # prefix 붙은 임베딩·LLM용 텍스트
)
```

### 컨텍스트 Prefix (임베딩 + LLM용)

NVMe 약어 exact-match를 위해 `[Keywords]` 라인을 포함합니다.

```
[Document] NVM Express NVM Command Set Specification Rev 1.1
[Keywords] FUA, LR, PRINFOW, STCR, STCW
[Section] 6 NVM Command Set > 6.16 Copy Command > 6.16.3 Copy – Command Dword 12
[Field] Figure 34: Copy – Command Dword 12

Figure 34: Copy – Command Dword 12
Bits    Description
31      Limited Retry (LR): If this bit is set to '1', ...
30      Force Unit Access (FUA): ...
```

### 하이브리드 검색 — Reciprocal Rank Fusion

```python
# Dense 시맨틱 검색 (top_k * 2 후보)
dense_hits = client.search(query_vector=NamedVector("dense", embed_dense(query)), ...)

# Sparse BM25 키워드 검색 (top_k * 2 후보)
sparse_hits = client.search(query_vector=NamedSparseVector("sparse", bm25(query)), ...)

# RRF 융합: score(d) = Σ 1/(k + rank(d))
fused = _rrf([dense_hits, sparse_hits], k=60, top_k=top_k)
```

- **dense**: "NVMe write command flow" 같은 의미론적 질문에 강함
- **sparse (BM25)**: `MPTR`, `PRP1`, `OACS` 같은 약어 exact-match에 강함
- **RRF**: 두 결과 리스트를 순위 기반으로 합산, score 스케일 차이 무관

### 부모-자식 컨텍스트 재구성

검색된 청크의 부모를 일괄 조회(batch retrieve)하여 LLM 프롬프트에 추가합니다.

```
field 청크 검색됨  →  parent_chunk_id로 section 청크 조회  →  LLM 프롬프트에 포함
table 청크 검색됨  →  parent_chunk_id로 section 청크 조회  →  LLM 프롬프트에 포함
```

```python
# LLM 프롬프트 예시
[Parent: §6.16 Copy Command]
The Copy command copies data to a destination location...

[Field] Figure 34: Copy – Command Dword 12
...
```

### 소스 인용

```python
chunk.citation()
# → "NVM Express NVM Command Set Spec Rev 1.1, §6.16.3 Copy – Command Dword 12 (pp.30–31)"

answer.formatted()
# → "<답변 텍스트>\n\nSources:\n  [1] NVM Express ..., §6.16.3 ... (pp.30–31)"
```

---

## 3. 테스트 현황

| 파일 | 테스트 수 | 주요 검증 항목 |
|------|-----------|---------------|
| `test_chunker.py` | 12개 | section/table/field/note 분류, NOTE 감지, section_depth, has_normative_language, table_number int 타입 |
| `test_real_format.py` | 8개 | NVMe NVM Command Set Spec 1.1 실제 형식, Figure→field 분류, 섹션 계층, 메타데이터 완전성 |
| `test_qdrant_pipeline.py` | 30개 | chunk_id 결정론적 UUID, 커맨드/필드/약어 추출, path 생성, context prefix, RRF 융합, MetadataEnricher 전체 |

**총 50 passed, 0 failed** (Qdrant / OpenAI / fastembed 없이 순수 Python 로직만 테스트)

---

## 4. 의존성 추가

```
# Qdrant RAG 파이프라인
qdrant-client>=1.9.0     # 벡터 DB (dense + sparse 컬렉션)
openai>=1.30.0           # dense 임베딩 + LLM 답변 생성
fastembed>=0.3.0         # sparse BM25 임베딩
```

모든 의존성은 optional import로 처리되어, 청커 단독 사용 시 미설치 패키지가 있어도 동작합니다.

---

## 5. 사용 예시

```python
from nvme_rag.core.qdrant_pipeline import NVMeQdrantPipeline

pipeline = NVMeQdrantPipeline(
    qdrant_url="http://localhost:6333",
    openai_api_key="sk-...",
    dense_model="text-embedding-3-small",
)

# 인덱싱
pipeline.build_index(
    "NVM_Express_NVM_Command_Set_Spec_1.1.pdf",
    doc_title="NVM Express NVM Command Set Specification",
    version="1.1",
    spec_family="NVM Command Set",
)

# 하이브리드 검색
results = pipeline.search("What is the FUA bit in Copy command?", top_k=5)
for r in results:
    print(r.citation())
    print(r.context_for_llm())

# 답변 생성 (LLM)
answer = pipeline.answer("What does OACS bit 3 indicate?")
print(answer.formatted())
```
