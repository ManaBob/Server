"""
NVMe Qdrant Pipeline 단위 테스트
----------------------------------
Qdrant / OpenAI / fastembed 없이 순수 Python 로직만 테스트합니다.
  - _chunk_id, _extract_command_name, _extract_field_info
  - _build_path, _build_context_text, _rrf
  - MetadataEnricher.enrich()
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.chunker import NVMeChunk
from core.qdrant_pipeline import (
    _chunk_id,
    _extract_command_name,
    _extract_field_info,
    _extract_keywords,
    _build_path,
    _build_context_text,
    _rrf,
    MetadataEnricher,
    NVMePointPayload,
    RetrievedChunk,
)


# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

def _make_chunk(
    text="Some text.",
    chunk_type="section",
    section_number="6.16",
    section_title="Copy Command",
    parent_section="6",
    chunk_index=0,
    metadata=None,
) -> NVMeChunk:
    return NVMeChunk(
        text=text,
        chunk_type=chunk_type,
        section_number=section_number,
        section_title=section_title,
        parent_section=parent_section,
        page_start=30,
        page_end=31,
        chunk_index=chunk_index,
        metadata=metadata or {},
    )


# ---------------------------------------------------------------------------
# _chunk_id
# ---------------------------------------------------------------------------

def test_chunk_id_deterministic():
    """같은 입력으로 항상 같은 ID 생성."""
    assert _chunk_id("doc1", 0) == _chunk_id("doc1", 0)
    assert _chunk_id("doc1", 0) != _chunk_id("doc1", 1)
    assert _chunk_id("doc1", 0) != _chunk_id("doc2", 0)
    print("  _chunk_id 결정론적 OK")


def test_chunk_id_uuid_format():
    """UUID 형식 문자열 반환 (8-4-4-4-12)."""
    import re
    cid = _chunk_id("test", 0)
    assert re.match(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", cid)
    print(f"  UUID: {cid}")


# ---------------------------------------------------------------------------
# _extract_command_name
# ---------------------------------------------------------------------------

def test_extract_command_name_basic():
    assert _extract_command_name("Copy Command") == "Copy Command"
    assert _extract_command_name("Read Command") == "Read Command"
    assert _extract_command_name("Get Log Page Command") == "Get Log Page Command"
    assert _extract_command_name("Identify Command") == "Identify Command"
    print("  _extract_command_name 기본 OK")


def test_extract_command_name_subsection():
    """서브섹션 제목에서는 빈 문자열 반환."""
    assert _extract_command_name("Copy – Command Dword 10") == ""
    assert _extract_command_name("Command Dword 12") == ""
    assert _extract_command_name("Dataset Management") == ""
    print("  _extract_command_name 서브섹션 필터링 OK")


# ---------------------------------------------------------------------------
# _extract_field_info
# ---------------------------------------------------------------------------

def test_extract_field_info_with_caption():
    """table_caption이 있으면 field_name으로 사용."""
    field_name, aliases = _extract_field_info(
        "Some field text with (FUA) and (LR) definitions.",
        table_caption="Copy – Command Dword 12",
    )
    assert field_name == "Copy – Command Dword 12"
    assert "FUA" in aliases
    assert "LR" in aliases
    print(f"  field_name={field_name!r}, aliases={aliases}")


def test_extract_field_info_aliases():
    """괄호 약어 (대문자 2~10자) 추출."""
    text = """
31      Limited Retry (LR): If this bit is set...
30      Force Unit Access (FUA): ...
29:26   Protection Information Write (PRINFOW): ...
"""
    _, aliases = _extract_field_info(text)
    assert "LR" in aliases
    assert "FUA" in aliases
    assert "PRINFOW" in aliases
    print(f"  aliases={aliases}")


def test_extract_field_info_no_noise():
    """일반 텍스트에서 약어 오탐 없음."""
    text = "The controller shall process the command within the time limit."
    field_name, aliases = _extract_field_info(text)
    # 일반 텍스트에는 괄호 약어가 없음
    assert len(aliases) == 0
    print("  약어 오탐 없음 OK")


# ---------------------------------------------------------------------------
# _build_path
# ---------------------------------------------------------------------------

def test_build_path_multilevel():
    section_map = {
        "6": "NVM Command Set",
        "6.16": "Copy Command",
        "6.16.1": "Copy – Command Dword 10",
    }
    path = _build_path("6.16.1", section_map)
    assert "6 NVM Command Set" in path
    assert "6.16 Copy Command" in path
    assert "6.16.1" in path
    assert ">" in path
    print(f"  path={path!r}")


def test_build_path_toplevel():
    section_map = {"6": "NVM Command Set"}
    path = _build_path("6", section_map)
    assert "6" in path
    assert "NVM Command Set" in path
    print(f"  toplevel path={path!r}")


def test_build_path_missing_parent():
    """중간 섹션이 section_map에 없어도 오류 없음."""
    section_map = {"6.16.1": "Copy – Command Dword 10"}
    path = _build_path("6.16.1", section_map)
    assert "6.16.1" in path
    print(f"  missing parent path={path!r}")


# ---------------------------------------------------------------------------
# _build_context_text
# ---------------------------------------------------------------------------

def test_context_text_section():
    chunk = _make_chunk(text="The Copy command copies data.", chunk_type="section")
    ctx = _build_context_text(chunk, "NVMe Spec Rev 1.1", "6 > 6.16 Copy Command", [])
    assert "[Document] NVMe Spec Rev 1.1" in ctx
    assert "[Section]" in ctx
    assert "The Copy command" in ctx
    print("  section context_text OK")


def test_context_text_field():
    chunk = _make_chunk(
        text="Bits    Description\n31 LR: ...",
        chunk_type="field",
        metadata={"table_type": "Figure", "table_number": 34, "table_caption": "Copy – Dword 12"},
    )
    ctx = _build_context_text(chunk, "NVMe Spec", "6 > 6.16.3", ["LR", "FUA"])
    assert "[Document]" in ctx
    assert "[Keywords] LR, FUA" in ctx
    assert "[Field] Figure 34: Copy – Dword 12" in ctx
    print("  field context_text OK")


def test_context_text_table():
    chunk = _make_chunk(
        text="Offset  Size  Name\n0h  4  CAP",
        chunk_type="table",
        metadata={"table_type": "Table", "table_number": 1, "table_caption": "NVM Express Registers"},
    )
    ctx = _build_context_text(chunk, "NVMe Spec", "2 > 2.1", [])
    assert "[Table] Table 1: NVM Express Registers" in ctx
    print("  table context_text OK")


def test_context_text_note():
    chunk = _make_chunk(text="NOTE: Behavior is undefined.", chunk_type="note")
    ctx = _build_context_text(chunk, "NVMe Spec", "3 > 3.1", [])
    assert "[Note/Requirement]" in ctx
    assert "NOTE: Behavior is undefined." in ctx
    print("  note context_text OK")


# ---------------------------------------------------------------------------
# _rrf
# ---------------------------------------------------------------------------

def test_rrf_fusion():
    """RRF가 양쪽 결과에 모두 나타난 항목을 상위에 배치합니다."""
    class MockHit:
        def __init__(self, id_): self.id = id_

    results_a = [MockHit(1), MockHit(2), MockHit(3)]
    results_b = [MockHit(2), MockHit(1), MockHit(4)]
    fused = _rrf([results_a, results_b], top_k=3)

    assert len(fused) <= 3
    ids = [str(h.id) for h in fused]
    # ID 1, 2는 양쪽에 있으므로 상위 2개여야 함
    assert "1" in ids[:2] or "2" in ids[:2]
    print(f"  RRF 융합 순서: {ids}")


def test_rrf_dedup():
    """같은 ID가 두 리스트에 있어도 최종 결과에 중복 없음."""
    class MockHit:
        def __init__(self, id_): self.id = id_

    results = [MockHit(i) for i in range(5)]
    fused = _rrf([results, results], top_k=5)
    ids = [str(h.id) for h in fused]
    assert len(ids) == len(set(ids)), "RRF 결과에 중복이 있습니다"
    print("  RRF 중복 제거 OK")


def test_rrf_top_k():
    """top_k 개수만 반환합니다."""
    class MockHit:
        def __init__(self, id_): self.id = id_

    results = [MockHit(i) for i in range(20)]
    fused = _rrf([results], top_k=5)
    assert len(fused) == 5
    print("  RRF top_k=5 OK")


# ---------------------------------------------------------------------------
# MetadataEnricher
# ---------------------------------------------------------------------------

def _sample_chunks() -> list[NVMeChunk]:
    """MetadataEnricher 테스트용 청크 목록."""
    return [
        NVMeChunk(
            text="The NVM Command Set defines commands for NVM storage operations.",
            chunk_type="section",
            section_number="6",
            section_title="NVM Command Set",
            parent_section="",
            page_start=1, page_end=1, chunk_index=0,
        ),
        NVMeChunk(
            text="The Copy command copies data to a destination location.",
            chunk_type="section",
            section_number="6.16",
            section_title="Copy Command",
            parent_section="6",
            page_start=30, page_end=30, chunk_index=1,
        ),
        NVMeChunk(
            text="Figure 31: Copy – Command Dword 10\nBits    Description\n31:30   Reserved\n29:25   NR: ...",
            chunk_type="field",
            section_number="6.16.1",
            section_title="Copy – Command Dword 10",
            parent_section="6.16",
            page_start=30, page_end=30, chunk_index=2,
            metadata={
                "table_type": "Figure", "table_number": 31,
                "table_caption": "Copy – Command Dword 10",
                "has_normative_language": False,
            },
        ),
        NVMeChunk(
            text="Table 1: NVM Express Registers\nOffset  Size  Name\n0h  4  CAP",
            chunk_type="table",
            section_number="2.1",
            section_title="NVMe Architecture",
            parent_section="2",
            page_start=5, page_end=5, chunk_index=3,
            metadata={
                "table_type": "Table", "table_number": 1,
                "table_caption": "NVM Express Registers",
                "has_normative_language": False,
            },
        ),
        NVMeChunk(
            text="NOTE: If the host submits to a Full SQ, behavior is undefined.",
            chunk_type="note",
            section_number="6.16",
            section_title="Copy Command",
            parent_section="6",
            page_start=30, page_end=30, chunk_index=4,
        ),
    ]


def test_enricher_basic():
    """MetadataEnricher가 올바른 수의 페이로드를 생성합니다."""
    chunks = _sample_chunks()
    enricher = MetadataEnricher("doc1", "NVMe Spec Rev 1.1", "1.1", "NVM Command Set")
    payloads = enricher.enrich(chunks)

    assert len(payloads) == len(chunks)
    print(f"  {len(payloads)}개 페이로드 생성 OK")


def test_enricher_doc_fields():
    """doc_id, doc_title, version, spec_family가 올바르게 설정됩니다."""
    payloads = MetadataEnricher("docX", "NVMe Spec", "2.0", "Base").enrich(_sample_chunks())
    for p in payloads:
        assert p.doc_id == "docX"
        assert p.doc_title == "NVMe Spec"
        assert p.version == "2.0"
        assert p.spec_family == "Base"
    print("  doc 필드 OK")


def test_enricher_chunk_ids_unique():
    """모든 청크 ID가 고유해야 합니다."""
    payloads = MetadataEnricher("d1", "Spec").enrich(_sample_chunks())
    ids = [p.chunk_id for p in payloads]
    assert len(ids) == len(set(ids)), "chunk_id 중복 발견"
    print(f"  chunk_id 고유성 OK ({len(ids)}개)")


def test_enricher_section_depth():
    """section_depth가 섹션 번호 계층에 맞습니다."""
    payloads = MetadataEnricher("d1", "Spec").enrich(_sample_chunks())
    meta = {p.section_number: p for p in payloads}

    assert meta["6"].section_depth == 1
    assert meta["6.16"].section_depth == 2
    assert meta["6.16.1"].section_depth == 3
    assert meta["2.1"].section_depth == 2
    print("  section_depth OK")


def test_enricher_path():
    """path에 섹션 계층이 포함됩니다."""
    payloads = MetadataEnricher("d1", "Spec").enrich(_sample_chunks())
    p = next(p for p in payloads if p.section_number == "6.16.1")

    assert "6" in p.path
    assert "6.16" in p.path
    assert "6.16.1" in p.path
    print(f"  path={p.path!r}")


def test_enricher_subsection():
    """subsection_number가 직전 상위 섹션입니다."""
    payloads = MetadataEnricher("d1", "Spec").enrich(_sample_chunks())
    meta = {p.section_number: p for p in payloads}

    assert meta["6.16"].subsection_number == "6"
    assert meta["6.16.1"].subsection_number == "6.16"
    assert meta["6"].subsection_number == ""
    print("  subsection_number OK")


def test_enricher_command_name():
    """커맨드 이름이 섹션 제목에서 추출됩니다."""
    payloads = MetadataEnricher("d1", "Spec").enrich(_sample_chunks())
    meta = {p.section_number: p for p in payloads}

    assert meta["6.16"].command_name == "Copy Command"
    # 서브섹션 "6.16.1"은 "Copy – Command Dword 10"에서 직접 추출 실패 →
    # 부모 "6.16"에서 "Copy Command" 상속
    assert meta["6.16.1"].command_name == "Copy Command"
    print("  command_name OK")


def test_enricher_field_metadata():
    """field 청크의 table_id, table_title, field_aliases가 설정됩니다."""
    payloads = MetadataEnricher("d1", "Spec").enrich(_sample_chunks())
    field_p = next(p for p in payloads if p.chunk_type == "field")

    assert field_p.table_id == 31
    assert "Copy" in field_p.table_title
    assert field_p.field_name != ""
    print(f"  field: table_id={field_p.table_id}, field_name={field_p.field_name!r}")


def test_enricher_parent_chunk_id():
    """field/table/note 청크의 parent_chunk_id가 같은 섹션의 section 청크 ID입니다."""
    payloads = MetadataEnricher("d1", "Spec").enrich(_sample_chunks())
    pid_map = {p.section_number: p.chunk_id for p in payloads if p.chunk_type == "section"}

    for p in payloads:
        if p.chunk_type in ("field", "table", "note"):
            expected_parent = pid_map.get(p.section_number, "")
            assert p.parent_chunk_id == expected_parent, (
                f"{p.chunk_type} #{p.chunk_index}: "
                f"parent_chunk_id={p.parent_chunk_id!r} != {expected_parent!r}"
            )
    print("  parent_chunk_id OK")


def test_enricher_context_text_prefixes():
    """context_text에 [Document], [Section], 타입별 prefix가 포함됩니다."""
    payloads = MetadataEnricher("d1", "NVMe Spec Rev 1.1").enrich(_sample_chunks())

    for p in payloads:
        assert "[Document] NVMe Spec Rev 1.1" in p.context_text, \
            f"chunk #{p.chunk_index}: [Document] 없음"
        assert "[Section]" in p.context_text, \
            f"chunk #{p.chunk_index}: [Section] 없음"

    field_p = next(p for p in payloads if p.chunk_type == "field")
    assert "[Field]" in field_p.context_text

    table_p = next(p for p in payloads if p.chunk_type == "table")
    assert "[Table]" in table_p.context_text

    note_p = next(p for p in payloads if p.chunk_type == "note")
    assert "[Note/Requirement]" in note_p.context_text

    print("  context_text prefix OK")


def test_enricher_to_dict():
    """to_dict()이 모든 필수 키를 포함합니다."""
    payloads = MetadataEnricher("d1", "Spec").enrich(_sample_chunks())
    required = [
        "doc_id", "doc_title", "version", "spec_family",
        "chunk_id", "chunk_type", "section_number", "section_title",
        "section_depth", "subsection_number", "subsection_title",
        "path", "command_name", "table_id", "table_title",
        "field_name", "field_aliases", "page_start", "page_end",
        "parent_chunk_id", "keywords", "text", "context_text",
    ]
    for p in payloads:
        d = p.to_dict()
        missing = [k for k in required if k not in d]
        assert not missing, f"chunk #{p.chunk_index}: 누락 키={missing}"
    print(f"  to_dict() {len(required)}개 키 완전성 OK")


# ---------------------------------------------------------------------------
# RetrievedChunk
# ---------------------------------------------------------------------------

def test_citation_format():
    """citation()이 문서 제목, 섹션, 페이지를 포함합니다."""
    p = MetadataEnricher("d1", "NVMe Spec Rev 1.1", "1.1", "Base").enrich(_sample_chunks())[1]
    chunk = RetrievedChunk(payload=p, score=0.9)

    cit = chunk.citation()
    assert "NVMe Spec Rev 1.1" in cit
    assert "6.16" in cit
    assert "p." in cit or "pp." in cit
    print(f"  citation={cit!r}")


def test_context_for_llm_with_parent():
    """부모가 있으면 context_for_llm에 Parent 섹션이 포함됩니다."""
    payloads = MetadataEnricher("d1", "NVMe Spec").enrich(_sample_chunks())

    field_p = next(p for p in payloads if p.chunk_type == "field")
    parent_p = next(
        (p for p in payloads
         if p.chunk_type == "section" and p.section_number == field_p.section_number),
        None,
    )

    chunk = RetrievedChunk(payload=field_p, score=0.8, parent_payload=parent_p)
    llm_ctx = chunk.context_for_llm()

    if parent_p:
        assert "[Parent:" in llm_ctx
    assert field_p.context_text in llm_ctx
    print("  context_for_llm OK")


# ---------------------------------------------------------------------------
# 실행
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_chunk_id_deterministic,
        test_chunk_id_uuid_format,
        test_extract_command_name_basic,
        test_extract_command_name_subsection,
        test_extract_field_info_with_caption,
        test_extract_field_info_aliases,
        test_extract_field_info_no_noise,
        test_build_path_multilevel,
        test_build_path_toplevel,
        test_build_path_missing_parent,
        test_context_text_section,
        test_context_text_field,
        test_context_text_table,
        test_context_text_note,
        test_rrf_fusion,
        test_rrf_dedup,
        test_rrf_top_k,
        test_enricher_basic,
        test_enricher_doc_fields,
        test_enricher_chunk_ids_unique,
        test_enricher_section_depth,
        test_enricher_path,
        test_enricher_subsection,
        test_enricher_command_name,
        test_enricher_field_metadata,
        test_enricher_parent_chunk_id,
        test_enricher_context_text_prefixes,
        test_enricher_to_dict,
        test_citation_format,
        test_context_for_llm_with_parent,
    ]

    passed = failed = 0
    for fn in tests:
        try:
            print(f"\n[RUN] {fn.__name__}")
            fn()
            print(f"[PASS] {fn.__name__}")
            passed += 1
        except Exception as e:
            print(f"[FAIL] {fn.__name__}: {e}")
            import traceback; traceback.print_exc()
            failed += 1

    print(f"\n결과: {passed} passed, {failed} failed")
