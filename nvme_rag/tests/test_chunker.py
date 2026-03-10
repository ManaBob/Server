"""
NVMe Chunker 단위 테스트
------------------------
실제 PDF 없이 텍스트 기반으로 청킹 로직을 검증합니다.

청크 타입 (4종):
  section - 설명 문단
  table   - 구조적 표 ("Table X:" 캡션)
  field   - 필드/레지스터 정의 (Bits XX:YY, 또는 Figure + Bits 컬럼)
  note    - NOTE: / Warning: / Caution: 블록
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.chunker import NVMeChunker, _parent_section, _is_register_block


# ---------------------------------------------------------------------------
# 샘플 텍스트
# ---------------------------------------------------------------------------

SAMPLE_TEXT = """\
1 Introduction

NVM Express (NVMe) is a scalable host controller interface designed to address
the needs of Enterprise and Client solid state drives.

1.1 Scope

This specification defines the NVM Express Base Specification.

1.1.1 Referenced Documents

The following documents are referenced in this specification.

2 Theory of Operation

2.1 NVMe Express Architecture

The NVM Express interface is accessed over a PCI Express bus.

Table 1: NVM Express Registers
Offset  Size  Name
0h      4     Controller Capabilities (CAP)
8h      4     Version (VS)
Ch      4     Interrupt Mask Set (INTMS)

2.1.1 Queue Model

Submissions and completions use separate queues.

Bits 31:16 (MQES): Maximum Queue Entries Supported
  This field is a 0-based value and defines the maximum
  individual queue size that the controller supports.

Bits 15:14 Reserved
Bits 13:12 (CQR): Contiguous Queues Required

3 NVM Command Set

3.1 Read Command

The Read command reads data from the NVM subsystem.
"""

# NOTE / Warning 블록과 normative language가 포함된 텍스트
NOTE_TEXT = """\
1 Commands

The host shall submit commands using the Submission Queue.

NOTE: If the host submits a command to a Full Submission Queue, the behavior is undefined.

2 Completion

The controller should signal completion within the time limit.

WARNING: Exceeding the time limit may result in undefined behavior.

The controller may delay completion under certain conditions.
"""


# ---------------------------------------------------------------------------
# 테스트
# ---------------------------------------------------------------------------

def test_basic_chunking():
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(SAMPLE_TEXT)
    assert len(chunks) > 0, "청크가 생성되어야 합니다"
    print(f"  총 {len(chunks)}개 청크 생성")


def test_section_numbers():
    """감지된 섹션 번호가 샘플 텍스트의 모든 섹션을 포함해야 합니다."""
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    section_numbers = {c.section_number for c in chunks}
    print(f"  발견된 섹션: {sorted(section_numbers)}")

    expected = {"1", "1.1", "1.1.1", "2", "2.1", "2.1.1", "3", "3.1"}
    for sec in expected:
        assert sec in section_numbers, f"섹션 {sec!r}이 감지되지 않았습니다"


def test_table_detection():
    """'Table X:' 캡션 → table 청크로 분류됩니다."""
    chunker = NVMeChunker(max_chunk_size=2000, keep_tables_intact=True)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    table_chunks = [c for c in chunks if c.chunk_type == "table"]
    print(f"  table 청크: {len(table_chunks)}개")
    assert len(table_chunks) >= 1, "table 청크가 1개 이상 있어야 합니다"

    tbl = table_chunks[0]
    assert "table_caption" in tbl.metadata
    assert "table_number" in tbl.metadata
    print(f"  캡션: {tbl.metadata.get('table_caption')}")


def test_field_detection():
    """'Bits XX:YY' 패턴 → field 청크로 분류됩니다."""
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    field_chunks = [c for c in chunks if c.chunk_type == "field"]
    print(f"  field 청크: {len(field_chunks)}개")
    assert len(field_chunks) >= 1, \
        "SAMPLE_TEXT에 'Bits XX:YY' 패턴이 있으므로 field 청크가 1개 이상 있어야 합니다"


def test_note_chunk_detection():
    """NOTE: / WARNING: 단락 → note 청크로 분류됩니다."""
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(NOTE_TEXT)

    note_chunks = [c for c in chunks if c.chunk_type == "note"]
    print(f"  note 청크: {len(note_chunks)}개 → {[c.text[:50] for c in note_chunks]}")
    assert len(note_chunks) >= 2, \
        "NOTE_TEXT에 NOTE: 와 WARNING: 블록이 있으므로 note 청크가 2개 이상 있어야 합니다"

    texts = " ".join(c.text for c in note_chunks)
    assert "NOTE" in texts or "WARNING" in texts, \
        "note 청크 텍스트에 NOTE 또는 WARNING 키워드가 있어야 합니다"


def test_parent_section():
    assert _parent_section("3.1.2") == "3.1"
    assert _parent_section("3.1") == "3"
    assert _parent_section("3") == ""
    print("  _parent_section() OK")


def test_is_register_block():
    # 범위 형태 (Bits X:Y)
    assert _is_register_block("Bits 31:16 (MQES): Maximum Queue Entries")
    assert _is_register_block("Bits 7:0 Reserved")
    # 단일 비트 형태 (Bit X, Bits X)
    assert _is_register_block("Bit 7 (LR): Limited Retry")
    assert _is_register_block("Bits 0 Reserved")
    # 일반 텍스트는 감지되지 않아야 함
    assert not _is_register_block("This is a normal paragraph.")
    assert not _is_register_block("The controller shall process the command.")
    print("  _is_register_block() OK")


def test_chunk_size_limit():
    """청크가 max_chunk_size를 크게 초과하지 않는지 확인합니다."""
    max_size = 300
    chunker = NVMeChunker(max_chunk_size=max_size, overlap_size=50)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    oversized = [c for c in chunks if len(c.text) > max_size * 2]
    print(f"  max_size={max_size}, 2배 초과 청크: {len(oversized)}개")
    assert len(oversized) == 0, f"심하게 큰 청크가 있습니다: {[len(c.text) for c in oversized]}"


def test_metadata_completeness():
    """모든 청크에 필수 메타데이터가 있는지 확인합니다."""
    chunker = NVMeChunker()
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    required_keys = [
        "chunk_type", "section_number", "section_title",
        "section_depth", "parent_section", "page_start",
    ]
    valid_chunk_types = {"section", "table", "field", "note"}

    for c in chunks:
        meta = c.to_metadata()
        for key in required_keys:
            assert key in meta, f"청크 #{c.chunk_index} 메타데이터 누락: {key!r}"
        assert meta["chunk_type"] in valid_chunk_types, \
            f"청크 #{c.chunk_index} chunk_type 값 이상: {meta['chunk_type']!r}"

    print(f"  {len(chunks)}개 청크 메타데이터 완전성 OK")


def test_section_depth():
    """section_depth가 섹션 번호 계층에 맞게 설정됩니다."""
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    meta_by_section = {c.section_number: c.to_metadata() for c in chunks}

    assert meta_by_section["1"]["section_depth"] == 1, "최상위 섹션은 depth=1"
    assert meta_by_section["1.1"]["section_depth"] == 2, "2단계 섹션은 depth=2"
    assert meta_by_section["1.1.1"]["section_depth"] == 3, "3단계 섹션은 depth=3"
    assert meta_by_section["3.1"]["section_depth"] == 2
    print("  section_depth OK")


def test_normative_language():
    """shall/should/may 포함 청크에 has_normative_language=True 플래그가 설정됩니다."""
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(NOTE_TEXT)

    normative_chunks = [c for c in chunks if c.metadata.get("has_normative_language")]
    assert len(normative_chunks) >= 1, \
        "shall/should/may 포함 청크에 has_normative_language=True가 설정되어야 합니다"
    print(f"  normative 청크: {len(normative_chunks)}개")


def test_table_number_type():
    """table_number 메타데이터가 int 타입이어야 합니다."""
    chunker = NVMeChunker(max_chunk_size=2000, keep_tables_intact=True)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    table_chunks = [c for c in chunks if c.chunk_type == "table"]
    assert len(table_chunks) >= 1, "table 청크가 있어야 합니다"

    for t in table_chunks:
        num = t.metadata.get("table_number")
        assert isinstance(num, int), \
            f"table_number가 int가 아닙니다: {num!r} (type={type(num).__name__})"
    print(f"  table_number int 타입 OK ({len(table_chunks)}개)")


# ---------------------------------------------------------------------------
# 실행
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_parent_section,
        test_is_register_block,
        test_basic_chunking,
        test_section_numbers,
        test_table_detection,
        test_field_detection,
        test_note_chunk_detection,
        test_chunk_size_limit,
        test_metadata_completeness,
        test_section_depth,
        test_normative_language,
        test_table_number_type,
    ]

    passed = 0
    failed = 0
    for test_fn in tests:
        try:
            print(f"\n[RUN] {test_fn.__name__}")
            test_fn()
            print(f"[PASS] {test_fn.__name__}")
            passed += 1
        except Exception as e:
            print(f"[FAIL] {test_fn.__name__}: {e}")
            failed += 1

    print(f"\n결과: {passed} passed, {failed} failed")
