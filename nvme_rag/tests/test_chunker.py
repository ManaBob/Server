"""
NVMe Chunker 단위 테스트
------------------------
실제 PDF 없이 텍스트 기반으로 청킹 로직을 검증합니다.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.chunker import NVMeChunker, _parent_section, _is_register_block


# ---------------------------------------------------------------------------
# 샘플 텍스트 (실제 NVMe 스펙 형식 모사)
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

2.1 NVM Express Architecture

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


# ---------------------------------------------------------------------------
# 테스트
# ---------------------------------------------------------------------------

def test_basic_chunking():
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(SAMPLE_TEXT)
    assert len(chunks) > 0, "청크가 생성되어야 합니다"
    print(f"  총 {len(chunks)}개 청크 생성")


def test_section_numbers():
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    section_numbers = {c.section_number for c in chunks}
    print(f"  발견된 섹션: {sorted(section_numbers)}")

    assert "1" in section_numbers or "1.1" in section_numbers, \
        "섹션 1 또는 1.1이 존재해야 합니다"
    assert "2.1" in section_numbers or "2" in section_numbers, \
        "섹션 2.x가 존재해야 합니다"


def test_table_detection():
    chunker = NVMeChunker(max_chunk_size=2000, keep_tables_intact=True)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    table_chunks = [c for c in chunks if c.chunk_type == "table"]
    print(f"  테이블 청크: {len(table_chunks)}개")
    assert len(table_chunks) >= 1, "테이블이 1개 이상 감지되어야 합니다"

    tbl = table_chunks[0]
    assert "table_caption" in tbl.metadata, "테이블 메타데이터가 있어야 합니다"
    print(f"  테이블 캡션: {tbl.metadata.get('table_caption')}")


def test_register_detection():
    chunker = NVMeChunker(max_chunk_size=2000)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    reg_chunks = [c for c in chunks if c.chunk_type == "register"]
    print(f"  레지스터 청크: {len(reg_chunks)}개")


def test_parent_section():
    assert _parent_section("3.1.2") == "3.1"
    assert _parent_section("3.1") == "3"
    assert _parent_section("3") == ""
    print("  _parent_section() OK")


def test_is_register_block():
    assert _is_register_block("Bits 31:16 (MQES): Maximum Queue Entries")
    assert _is_register_block("Bits 7:0 Reserved")
    assert not _is_register_block("This is a normal paragraph.")
    print("  _is_register_block() OK")


def test_chunk_size_limit():
    """청크가 max_chunk_size를 크게 초과하지 않는지 확인합니다."""
    max_size = 300
    chunker = NVMeChunker(max_chunk_size=max_size, overlap_size=50)
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    oversized = [c for c in chunks if len(c.text) > max_size * 2]
    print(f"  max_size={max_size}, 2배 초과 청크: {len(oversized)}개")
    # overlap이 있으므로 약간 초과 허용, 심하게 초과하는 건 없어야 함
    assert len(oversized) == 0, f"심하게 큰 청크가 있습니다: {[len(c.text) for c in oversized]}"


def test_metadata_completeness():
    """모든 청크에 필수 메타데이터가 있는지 확인합니다."""
    chunker = NVMeChunker()
    chunks = chunker.chunk_text(SAMPLE_TEXT)

    for c in chunks:
        meta = c.to_metadata()
        for key in ["chunk_type", "section_number", "section_title", "page_start"]:
            assert key in meta, f"메타데이터 누락: {key}"
    print(f"  {len(chunks)}개 청크 메타데이터 완전성 OK")


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
        test_register_detection,
        test_chunk_size_limit,
        test_metadata_completeness,
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
