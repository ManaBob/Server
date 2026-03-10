"""
실제 NVMe 스펙 형식 기반 테스트
-----------------------------------
스크린샷에서 확인된 NVM Command Set Spec 1.1 (p.30) 형식을 그대로 사용.
- Figure 34: Copy – Command Dword 12 (Bits 테이블 + 중첩 테이블)
- Figure 35: Copy – Command Dword 13
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.chunker import NVMeChunker

# ---------------------------------------------------------------------------
# 실제 스펙과 동일한 형식의 샘플 텍스트
# ---------------------------------------------------------------------------

NVME_NVM_CMD_SPEC_SAMPLE = """\
NVM Express® NVM Command Set Specification, Revision 1.1

6 NVM Command Set

6.16 Copy Command

The Copy command copies data to a destination location specified in the command from source locations
specified by source range entries in the data buffer.

6.16.1 Copy – Command Dword 10

The fields defined in this command are specified in Figure 31.

Figure 31: Copy – Command Dword 10
Bits    Description
31:30   Reserved
29:25   Number of Ranges (NR): Specifies the number of Source Range entries that are specified in the command. This is a 0s-based value.
24:00   Starting LBA (SLBA) (lower 25 bits): Specifies bits 24:00 of the 64-bit Starting LBA of the destination.

6.16.2 Copy – Command Dword 11

Figure 32: Copy – Command Dword 11
Bits    Description
31:16   Starting LBA (SLBA) (upper 16 bits): Specifies bits 47:32 of the 64-bit Starting LBA of the destination.
15:00   Reserved

6.16.3 Copy – Command Dword 12

The fields defined in this command are specified in Figure 34.

Figure 34: Copy – Command Dword 12
Bits    Description
31      Limited Retry (LR): If this bit is set to '1', then the controller should apply limited retry efforts for the write portion of the copy operation. If this bit is cleared to '0', then the controller should apply all available error recovery means to write the data to the NVM.
30      Force Unit Access (FUA): If this bit is set to '1', then for data and metadata, if any, associated with logical blocks specified by the write portion of the copy operation, the controller shall write that data and metadata, if any, to non-volatile medium before indicating command completion. There is no implied ordering with other commands. If this bit is cleared to '0', then this bit has no effect.
29:26   Protection Information Write (PRINFOW): Specifies the protection information action and check field, as defined in Figure 11, to be used for the write portion of the copy operation.
25      Storage Tag Check Read (STCR): This bit specifies the Storage Tag field shall be checked as part of end-to-end data protection processing as defined in Figure 12, to be used for the read portion of the copy operation. If the Storage Tag Check Read Support (STCRS) bit (refer to Figure 111) is cleared to '0', then this bit is reserved.
24      Storage Tag Check Write (STCW): This bit specifies the Storage Tag field shall be checked as part of end-to-end data protection processing as defined in Figure 12, to be used for the write portion of the copy operation.
23:20   Directive Type (DTYPE): Specifies the Directive Type associated with the Directive Specific field (refer to the Directives section in the NVM Express Base Specification) used for the write portion of the copy operation.
19:16   Command Extension Type (CETYPE): Specifies the Command Extension Type that applies to the command (refer to the Key Per I/O section in the NVM Express Base Specification). This field is used for the write portion of the copy operation.
15:12   Protection Information Read (PRINFOR): Specifies the protection information action and check field, as defined in Figure 11, to be used for the read portion of the copy operation specified by each Source Range entries.
11:08   Descriptor Format (DESFMT): Specifies the type of the Copy Descriptor Format that is used. The Copy Descriptor Format specifies the starting location, length, and parameters associated with the read portion of the operation.

        Code Descriptor Format Type  Definition
        0h                           Source Range Entries Copy Descriptor Format 0h is used (refer to Figure 39).
        1h                           Source Range Entries Copy Descriptor Format 1h is used (refer to Figure 40).
        2h                           Source Range Entries Copy Descriptor Format 2h is used (refer to Figure 39).
        3h                           Source Range Entries Copy Descriptor Format 3h is used (refer to Figure 40).
        4h                           Source Range Entries Copy Descriptor Format 4h is used (refer to NVM Express Subsystem Local Memory Command Set Specification).
        All Others                   Reserved

07:00   Number of Ranges (NR): Specifies the number of Source Range entries that are specified in the command. This is a 0's-based value.

6.16.4 Copy – Command Dword 13

Figure 35: Copy – Command Dword 13
Bits    Description
31:16   Directive Specific (DSPEC): Specifies the Directive Specific value associated with the Directive Type field (refer to the Directives section in the NVM Express Base Specification).
15:00   Command Extension Value (CEV): The definition of this field is dependent on the value of the CETYPE field. Refer to the Key Per I/O section in the NVM Express Base Specification. This field is used for the write portion of the copy operation.

6.16.5 Copy – Command Dword 14

Figure 36: Copy – Command Dword 14
Bits    Description
31:16   Reserved
15:00   Expected Initial Logical Block Reference Tag (EILBRT) (lower 16 bits): Specifies bits 15:00 of the 32-bit EILBRT field.

7 Dataset Management Command

The Dataset Management command is used by the host to indicate attributes for ranges of logical blocks.

7.1 Dataset Management – Command Dword 10

Figure 45: Dataset Management – Command Dword 10
Bits    Description
31:08   Reserved
07:00   Number of Ranges (NR): This field specifies the number of 16-byte range sets that are specified in the command. This is a 0's-based value.

7.2 Dataset Management – Command Dword 11

Figure 46: Dataset Management – Command Dword 11
Bits    Description
31:04   Reserved
03      Deallocate (AD): If this bit is set to '1', then the controller should deallocate the specified ranges.
02      Integral Dataset for Write (IDW): If this bit is set to '1', then the dataset specified is an integral unit for write operations.
01      Integral Dataset for Read (IDR): If this bit is set to '1', then the dataset specified is an integral unit for read operations.
00      Reserved
"""


def _get_chunks():
    chunker = NVMeChunker(
        max_chunk_size=1500,
        overlap_size=150,
        keep_tables_intact=True,
    )
    return chunker.chunk_text(NVME_NVM_CMD_SPEC_SAMPLE)


# ---------------------------------------------------------------------------
# 섹션 감지
# ---------------------------------------------------------------------------

def test_section_detection():
    """스펙 내 모든 섹션 번호가 감지됩니다."""
    chunks = _get_chunks()
    section_numbers = {c.section_number for c in chunks}

    expected_sections = {
        "6", "6.16", "6.16.1", "6.16.2", "6.16.3", "6.16.4", "6.16.5",
        "7", "7.1", "7.2",
    }
    for sec in expected_sections:
        assert sec in section_numbers, f"섹션 {sec!r}이 감지되지 않았습니다"
    print(f"  감지된 섹션: {sorted(section_numbers)}")


# ---------------------------------------------------------------------------
# Figure/Table 감지
# ---------------------------------------------------------------------------

def test_figure_detection():
    """스펙 내 Figure 31, 32, 34, 35, 36, 45, 46이 field 청크로 감지됩니다.

    모든 샘플 Figure는 'Bits Description' 컬럼 형식이므로 field 청크입니다.
    """
    chunks = _get_chunks()
    field_chunks = [c for c in chunks if c.chunk_type == "field"]
    field_numbers = {c.metadata.get("table_number") for c in field_chunks
                     if "table_number" in c.metadata}

    expected_figures = {31, 32, 34, 35, 36, 45, 46}
    for fig_num in expected_figures:
        assert fig_num in field_numbers, \
            f"Figure {fig_num}이 field 청크로 감지되지 않았습니다 (감지됨: {sorted(field_numbers)})"
    print(f"  감지된 field Figure 번호: {sorted(field_numbers)}")


def test_table_number_is_int():
    """table_number 메타데이터가 int 타입입니다."""
    chunks = _get_chunks()
    typed_chunks = [c for c in chunks if "table_number" in c.metadata]
    assert len(typed_chunks) >= 1

    for t in typed_chunks:
        num = t.metadata.get("table_number")
        assert isinstance(num, int), \
            f"table_number가 int가 아닙니다: {num!r} (type={type(num).__name__})"
    print(f"  table_number int 타입 OK ({len(typed_chunks)}개)")


def test_table_section_attribution():
    """각 Figure가 올바른 섹션에 귀속됩니다."""
    chunks = _get_chunks()
    # field 또는 table 청크에서 table_number를 가진 것
    typed_chunks = [c for c in chunks if "table_number" in c.metadata]
    tbl_map = {c.metadata["table_number"]: c.section_number for c in typed_chunks}

    assert tbl_map.get(31) == "6.16.1", f"Figure 31은 섹션 6.16.1 소속이어야 합니다, 실제: {tbl_map.get(31)}"
    assert tbl_map.get(34) == "6.16.3", f"Figure 34는 섹션 6.16.3 소속이어야 합니다, 실제: {tbl_map.get(34)}"
    assert tbl_map.get(45) == "7.1",    f"Figure 45는 섹션 7.1 소속이어야 합니다, 실제: {tbl_map.get(45)}"
    print(f"  Figure-섹션 귀속 OK")


# ---------------------------------------------------------------------------
# 섹션 계층 구조
# ---------------------------------------------------------------------------

def test_section_hierarchy():
    """섹션 parent_section이 계층에 맞게 설정됩니다."""
    chunks = _get_chunks()
    parent_map = {c.section_number: c.parent_section for c in chunks}

    assert parent_map.get("6.16") == "6",      f"6.16의 parent는 '6'이어야 합니다: {parent_map.get('6.16')}"
    assert parent_map.get("6.16.1") == "6.16", f"6.16.1의 parent는 '6.16'이어야 합니다: {parent_map.get('6.16.1')}"
    assert parent_map.get("7.1") == "7",        f"7.1의 parent는 '7'이어야 합니다: {parent_map.get('7.1')}"
    assert parent_map.get("6") == "",           f"최상위 섹션 6의 parent는 ''이어야 합니다: {parent_map.get('6')}"
    print("  섹션 계층 구조 OK")


def test_section_depth():
    """section_depth가 섹션 번호 레벨에 맞게 계산됩니다."""
    chunks = _get_chunks()
    meta_map = {c.section_number: c.to_metadata() for c in chunks}

    assert meta_map["6"]["section_depth"] == 1,      "섹션 6: depth=1"
    assert meta_map["6.16"]["section_depth"] == 2,   "섹션 6.16: depth=2"
    assert meta_map["6.16.1"]["section_depth"] == 3, "섹션 6.16.1: depth=3"
    assert meta_map["7"]["section_depth"] == 1,      "섹션 7: depth=1"
    assert meta_map["7.1"]["section_depth"] == 2,    "섹션 7.1: depth=2"
    print("  section_depth OK")


# ---------------------------------------------------------------------------
# 메타데이터 완전성
# ---------------------------------------------------------------------------

def test_metadata_completeness():
    """모든 청크에 필수 메타데이터가 포함됩니다."""
    chunks = _get_chunks()
    required_keys = [
        "chunk_type", "section_number", "section_title",
        "section_depth", "parent_section", "page_start", "chunk_index",
    ]
    for c in chunks:
        meta = c.to_metadata()
        missing = [k for k in required_keys if k not in meta]
        assert not missing, f"청크 #{c.chunk_index}: 누락 키={missing}"
    print(f"  OK - {len(chunks)}개 청크 모두 필수 메타데이터 보유")


# ---------------------------------------------------------------------------
# Normative language 감지
# ---------------------------------------------------------------------------

def test_normative_language_in_figure34():
    """Figure 34 (field 청크) 내 'shall'/'should' 포함 → has_normative_language=True."""
    chunks = _get_chunks()
    # Figure 34는 Bits Description 형식 → field 청크
    fig34 = next(
        (c for c in chunks
         if c.chunk_type == "field" and c.metadata.get("table_number") == 34),
        None,
    )
    assert fig34 is not None, "Figure 34 field 청크를 찾지 못했습니다"
    assert fig34.metadata.get("has_normative_language") is True, \
        "Figure 34는 'shall'/'should'를 포함하므로 has_normative_language=True이어야 합니다"
    print("  Figure 34 normative language 감지 OK")


# ---------------------------------------------------------------------------
# 실행
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_section_detection,
        test_figure_detection,
        test_table_number_is_int,
        test_table_section_attribution,
        test_section_hierarchy,
        test_section_depth,
        test_metadata_completeness,
        test_normative_language_in_figure34,
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
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n결과: {passed} passed, {failed} failed")
