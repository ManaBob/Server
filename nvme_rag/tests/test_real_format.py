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


def run_test():
    print("=" * 70)
    print("NVMe NVM Command Set Spec 1.1 형식 청킹 테스트")
    print("(스크린샷 p.30 기반 - Figure 34, 35 포함)")
    print("=" * 70)

    chunker = NVMeChunker(
        max_chunk_size=1500,
        overlap_size=150,
        keep_tables_intact=True,
    )
    chunks = chunker.chunk_text(NVME_NVM_CMD_SPEC_SAMPLE)

    # 결과 출력
    print(f"\n총 {len(chunks)}개 청크 생성\n")
    print(f"{'#':<5} {'타입':<10} {'섹션':<10} {'제목':<35} {'길이'}")
    print("-" * 75)
    for c in chunks:
        print(
            f"{c.chunk_index:<5} {c.chunk_type:<10} {c.section_number:<10} "
            f"{c.section_title[:33]:<35} {len(c.text)}"
        )

    # 검증
    print("\n" + "=" * 70)
    print("검증 결과")
    print("=" * 70)

    section_chunks = [c for c in chunks if c.chunk_type == "section"]
    table_chunks   = [c for c in chunks if c.chunk_type == "table"]
    reg_chunks     = [c for c in chunks if c.chunk_type == "register"]

    print(f"  section 청크: {len(section_chunks)}개")
    print(f"  table   청크: {len(table_chunks)}개")
    print(f"  register청크: {len(reg_chunks)}개")

    # 테이블 청크 상세
    if table_chunks:
        print("\n[테이블 청크 상세]")
        for t in table_chunks:
            print(f"  - {t.metadata.get('table_type')} {t.metadata.get('table_number')}: "
                  f"{t.metadata.get('table_caption')}")
            print(f"    소속 섹션: {t.section_number} {t.section_title}")
            print(f"    텍스트 앞 100자: {t.text[:100].replace(chr(10), ' ')!r}")

    # 레지스터 청크 상세
    if reg_chunks:
        print("\n[레지스터 청크 상세]")
        for r in reg_chunks:
            print(f"  - 섹션 {r.section_number}: {r.section_title}")
            print(f"    첫 줄: {r.text.splitlines()[0]!r}")

    # 섹션 계층 검증
    print("\n[섹션 계층 확인]")
    seen_sections = sorted({c.section_number for c in chunks})
    for sec in seen_sections:
        parent = next((c.parent_section for c in chunks if c.section_number == sec), "")
        title  = next((c.section_title   for c in chunks if c.section_number == sec), "")
        print(f"  {sec:<12} parent={parent!r:<10} title={title[:40]!r}")

    # 메타데이터 완전성
    print("\n[메타데이터 완전성]")
    required_keys = ["chunk_type", "section_number", "section_title",
                     "parent_section", "page_start", "chunk_index"]
    all_ok = True
    for c in chunks:
        meta = c.to_metadata()
        missing = [k for k in required_keys if k not in meta]
        if missing:
            print(f"  FAIL 청크 #{c.chunk_index}: 누락={missing}")
            all_ok = False
    if all_ok:
        print(f"  OK - {len(chunks)}개 청크 모두 필수 메타데이터 보유")

    print("\n[특정 청크 내용 미리보기 - Figure 34 테이블]")
    fig34 = next((c for c in table_chunks if "34" in c.metadata.get("table_number", "")), None)
    if fig34:
        print(fig34.text[:400])
        print("...")
    else:
        print("  Figure 34 테이블 청크를 찾지 못했습니다.")


if __name__ == "__main__":
    run_test()
