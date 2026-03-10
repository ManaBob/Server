"""
NVMe Spec Chunker
-----------------
NVMe 스펙 PDF를 구조-aware하게 청킹합니다.

문서 구조 인식:
  Document → Chapter → Section/Subsection → Paragraph / Table / Figure
           → Field definition → Note/Requirement

청크 타입 (4종):
  section  - 설명 문단 중심 (command overview, 개념 설명)
  table    - 표 전체 또는 row group (Identify Controller Data Structure, SMART 등)
  field    - 필드 정의 1개 또는 묶음 (MPTR, PRP1, Bits XX:YY 등)
  note     - NOTE: / Warning: / shall·must 전용 단락 (제약 / 예외사항)

메타데이터 (모든 청크 공통):
  chunk_type, section_number, section_title, section_depth,
  parent_section, page_start, page_end, chunk_index,
  has_normative_language
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# PyMuPDF는 PDF 파싱에만 필요하므로 optional import
try:
    import fitz  # PyMuPDF
    _FITZ_AVAILABLE = True
except ImportError:
    _FITZ_AVAILABLE = False


# ---------------------------------------------------------------------------
# 데이터 모델
# ---------------------------------------------------------------------------

@dataclass
class NVMeChunk:
    """청킹된 단위 하나를 나타냅니다.

    chunk_type:
        "section" - 설명 문단
        "table"   - 구조적 표 (Identify Controller Data Structure, SMART 등)
        "field"   - 필드/레지스터 정의 (Bits XX:YY, MPTR, PRP1 등)
        "note"    - NOTE: / Warning: / Caution: 블록
    """
    text: str
    chunk_type: str          # "section" | "table" | "field" | "note"
    section_number: str      # e.g. "3.1.2"
    section_title: str
    parent_section: str      # e.g. "3.1"  (최상위는 "")
    page_start: int
    page_end: int
    chunk_index: int
    metadata: dict = field(default_factory=dict)

    def to_metadata(self) -> dict:
        """LlamaIndex TextNode에 붙일 메타데이터 딕셔너리를 반환합니다."""
        if self.section_number.startswith("Annex"):
            depth = 1
        else:
            depth = len(self.section_number.split("."))
        return {
            "chunk_type": self.chunk_type,
            "section_number": self.section_number,
            "section_title": self.section_title,
            "section_depth": depth,
            "parent_section": self.parent_section,
            "page_start": self.page_start,
            "page_end": self.page_end,
            "chunk_index": self.chunk_index,
            **self.metadata,
        }


# ---------------------------------------------------------------------------
# 정규식 패턴
# ---------------------------------------------------------------------------

# NVMe 스펙 섹션 헤더: "3.1.2 Command Dword 0" 형태
_SECTION_HEADER_RE = re.compile(
    r"^(\d+(?:\.\d+){0,4})\s{1,4}([A-Z][^\n]{2,80})$",
    re.MULTILINE,
)

# 부록 섹션: "Annex A" 또는 "Appendix A"
_ANNEX_RE = re.compile(
    r"^(Annex|Appendix)\s+([A-Z])\s*[–\-—]?\s*(.{2,60})$",
    re.MULTILINE,
)

# 테이블/피겨 캡션: "Figure 123:" 또는 "Table 456:"
_TABLE_CAPTION_RE = re.compile(
    r"^(Table|Figure)\s+(\d+)[:\s–\-—]+(.{0,100})$",
    re.MULTILINE,
)

# 필드/레지스터 정의 (Bits XX:YY 또는 Bit X 형태, 단일 비트 포함)
_REGISTER_RE = re.compile(
    r"^Bits?\s+\d+(?::\d+)?\b",
    re.MULTILINE | re.IGNORECASE,
)

# "Bits  Description" 컬럼 헤더 형식 (NVMe spec Figure 테이블)
# 예: Figure 31: Bits / Description 컬럼으로 구성된 필드 정의 테이블
_FIELD_TABLE_HEADER_RE = re.compile(
    r"^Bits\s{2,}(?:Description|Type|Value)\s*$",
    re.MULTILINE | re.IGNORECASE,
)

# NVMe 스펙 규범적 언어 (RFC 2119 용어)
_NORMATIVE_RE = re.compile(
    r"\b(shall(?: not)?|should(?: not)?|may(?: not)?|must(?: not)?)\b",
    re.IGNORECASE,
)

# NOTE / Warning / Caution 블록 (단락 첫 줄 기준)
_NOTE_BLOCK_RE = re.compile(
    r"^(NOTE|note|Warning|Caution|CAUTION|WARNING|IMPORTANT)\s*[:\-–—]",
    re.MULTILINE,
)


# ---------------------------------------------------------------------------
# PDF 파서
# ---------------------------------------------------------------------------

class NVMePDFParser:
    """PyMuPDF로 NVMe spec PDF 텍스트를 페이지별로 추출합니다."""

    def __init__(self, pdf_path: str | Path) -> None:
        self.pdf_path = Path(pdf_path)
        self._doc: Optional[object] = None

    def __enter__(self):
        if not _FITZ_AVAILABLE:
            raise ImportError(
                "PyMuPDF(fitz)가 설치되어 있지 않습니다. "
                "PDF 파싱에 필요합니다: pip install pymupdf"
            )
        self._doc = fitz.open(str(self.pdf_path))
        return self

    def __exit__(self, *_):
        if self._doc:
            self._doc.close()

    def iter_pages(self) -> list[dict]:
        """각 페이지의 텍스트와 번호를 반환합니다."""
        pages = []
        for page_num in range(len(self._doc)):
            page = self._doc[page_num]
            text = page.get_text("text")
            pages.append({"page": page_num + 1, "text": text})
        return pages

    def get_full_text_with_pages(self) -> list[tuple[int, str]]:
        """(page_number, text) 튜플 리스트를 반환합니다."""
        return [(p["page"], p["text"]) for p in self.iter_pages()]


# ---------------------------------------------------------------------------
# 섹션 분리기
# ---------------------------------------------------------------------------

@dataclass
class _RawSection:
    number: str
    title: str
    text: str
    page_start: int
    page_end: int


class SectionSplitter:
    """
    전체 문서 텍스트를 NVMe 섹션 헤더 패턴으로 분리합니다.
    페이지 경계 정보를 보존합니다.
    """

    def split(self, pages: list[tuple[int, str]]) -> list[_RawSection]:
        # 전체 텍스트에 페이지 마커를 삽입
        combined = ""
        page_offsets: list[tuple[int, int]] = []  # (char_offset, page_num)

        for page_num, text in pages:
            page_offsets.append((len(combined), page_num))
            combined += text + f"\n<<<PAGE:{page_num}>>>\n"

        def _get_page(offset: int) -> int:
            pg = 1
            for char_off, pnum in page_offsets:
                if char_off <= offset:
                    pg = pnum
                else:
                    break
            return pg

        # 섹션 헤더 위치 수집
        matches = list(_SECTION_HEADER_RE.finditer(combined))
        matches += list(_ANNEX_RE.finditer(combined))
        matches.sort(key=lambda m: m.start())

        sections: list[_RawSection] = []

        for i, match in enumerate(matches):
            start = match.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(combined)
            body = combined[start:end]

            # 페이지 마커 제거
            clean_body = re.sub(r"<<<PAGE:\d+>>>", "", body).strip()

            if match.re == _ANNEX_RE:
                sec_num = f"Annex {match.group(2)}"
                sec_title = match.group(3).strip()
            else:
                sec_num = match.group(1)
                sec_title = match.group(2).strip()

            sections.append(_RawSection(
                number=sec_num,
                title=sec_title,
                text=clean_body,
                page_start=_get_page(start),
                page_end=_get_page(end - 1),
            ))

        return sections


# ---------------------------------------------------------------------------
# 청크 생성기
# ---------------------------------------------------------------------------

class NVMeChunker:
    """
    NVMe 스펙 PDF를 청킹하는 메인 클래스입니다.

    Parameters
    ----------
    max_chunk_size : int
        섹션 텍스트가 이 크기를 초과하면 하위 청크로 분할합니다 (문자 수 기준).
    overlap_size : int
        분할 청크 간 겹치는 문자 수 (컨텍스트 보존).
    keep_tables_intact : bool
        True면 테이블/피겨를 분할하지 않고 단일 청크로 유지합니다.
    """

    def __init__(
        self,
        max_chunk_size: int = 1500,
        overlap_size: int = 200,
        keep_tables_intact: bool = True,
    ) -> None:
        self.max_chunk_size = max_chunk_size
        self.overlap_size = overlap_size
        self.keep_tables_intact = keep_tables_intact
        self._splitter = SectionSplitter()

    # ------------------------------------------------------------------
    # 공개 API
    # ------------------------------------------------------------------

    def chunk_pdf(self, pdf_path: str | Path) -> list[NVMeChunk]:
        """PDF 파일을 읽어 NVMeChunk 리스트로 반환합니다."""
        with NVMePDFParser(pdf_path) as parser:
            pages = parser.get_full_text_with_pages()
        return self._chunk_pages(pages)

    def chunk_text(self, text: str) -> list[NVMeChunk]:
        """이미 추출된 텍스트를 청킹합니다 (테스트/디버그용)."""
        pages = [(1, text)]
        return self._chunk_pages(pages)

    # ------------------------------------------------------------------
    # 내부 로직
    # ------------------------------------------------------------------

    def _chunk_pages(self, pages: list[tuple[int, str]]) -> list[NVMeChunk]:
        sections = self._splitter.split(pages)
        chunks: list[NVMeChunk] = []
        idx = 0

        for sec in sections:
            parent = _parent_section(sec.number)
            sec_chunks = self._process_section(sec, parent, idx)
            chunks.extend(sec_chunks)
            idx += len(sec_chunks)

        return chunks

    def _process_section(
        self,
        sec: _RawSection,
        parent: str,
        start_idx: int,
    ) -> list[NVMeChunk]:
        """
        섹션 하나를 처리하여 청크 목록을 반환합니다.

        처리 순서:
        1. Table / Figure 추출 → table 또는 field 청크
        2. 나머지 텍스트 단락 분류 → section / field / note 청크
        """
        chunks: list[NVMeChunk] = []

        # ── 1. Table / Figure 추출 ─────────────────────────────────────
        text_parts, table_parts = self._extract_tables(sec.text)

        for tbl_text, tbl_meta, tbl_type in table_parts:
            tbl_meta["has_normative_language"] = bool(_NORMATIVE_RE.search(tbl_text))
            chunks.append(NVMeChunk(
                text=tbl_text,
                chunk_type=tbl_type,
                section_number=sec.number,
                section_title=sec.title,
                parent_section=parent,
                page_start=sec.page_start,
                page_end=sec.page_end,
                chunk_index=start_idx + len(chunks),
                metadata=tbl_meta,
            ))

        # ── 2. 나머지 텍스트 → 단락 단위 분류 ─────────────────────────
        for part in text_parts:
            if not part.strip():
                continue
            self._classify_text_part(
                part, sec, parent, start_idx, chunks
            )

        return chunks

    def _classify_text_part(
        self,
        part: str,
        sec: _RawSection,
        parent: str,
        start_idx: int,
        chunks: list[NVMeChunk],
    ) -> None:
        """
        텍스트 파트를 단락 단위로 분류합니다.

        - NOTE:/Warning:/Caution: 로 시작하는 단락 → note 청크
        - Bits X:Y 형태의 레지스터/필드 정의 블록 → field 청크
        - 그 외 → section 청크 (max_chunk_size 초과 시 분할)

        연속된 non-note 단락은 하나로 묶어 처리합니다.
        """
        paragraphs = re.split(r"\n{2,}", part.strip())
        regular_buf: list[str] = []

        def _flush_regular():
            """regular_buf를 section/field 청크로 변환합니다."""
            if not regular_buf:
                return
            text = "\n\n".join(regular_buf)
            chunk_type = "field" if _is_register_block(text) else "section"
            for sub in self._split_text(text):
                if sub.strip():
                    chunks.append(NVMeChunk(
                        text=sub,
                        chunk_type=chunk_type,
                        section_number=sec.number,
                        section_title=sec.title,
                        parent_section=parent,
                        page_start=sec.page_start,
                        page_end=sec.page_end,
                        chunk_index=start_idx + len(chunks),
                        metadata={"has_normative_language": bool(_NORMATIVE_RE.search(sub))},
                    ))
            regular_buf.clear()

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            if _NOTE_BLOCK_RE.match(para):
                _flush_regular()
                chunks.append(NVMeChunk(
                    text=para,
                    chunk_type="note",
                    section_number=sec.number,
                    section_title=sec.title,
                    parent_section=parent,
                    page_start=sec.page_start,
                    page_end=sec.page_end,
                    chunk_index=start_idx + len(chunks),
                    metadata={"has_normative_language": bool(_NORMATIVE_RE.search(para))},
                ))
            else:
                regular_buf.append(para)

        _flush_regular()

    def _extract_tables(
        self,
        text: str,
    ) -> tuple[list[str], list[tuple[str, dict, str]]]:
        """
        텍스트에서 Table/Figure 블록을 분리합니다.

        chunk_type 결정:
          - "Table X:" → "table"  (구조적 데이터 표)
          - "Figure X:" + Bits 컬럼 → "field"  (필드/레지스터 정의)
          - "Figure X:" + Bits 컬럼 없음 → "table"  (일반 피겨)

        Returns:
            text_parts: 테이블을 제거한 나머지 텍스트 조각들
            table_parts: (table_text, metadata, chunk_type) 튜플 목록
        """
        if not self.keep_tables_intact:
            return [text], []

        table_parts: list[tuple[str, dict, str]] = []
        text_parts: list[str] = []

        last_end = 0
        for match in _TABLE_CAPTION_RE.finditer(text):
            text_parts.append(text[last_end:match.start()])

            tbl_start = match.start()
            next_section = _SECTION_HEADER_RE.search(text, match.end())
            next_table = _TABLE_CAPTION_RE.search(text, match.end())

            candidates = [m.start() for m in [next_section, next_table] if m]
            tbl_end = min(candidates) if candidates else len(text)

            tbl_text = text[tbl_start:tbl_end].strip()
            caption_type = match.group(1)   # "Table" or "Figure"

            # Figure + Bits 컬럼 형식 → field chunk
            if caption_type == "Figure" and _is_register_block(tbl_text):
                chunk_type = "field"
            else:
                chunk_type = "table"

            tbl_meta = {
                "table_type": caption_type,
                "table_number": int(match.group(2)),
                "table_caption": match.group(3).strip(),
            }
            table_parts.append((tbl_text, tbl_meta, chunk_type))
            last_end = tbl_end

        text_parts.append(text[last_end:])
        return text_parts, table_parts

    def _split_text(self, text: str) -> list[str]:
        """max_chunk_size를 초과하는 텍스트를 문단 경계로 분할합니다."""
        if len(text) <= self.max_chunk_size:
            return [text]

        chunks: list[str] = []
        paragraphs = re.split(r"\n{2,}", text)
        current = ""

        for para in paragraphs:
            if len(current) + len(para) + 2 <= self.max_chunk_size:
                current = (current + "\n\n" + para).lstrip()
            else:
                if current:
                    chunks.append(current)
                if len(para) > self.max_chunk_size:
                    for i in range(0, len(para), self.max_chunk_size - self.overlap_size):
                        chunks.append(para[i:i + self.max_chunk_size])
                    current = ""
                else:
                    current = para

        if current:
            chunks.append(current)

        if self.overlap_size > 0 and len(chunks) > 1:
            chunks = _apply_overlap(chunks, self.overlap_size)

        return chunks


# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

def _parent_section(section_number: str) -> str:
    """'3.1.2' -> '3.1', '3.1' -> '3', '3' -> '' """
    parts = section_number.rsplit(".", 1)
    return parts[0] if len(parts) > 1 else ""


def _is_register_block(text: str) -> bool:
    """텍스트가 레지스터/필드 정의 블록인지 판단합니다.

    아래 중 하나라도 해당하면 True:
    - 'Bits XX:YY' 또는 'Bit X' 패턴이 있는 경우 (inline 형식)
    - 'Bits  Description' 컬럼 헤더가 있는 경우 (NVMe Figure 테이블 형식)
    """
    return bool(_REGISTER_RE.search(text)) or bool(_FIELD_TABLE_HEADER_RE.search(text))


def _apply_overlap(chunks: list[str], overlap: int) -> list[str]:
    """인접 청크 사이에 overlap을 추가합니다."""
    result = [chunks[0]]
    for i in range(1, len(chunks)):
        tail = result[-1][-overlap:] if len(result[-1]) >= overlap else result[-1]
        result.append(tail + "\n" + chunks[i])
    return result
