"""
NVMe Spec Chunker
-----------------
NVMe 스펙 PDF를 구조-aware하게 청킹합니다.

청킹 전략:
1. 계층적 섹션 분리 (1.1, 1.1.1, ... 패턴)
2. 테이블은 원자 단위로 보존 + 소속 섹션 메타데이터
3. 레지스터/필드 정의 블록 보존
4. 각 청크에 풍부한 메타데이터 부착 (섹션 번호, 제목, 페이지, 부모 섹션)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF


# ---------------------------------------------------------------------------
# 데이터 모델
# ---------------------------------------------------------------------------

@dataclass
class NVMeChunk:
    """청킹된 단위 하나를 나타냅니다."""
    text: str
    chunk_type: str          # "section" | "table" | "figure" | "register"
    section_number: str      # e.g. "3.1.2"
    section_title: str
    parent_section: str      # e.g. "3.1"
    page_start: int
    page_end: int
    chunk_index: int
    metadata: dict = field(default_factory=dict)

    def to_metadata(self) -> dict:
        return {
            "chunk_type": self.chunk_type,
            "section_number": self.section_number,
            "section_title": self.section_title,
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

# 테이블 캡션: "Figure 123:" 또는 "Table 456:"
_TABLE_CAPTION_RE = re.compile(
    r"^(Table|Figure)\s+(\d+)[:\s–\-—]+(.{0,100})$",
    re.MULTILINE,
)

# 레지스터 필드 정의 (Bits XX:YY 형태)
_REGISTER_RE = re.compile(
    r"^Bits?\s+\d+:\d+\b",
    re.MULTILINE | re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# PDF 파서
# ---------------------------------------------------------------------------

class NVMePDFParser:
    """PyMuPDF로 NVMe spec PDF 텍스트를 페이지별로 추출합니다."""

    def __init__(self, pdf_path: str | Path) -> None:
        self.pdf_path = Path(pdf_path)
        self._doc: Optional[fitz.Document] = None

    def __enter__(self):
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
            text = page.get_text("text")  # 단순 텍스트 추출
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
        True면 테이블을 분할하지 않고 단일 청크로 유지합니다.
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
        """섹션 하나를 처리하여 청크 목록을 반환합니다."""
        chunks: list[NVMeChunk] = []

        # 테이블/피겨를 먼저 추출
        text_parts, table_parts = self._extract_tables(sec.text)

        # 테이블 청크
        for tbl_text, tbl_meta in table_parts:
            chunks.append(NVMeChunk(
                text=tbl_text,
                chunk_type="table",
                section_number=sec.number,
                section_title=sec.title,
                parent_section=parent,
                page_start=sec.page_start,
                page_end=sec.page_end,
                chunk_index=start_idx + len(chunks),
                metadata=tbl_meta,
            ))

        # 일반 텍스트 청크 (레지스터 블록 감지 포함)
        for part in text_parts:
            if not part.strip():
                continue
            chunk_type = "register" if _is_register_block(part) else "section"
            sub_chunks = self._split_text(part)

            for sub in sub_chunks:
                if not sub.strip():
                    continue
                chunks.append(NVMeChunk(
                    text=sub,
                    chunk_type=chunk_type,
                    section_number=sec.number,
                    section_title=sec.title,
                    parent_section=parent,
                    page_start=sec.page_start,
                    page_end=sec.page_end,
                    chunk_index=start_idx + len(chunks),
                ))

        return chunks

    def _extract_tables(
        self,
        text: str,
    ) -> tuple[list[str], list[tuple[str, dict]]]:
        """
        텍스트에서 Table/Figure 블록을 분리합니다.
        Returns:
            text_parts: 테이블을 제거한 나머지 텍스트 조각들
            table_parts: (table_text, metadata) 튜플 목록
        """
        if not self.keep_tables_intact:
            return [text], []

        table_parts: list[tuple[str, dict]] = []
        text_parts: list[str] = []

        last_end = 0
        for match in _TABLE_CAPTION_RE.finditer(text):
            # 캡션 앞부분 텍스트 저장
            text_parts.append(text[last_end:match.start()])

            # 테이블 본문: 다음 섹션 헤더 or 다음 테이블 캡션까지
            tbl_start = match.start()
            next_section = _SECTION_HEADER_RE.search(text, match.end())
            next_table = _TABLE_CAPTION_RE.search(text, match.end())

            candidates = [m.start() for m in [next_section, next_table] if m]
            tbl_end = min(candidates) if candidates else len(text)

            tbl_text = text[tbl_start:tbl_end].strip()
            tbl_meta = {
                "table_type": match.group(1),   # Table or Figure
                "table_number": match.group(2),
                "table_caption": match.group(3).strip(),
            }
            table_parts.append((tbl_text, tbl_meta))
            last_end = tbl_end

        text_parts.append(text[last_end:])
        return text_parts, table_parts

    def _split_text(self, text: str) -> list[str]:
        """max_chunk_size를 초과하는 텍스트를 문단 경계로 분할합니다."""
        if len(text) <= self.max_chunk_size:
            return [text]

        chunks: list[str] = []
        # 문단 단위로 분할 시도
        paragraphs = re.split(r"\n{2,}", text)
        current = ""

        for para in paragraphs:
            if len(current) + len(para) + 2 <= self.max_chunk_size:
                current = (current + "\n\n" + para).lstrip()
            else:
                if current:
                    chunks.append(current)
                # 문단 자체가 max_chunk_size 초과하는 경우 강제 분할
                if len(para) > self.max_chunk_size:
                    for i in range(0, len(para), self.max_chunk_size - self.overlap_size):
                        chunks.append(para[i:i + self.max_chunk_size])
                    current = ""
                else:
                    current = para

        if current:
            chunks.append(current)

        # overlap 적용
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
    """텍스트가 레지스터 필드 정의 블록인지 판단합니다."""
    return bool(_REGISTER_RE.search(text))


def _apply_overlap(chunks: list[str], overlap: int) -> list[str]:
    """인접 청크 사이에 overlap을 추가합니다."""
    result = [chunks[0]]
    for i in range(1, len(chunks)):
        tail = result[-1][-overlap:] if len(result[-1]) >= overlap else result[-1]
        result.append(tail + "\n" + chunks[i])
    return result
