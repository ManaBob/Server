"""
NVMe RAG - 실행 예시
----------------------
사용법:
    # 청킹 결과만 확인 (LLM/임베딩 불필요)
    python main.py inspect <pdf_path>

    # 인덱스 빌드 (OPENAI_API_KEY 환경변수 필요)
    python main.py build <pdf_path> [--persist ./index_store]

    # 쿼리
    python main.py query "What is the Admin Submission Queue?" [--persist ./index_store]
"""

import argparse
import os
import sys
from pathlib import Path


def setup_llm(use_local: bool = False):
    """LLM과 임베딩 모델을 초기화합니다."""
    if use_local:
        # 로컬 모델 사용 (Ollama + HuggingFace)
        from llama_index.llms.ollama import Ollama
        from llama_index.embeddings.huggingface import HuggingFaceEmbedding
        from llama_index.core import Settings

        Settings.llm = Ollama(model="llama3", request_timeout=120.0)
        Settings.embed_model = HuggingFaceEmbedding(
            model_name="BAAI/bge-m3"  # 다국어 + 영어 모두 지원
        )
        print("[Setup] 로컬 모델 사용: Ollama(llama3) + BAAI/bge-m3")
    else:
        # OpenAI 사용
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("[Error] OPENAI_API_KEY 환경변수를 설정하세요.")
            print("        로컬 모델 사용 시 --local 플래그를 추가하세요.")
            sys.exit(1)

        from llama_index.llms.openai import OpenAI
        from llama_index.embeddings.openai import OpenAIEmbedding
        from llama_index.core import Settings

        Settings.llm = OpenAI(model="gpt-4o-mini", temperature=0.1)
        Settings.embed_model = OpenAIEmbedding(model="text-embedding-3-small")
        print("[Setup] OpenAI 모델 사용: gpt-4o-mini + text-embedding-3-small")


def cmd_inspect(args):
    """청킹 결과만 확인합니다 (LLM 불필요)."""
    from core.pipeline import NVMeRAGPipeline

    pipeline = NVMeRAGPipeline(
        chunker_kwargs={
            "max_chunk_size": args.chunk_size,
            "overlap_size": args.overlap,
        }
    )
    chunks = pipeline.inspect_chunks(args.pdf, max_display=args.top)

    if args.save:
        import json
        out = [
            {"text": c.text, **c.to_metadata()}
            for c in chunks
        ]
        with open(args.save, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"\n청킹 결과 저장: {args.save}")


def cmd_build(args):
    """인덱스를 빌드합니다."""
    setup_llm(use_local=args.local)

    from core.pipeline import NVMeRAGPipeline

    pipeline = NVMeRAGPipeline(
        chunker_kwargs={
            "max_chunk_size": args.chunk_size,
            "overlap_size": args.overlap,
        }
    )
    pipeline.build_index(args.pdf, persist_dir=args.persist)
    print("\n[완료] 인덱스 빌드 성공")


def cmd_query(args):
    """인덱스에 질문합니다."""
    setup_llm(use_local=args.local)

    from core.pipeline import NVMeRAGPipeline

    pipeline = NVMeRAGPipeline()

    if args.persist and Path(args.persist).exists():
        pipeline.load_index(args.persist)
    else:
        if not args.pdf:
            print("[Error] --persist 경로가 없으면 --pdf 옵션으로 PDF를 지정해야 합니다.")
            sys.exit(1)
        pipeline.build_index(args.pdf, persist_dir=args.persist)

    print(f"\n[Query] {args.question}\n")

    if args.retrieve_only:
        nodes = pipeline.retrieve(args.question, similarity_top_k=args.top_k)
        for i, node in enumerate(nodes, 1):
            meta = node.node.metadata
            print(f"--- [{i}] Section {meta.get('section_number')} | "
                  f"{meta.get('section_title')} | Page {meta.get('page_start')} | "
                  f"Score: {node.score:.4f}")
            print(node.node.text[:300])
            print()
    else:
        response = pipeline.query(
            args.question,
            similarity_top_k=args.top_k,
            response_mode=args.mode,
        )
        print("[Answer]")
        print(response.response)
        print("\n[Sources]")
        for node in response.source_nodes:
            meta = node.node.metadata
            print(f"  - Section {meta.get('section_number')}: "
                  f"{meta.get('section_title')} (Page {meta.get('page_start')})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="NVMe Spec RAG Pipeline")
    parser.add_argument("--chunk-size", type=int, default=1500, help="최대 청크 크기 (문자)")
    parser.add_argument("--overlap", type=int, default=200, help="청크 간 overlap 크기")
    parser.add_argument("--local", action="store_true", help="로컬 LLM/임베딩 사용")

    sub = parser.add_subparsers(dest="command", required=True)

    # inspect
    p_inspect = sub.add_parser("inspect", help="청킹 결과 확인 (LLM 불필요)")
    p_inspect.add_argument("pdf", help="NVMe spec PDF 파일 경로")
    p_inspect.add_argument("--top", type=int, default=20, help="출력할 청크 수")
    p_inspect.add_argument("--save", help="JSON 파일로 저장할 경로")
    p_inspect.set_defaults(func=cmd_inspect)

    # build
    p_build = sub.add_parser("build", help="벡터 인덱스 빌드")
    p_build.add_argument("pdf", help="NVMe spec PDF 파일 경로")
    p_build.add_argument("--persist", default="./nvme_index", help="인덱스 저장 디렉토리")
    p_build.set_defaults(func=cmd_build)

    # query
    p_query = sub.add_parser("query", help="인덱스에 질문")
    p_query.add_argument("question", help="질문 문자열")
    p_query.add_argument("--pdf", help="인덱스 없을 때 사용할 PDF")
    p_query.add_argument("--persist", default="./nvme_index", help="인덱스 디렉토리")
    p_query.add_argument("--top-k", type=int, default=5, help="검색할 청크 수")
    p_query.add_argument(
        "--mode",
        choices=["compact", "refine", "tree_summarize"],
        default="compact",
        help="응답 생성 모드",
    )
    p_query.add_argument("--retrieve-only", action="store_true", help="LLM 생성 없이 검색만")
    p_query.set_defaults(func=cmd_query)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
