"""
NVMe RAG - 실행 예시
----------------------
사용법:
    # 청킹 결과만 확인 (LLM/임베딩 불필요)
    python main.py inspect <pdf_path>

    # 인덱스 빌드 (기본: in-memory)
    python main.py build <pdf_path> [--persist ./index_store]

    # ChromaDB에 인덱스 빌드
    python main.py --chroma-host localhost build <pdf_path>
    python main.py --chroma-path ./chroma_db build <pdf_path>

    # Neo4j Knowledge Graph 구축
    python main.py --neo4j-url bolt://localhost:7687 --neo4j-password pw graph-build <pdf_path>

    # 벡터 쿼리
    python main.py query "What is the Admin Submission Queue?"

    # Neo4j 그래프 쿼리
    python main.py --neo4j-url bolt://localhost:7687 --neo4j-password pw graph-query "NVMe command란?"

API Key 설정:
    cp .env.example .env  # 후 각 값 입력
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def setup_llm(use_local: bool = False):
    """LLM과 임베딩 모델을 초기화합니다."""
    if use_local:
        from llama_index.llms.ollama import Ollama
        from llama_index.embeddings.huggingface import HuggingFaceEmbedding
        from llama_index.core import Settings

        Settings.llm = Ollama(model="llama3", request_timeout=120.0)
        Settings.embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-m3")
        print("[Setup] 로컬 모델 사용: Ollama(llama3) + BAAI/bge-m3")
    else:
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


def make_pipeline(args):
    """CLI 인자를 바탕으로 NVMeRAGPipeline을 생성합니다."""
    from core.pipeline import NVMeRAGPipeline

    # 환경변수 fallback 처리
    chroma_host = getattr(args, "chroma_host", None) or os.environ.get("CHROMA_HOST")
    chroma_port = getattr(args, "chroma_port", 8000) or int(os.environ.get("CHROMA_PORT", 8000))
    chroma_path = getattr(args, "chroma_path", None) or os.environ.get("CHROMA_PATH")
    chroma_collection = (
        getattr(args, "chroma_collection", None)
        or os.environ.get("CHROMA_COLLECTION", "nvme_docs")
    )

    neo4j_url = getattr(args, "neo4j_url", None) or os.environ.get("NEO4J_URL")
    neo4j_username = (
        getattr(args, "neo4j_username", None)
        or os.environ.get("NEO4J_USERNAME", "neo4j")
    )
    neo4j_password = getattr(args, "neo4j_password", None) or os.environ.get("NEO4J_PASSWORD")
    neo4j_database = (
        getattr(args, "neo4j_database", None)
        or os.environ.get("NEO4J_DATABASE", "neo4j")
    )

    return NVMeRAGPipeline(
        chunker_kwargs={
            "max_chunk_size": args.chunk_size,
            "overlap_size": args.overlap,
        },
        chroma_host=chroma_host,
        chroma_port=chroma_port,
        chroma_path=chroma_path,
        chroma_collection=chroma_collection,
        neo4j_url=neo4j_url,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_database=neo4j_database,
    )


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
        out = [{"text": c.text, **c.to_metadata()} for c in chunks]
        with open(args.save, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"\n청킹 결과 저장: {args.save}")


def cmd_build(args):
    """벡터 인덱스를 빌드합니다 (기본 or ChromaDB)."""
    setup_llm(use_local=args.local)
    pipeline = make_pipeline(args)
    pipeline.build_index(args.pdf, persist_dir=args.persist)
    print("\n[완료] 인덱스 빌드 성공")


def cmd_graph_build(args):
    """Neo4j Knowledge Graph 인덱스를 빌드합니다."""
    setup_llm(use_local=args.local)
    pipeline = make_pipeline(args)
    pipeline.build_graph_index(
        args.pdf,
        max_triplets_per_chunk=args.triplets,
        include_embeddings=not args.no_embeddings,
    )
    print("\n[완료] Neo4j Knowledge Graph 구축 성공")


def cmd_query(args):
    """벡터 인덱스에 질문합니다."""
    setup_llm(use_local=args.local)
    pipeline = make_pipeline(args)

    use_chroma = (
        getattr(args, "chroma_host", None)
        or os.environ.get("CHROMA_HOST")
        or getattr(args, "chroma_path", None)
        or os.environ.get("CHROMA_PATH")
    )

    if use_chroma:
        pipeline.load_index(persist_dir=None)
    elif args.persist and Path(args.persist).exists():
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
            print(
                f"--- [{i}] Section {meta.get('section_number')} | "
                f"{meta.get('section_title')} | Page {meta.get('page_start')} | "
                f"Score: {node.score:.4f}"
            )
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
            print(
                f"  - Section {meta.get('section_number')}: "
                f"{meta.get('section_title')} (Page {meta.get('page_start')})"
            )


def cmd_graph_query(args):
    """Neo4j 그래프 인덱스에 질문합니다."""
    setup_llm(use_local=args.local)
    pipeline = make_pipeline(args)
    pipeline.load_graph_index()

    print(f"\n[Graph Query] {args.question}\n")

    if args.retrieve_only:
        nodes = pipeline.retrieve_from_graph(args.question, similarity_top_k=args.top_k)
        for i, node in enumerate(nodes, 1):
            print(f"--- [{i}] {node.node.text[:300]}")
            print()
    else:
        response = pipeline.query_graph(
            args.question,
            similarity_top_k=args.top_k,
            response_mode=args.mode,
            use_keyword=not args.embedding_retriever,
        )
        print("[Answer]")
        print(response.response)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="NVMe Spec RAG Pipeline")
    parser.add_argument("--chunk-size", type=int, default=1500, help="최대 청크 크기 (문자)")
    parser.add_argument("--overlap", type=int, default=200, help="청크 간 overlap 크기")
    parser.add_argument("--local", action="store_true", help="로컬 LLM/임베딩 사용")

    # ChromaDB 공통 옵션
    chroma_group = parser.add_argument_group("ChromaDB (벡터 스토어)")
    chroma_ex = chroma_group.add_mutually_exclusive_group()
    chroma_ex.add_argument("--chroma-host", help="ChromaDB 서버 호스트 (예: localhost)")
    chroma_ex.add_argument("--chroma-path", help="ChromaDB 로컬 저장 경로 (예: ./chroma_db)")
    chroma_group.add_argument("--chroma-port", type=int, default=8000, help="ChromaDB 포트 (기본: 8000)")
    chroma_group.add_argument("--chroma-collection", default="nvme_docs", help="ChromaDB 컬렉션 이름")

    # Neo4j 공통 옵션
    neo4j_group = parser.add_argument_group("Neo4j (그래프 스토어)")
    neo4j_group.add_argument("--neo4j-url", help="Neo4j Bolt URL (예: bolt://localhost:7687)")
    neo4j_group.add_argument("--neo4j-username", default="neo4j", help="Neo4j 사용자 이름")
    neo4j_group.add_argument("--neo4j-password", help="Neo4j 비밀번호")
    neo4j_group.add_argument("--neo4j-database", default="neo4j", help="Neo4j 데이터베이스 이름")

    sub = parser.add_subparsers(dest="command", required=True)

    # inspect
    p_inspect = sub.add_parser("inspect", help="청킹 결과 확인 (LLM 불필요)")
    p_inspect.add_argument("pdf", help="NVMe spec PDF 파일 경로")
    p_inspect.add_argument("--top", type=int, default=20, help="출력할 청크 수")
    p_inspect.add_argument("--save", help="JSON 파일로 저장할 경로")
    p_inspect.set_defaults(func=cmd_inspect)

    # build (벡터)
    p_build = sub.add_parser("build", help="벡터 인덱스 빌드 (기본 or ChromaDB)")
    p_build.add_argument("pdf", help="NVMe spec PDF 파일 경로")
    p_build.add_argument(
        "--persist", default="./nvme_index",
        help="인덱스 저장 디렉토리 (in-memory 모드에서만 사용)"
    )
    p_build.set_defaults(func=cmd_build)

    # graph-build (Neo4j)
    p_gbuild = sub.add_parser("graph-build", help="Neo4j Knowledge Graph 구축")
    p_gbuild.add_argument("pdf", help="NVMe spec PDF 파일 경로")
    p_gbuild.add_argument(
        "--triplets", type=int, default=2,
        help="청크당 최대 추출 트리플 수 (기본: 2)"
    )
    p_gbuild.add_argument(
        "--no-embeddings", action="store_true",
        help="그래프 노드에 임베딩 포함 안 함 (속도 향상)"
    )
    p_gbuild.set_defaults(func=cmd_graph_build)

    # query (벡터)
    p_query = sub.add_parser("query", help="벡터 인덱스에 질문")
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

    # graph-query (Neo4j)
    p_gquery = sub.add_parser("graph-query", help="Neo4j 그래프에 질문")
    p_gquery.add_argument("question", help="질문 문자열")
    p_gquery.add_argument("--top-k", type=int, default=5, help="검색할 노드 수")
    p_gquery.add_argument(
        "--mode",
        choices=["compact", "refine", "tree_summarize"],
        default="compact",
        help="응답 생성 모드",
    )
    p_gquery.add_argument(
        "--embedding-retriever", action="store_true",
        help="키워드 대신 임베딩 기반 검색 사용"
    )
    p_gquery.add_argument("--retrieve-only", action="store_true", help="LLM 생성 없이 검색만")
    p_gquery.set_defaults(func=cmd_graph_query)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
