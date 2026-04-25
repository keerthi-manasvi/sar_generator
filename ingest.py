import os
import re
import json
import uuid
import hashlib
import logging
from datetime import datetime, timezone
from time import sleep

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
from sentence_transformers import SentenceTransformer
import pdfplumber

# ── CONFIG ─────────────────────────────────────────────────────────────────────
BASE_PATH = "./Embedding_Data"
COLLECTION = "fincen_knowledge_base"

QDRANT_URL = "https://0d3e82ee-f4f0-4689-9c28-5dd3132fcca0.eu-west-2-0.aws.cloud.qdrant.io"
QDRANT_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwic3ViamVjdCI6ImFwaS1rZXk6M2JlYjlhOGMtMThkMC00OTBjLTllMWUtOGFiMDkyOTczMmQyIn0.FNCOktKU8kmXgNAJcwaYf2L2R3uPrLtqKoYRtnCl3-0"

RESET_COLLECTION = False  # set False if you don’t want deletion

BATCH_SIZE = 64
CHUNK_OVERLAP = 50
MAX_CHUNK_CHARS = 1200

# ── INIT ───────────────────────────────────────────────────────────────────────
client = QdrantClient(
    url=QDRANT_URL,
    api_key=QDRANT_API_KEY,
    timeout=120,
    check_compatibility=False
)

embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── COLLECTION SETUP ───────────────────────────────────────────────────────────
def setup_collection():
    if RESET_COLLECTION:
        try:
            client.delete_collection(COLLECTION)
            logger.info("🗑️ Old collection deleted")
        except:
            pass

    if not client.collection_exists(COLLECTION):
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
        )
        logger.info("✅ Collection created")
    else:
        logger.info("✅ Collection exists")

# ── PII SCRUBBING ──────────────────────────────────────────────────────────────
PII_PATTERNS = [
    (r"\b\d{3}-\d{2}-\d{4}\b", "[SSN]"),
    (r"\b\d{16,19}\b", "[ACCT]"),
    (r"\b[A-Z]{2}\d{6,9}\b", "[PASSPORT]"),
    (r"\b\d{1,5}\s[\w\s]{3,30}(Street|St|Ave|Rd|Blvd|Lane|Ln|Drive|Dr)\b", "[ADDR]"),
    (r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", "[EMAIL]"),
    (r"\b(\+?\d{1,3})?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b", "[PHONE]"),
]

def scrub_pii(text: str) -> str:
    for pattern, replacement in PII_PATTERNS:
        text = re.sub(pattern, replacement, text)
    return text

# ── CHUNKING ───────────────────────────────────────────────────────────────────
def chunk_text(text):
    paragraphs = [p.strip() for p in text.split("\n\n") if len(p.strip()) > 50]
    chunks, current = [], ""

    for para in paragraphs:
        if len(current) + len(para) > MAX_CHUNK_CHARS:
            if current:
                chunks.append(current.strip())
            current = current[-CHUNK_OVERLAP:] + "\n\n" + para if current else para
        else:
            current = (current + "\n\n" + para).strip()

    if current:
        chunks.append(current.strip())

    return chunks

# ── UTILS ──────────────────────────────────────────────────────────────────────
def make_id(namespace_key, content):
    return str(uuid.uuid5(NAMESPACE, f"{namespace_key}:{content[:200]}"))

def content_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()[:16]

def get_embedding(text):
    return embedding_model.encode(text).tolist()

# ── UPSERT ─────────────────────────────────────────────────────────────────────
def upsert_batch(points):
    for i in range(0, len(points), BATCH_SIZE):
        batch = points[i:i+BATCH_SIZE]

        for attempt in range(3):
            try:
                client.upsert(collection_name=COLLECTION, points=batch)
                logger.info(f"Upserted batch {i//BATCH_SIZE}")
                break
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Batch failed: {e}")
                    raise
                sleep(2 ** attempt)

# ── ADVISORY PARSER (ROBUST) ───────────────────────────────────────────────────
def extract_advisory_text(data):
    if "results" in data:
        return "\n\n".join(
            r.get("text", "") for r in data["results"] if r.get("text")
        )

    if "text" in data:
        return data["text"]

    if "content" in data:
        return data["content"]

    return json.dumps(data)

# ── INGESTION ──────────────────────────────────────────────────────────────────
def ingest_data():
    points = []
    ingest_ts = datetime.now(timezone.utc).isoformat()

    for root, _, files in os.walk(BASE_PATH):
        folder_name = os.path.basename(root)

        for file in files:
            file_path = os.path.join(root, file)

            try:
                content = ""
                doc_type = None
                extra_meta = {}

                # ── ADVISORIES ────────────────────────────────
                if folder_name == "FincenAdvisories" and file.endswith(".json"):
                    with open(file_path, encoding="utf-8") as f:
                        data = json.load(f)

                    content = extract_advisory_text(data)

                    doc_type = "advisory"
                    extra_meta = {
                        "title": data.get("title", ""),
                        "url": data.get("url", "")
                    }

                # ── GUIDELINES ───────────────────────────────
                elif folder_name == "Guidelines" and file.endswith(".pdf"):
                    with pdfplumber.open(file_path) as pdf:
                        content = "\n\n".join(
                            page.extract_text() or "" for page in pdf.pages
                        )
                    doc_type = "regulatory_guideline"

                else:
                    continue

                if not content.strip():
                    logger.warning(f"❌ Empty content: {file}")
                    continue

                clean = scrub_pii(content)
                chunks = chunk_text(clean)

                logger.info(f"📄 {file} → {len(chunks)} chunks")

                for i, chunk in enumerate(chunks):
                    points.append(PointStruct(
                        id=make_id(file, str(i)),
                        vector=get_embedding(chunk),
                        payload={
                            "file_name": file,
                            "doc_type": doc_type,
                            "chunk_index": i,
                            "total_chunks": len(chunks),
                            "content": chunk,
                            "source": "FinCEN",
                            "content_hash": content_hash(chunk),
                            "ingest_timestamp": ingest_ts,
                            "file_path": file_path,
                            **extra_meta,
                        }
                    ))

            except Exception as e:
                logger.warning(f"Skipping {file_path} — {e}")

    if not points:
        logger.warning("⚠️ No data found!")
        return

    upsert_batch(points)
    logger.info(f"✅ DONE. Total vectors: {len(points)}")

# ── RUN ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    setup_collection()
    ingest_data()