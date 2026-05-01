"""
Local embeddings using sentence-transformers (no API key required).
Model: all-MiniLM-L6-v2 — 384 dimensions, fast, good quality for semantic search.
Downloaded automatically on first use (~90 MB, cached locally after that).
"""
from sentence_transformers import SentenceTransformer

_model: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def embed(texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts. Returns list of 384-dimensional vectors."""
    if not texts:
        return []
    vecs = _get_model().encode(texts, convert_to_numpy=True)
    return vecs.tolist()


def embed_one(text: str) -> list[float]:
    return embed([text])[0]
