"""Canonical normalization for `programs.raw_degree_type`.

Maps a free-text degree label (e.g. "B.Des.", "BA (Hons)", "BA/MArch",
"Licenciatura", "Ph.D.", "Other") to a controlled vocabulary held in the
`degree_labels` dictionary table.

Public API
----------
`normalize_degree(raw)` returns the two fields written into `programs`:

    normalized_degree_type : code from `degree_labels` (e.g. "BDes", "PhD",
                             "BA/MArch") — or None when the raw text cannot
                             be confidently mapped.
    honours_flag           : True when the raw text carries (Hons)/(Honours).

`iter_label_catalog()` yields the full controlled vocabulary (single +
combined entries) so `scripts/sync_degree_labels.py` can keep the dictionary
table aligned with this module without duplicating the source of truth.

Design note
-----------
The original wording stays in `raw_degree_type` (audit trail). All other
metadata that used to live as columns on `programs` (`degree_family`,
`combined_degree_flag`, `combined_with`) is now an attribute of the
controlled vocabulary itself — joined in via `degree_labels` when needed.

Combined degrees are NOT a Cartesian product. Only the patterns explicitly
listed in `_COMBINED_CATALOG` map to a non-null `normalized_degree_type`;
unknown combinations fall through to None so they surface for review.
"""
from __future__ import annotations

import re
from typing import Iterator, Optional, TypedDict


class DegreeFields(TypedDict):
    normalized_degree_type: Optional[str]
    honours_flag: bool


class DegreeLabel(TypedDict):
    code: str
    family: str
    is_combined: bool
    parts: Optional[list[str]]


BACHELOR = "Bachelor"
MASTER = "Master"
DOCTORATE = "Doctorate"
DIPLOMA = "Diploma"
OTHER = "Other"

_FAMILY_RANK = {OTHER: 0, DIPLOMA: 1, BACHELOR: 2, MASTER: 3, DOCTORATE: 4}

# Single-degree canonicals. Code -> family. These are the atomic vocabulary
# items; every code listed here must be present in the `degree_labels` table.
_CANONICAL_FAMILY: dict[str, str] = {
    # Bachelor (specific)
    "BA": BACHELOR, "BSc": BACHELOR, "BS": BACHELOR, "BFA": BACHELOR,
    "BDes": BACHELOR, "BEng": BACHELOR, "BArch": BACHELOR, "BMus": BACHELOR,
    "BBA": BACHELOR, "LLB": BACHELOR,
    # Bachelor (specific, art/design long tail)
    "BEd": BACHELOR, "BEnvD": BACHELOR, "BAS": BACHELOR, "BAFT": BACHELOR,
    "BVA": BACHELOR, "BDI": BACHELOR, "BID": BACHELOR,
    # Bachelor (generic / national variants)
    "Bachelor": BACHELOR,
    "Licenciatura": BACHELOR,
    "Specialist": BACHELOR,  # 5-year post-Soviet undergrad, mirrors Licenciatura
    # Master (specific)
    "MA": MASTER, "MSc": MASTER, "MS": MASTER, "MFA": MASTER,
    "MDes": MASTER, "MArch": MASTER, "MEng": MASTER, "MPhil": MASTER,
    "MBA": MASTER, "MRes": MASTER, "MMus": MASTER, "LLM": MASTER,
    # Master (specific, art/design long tail)
    "MLitt": MASTER, "MPA": MASTER, "MVS": MASTER, "MDI": MASTER, "MID": MASTER,
    "Meisterschüler": MASTER,  # German art academy post-diploma qualification
    # Master (generic)
    "Master": MASTER,
    # Doctorate
    "PhD": DOCTORATE, "DPhil": DOCTORATE, "EdD": DOCTORATE, "MD": DOCTORATE,
    "DFA": DOCTORATE,
    "Doctorate": DOCTORATE,
    # Diploma / certificate tier
    "Diploma": DIPLOMA, "Higher Diploma": DIPLOMA, "HND": DIPLOMA,
    "Certificate": DIPLOMA, "PGDip": DIPLOMA, "PGCert": DIPLOMA,
    "Foundation": DIPLOMA,
    "AFA": DIPLOMA,  # Associate of Fine Arts (2-year US pre-bachelor)
}

# Known combined-degree codes. These are real curriculum patterns observed in
# art / design / architecture corpora, not arbitrary cartesian products.
# To accept a new combined pattern: add it here, then re-run
#   python -m scripts.sync_degree_labels
# so the dictionary table picks up the new entry. The key MUST equal
# "/".join(parts) using the canonical single-degree codes.
_COMBINED_CATALOG: dict[str, list[str]] = {
    "BA/BS": ["BA", "BS"],
    "BA/MA": ["BA", "MA"],
    "BA/MArch": ["BA", "MArch"],
    "BA/MDes": ["BA", "MDes"],
    "BDes/MArch": ["BDes", "MArch"],
    "MDes/MFA": ["MDes", "MFA"],
}

# Raw-variant (lowercased) -> canonical code. Punctuation and case are
# normalized before lookup; add new aliases here when a new spelling appears.
_ALIASES: dict[str, str] = {
    # --- Bachelor specifics ---
    "ba": "BA", "b.a.": "BA", "b.a": "BA",
    "bachelor of arts": "BA",
    "bsc": "BSc", "b.sc.": "BSc", "b.sc": "BSc",
    "bachelor of science": "BSc",
    "bs": "BS", "b.s.": "BS", "b.s": "BS",
    "bfa": "BFA", "b.f.a.": "BFA", "b.f.a": "BFA",
    "bachelor of fine arts": "BFA",
    "bdes": "BDes", "b.des.": "BDes", "b.des": "BDes", "bdesign": "BDes",
    "bachelor of design": "BDes",
    "beng": "BEng", "b.eng.": "BEng", "b.eng": "BEng",
    "bachelor of engineering": "BEng",
    "barch": "BArch", "b.arch.": "BArch", "b.arch": "BArch",
    "bachelor of architecture": "BArch",
    "bmus": "BMus", "b.mus.": "BMus",
    "bba": "BBA",
    "llb": "LLB",
    # --- Bachelor generic / national variants ---
    "bachelor": "Bachelor", "bachelors": "Bachelor", "bachelor's": "Bachelor",
    "bachelor (honours)": "Bachelor", "bachelor honours": "Bachelor",
    "bachelor honors": "Bachelor", "bachelor (hons)": "Bachelor",
    "licence": "Licenciatura", "license": "Licenciatura",
    "licenciatura": "Licenciatura",
    "titulo profesional": "Licenciatura", "título profesional": "Licenciatura",
    # Bachelor long tail (art/design corpus)
    "bed": "BEd", "b.ed.": "BEd", "b.ed": "BEd",
    "bachelor of education": "BEd",
    "benvd": "BEnvD", "b.envd.": "BEnvD",
    "bachelor of environmental design": "BEnvD",
    "bas": "BAS", "b.a.s.": "BAS",
    "bachelor of applied science": "BAS",
    "baft": "BAFT",
    "bva": "BVA", "b.v.a.": "BVA",
    "bachelor of visual arts": "BVA",
    "bdi": "BDI",
    "bid": "BID",
    "bachelor of interior design": "BID",
    "bachelor of industrial design": "BID",
    "specialist": "Specialist", "specialist degree": "Specialist",
    "specialist diploma": "Specialist",
    # --- Master specifics ---
    "ma": "MA", "m.a.": "MA", "m.a": "MA",
    "master of arts": "MA",
    "msc": "MSc", "m.sc.": "MSc", "m.sc": "MSc",
    "master of science": "MSc",
    "ms": "MS", "m.s.": "MS", "m.s": "MS",
    "mfa": "MFA", "m.f.a.": "MFA", "m.f.a": "MFA",
    "master of fine arts": "MFA",
    "mdes": "MDes", "m.des.": "MDes", "m.des": "MDes",
    "master of design": "MDes",
    "march": "MArch", "m.arch.": "MArch", "m.arch": "MArch",
    "master of architecture": "MArch",
    "meng": "MEng", "m.eng.": "MEng", "m.eng": "MEng",
    "mphil": "MPhil", "m.phil.": "MPhil", "m.phil": "MPhil",
    "mba": "MBA", "m.b.a.": "MBA",
    "mres": "MRes",
    "mmus": "MMus", "m.mus.": "MMus",
    "llm": "LLM",
    # Master long tail (art/design corpus)
    "mlitt": "MLitt", "m.litt.": "MLitt", "m.litt": "MLitt",
    "master of letters": "MLitt",
    "mpa": "MPA", "m.p.a.": "MPA",
    "mvs": "MVS", "m.v.s.": "MVS",
    "master of visual studies": "MVS",
    "mdi": "MDI",
    "mid": "MID",
    "master of interior design": "MID",
    "master of industrial design": "MID",
    "meisterschüler": "Meisterschüler", "meisterschülerin": "Meisterschüler",
    "meisterschüler*in": "Meisterschüler", "meisterschueler": "Meisterschüler",
    "absolvent/meisterschüler*in": "Meisterschüler",
    "absolvent / meisterschüler*in": "Meisterschüler",
    # --- Master generic ---
    "master": "Master", "masters": "Master", "master's": "Master",
    # --- Doctorate ---
    "phd": "PhD", "ph.d.": "PhD", "ph.d": "PhD", "ph d": "PhD",
    "doctor of philosophy": "PhD",
    "dphil": "DPhil", "d.phil.": "DPhil", "d.phil": "DPhil",
    "edd": "EdD", "ed.d.": "EdD",
    "md": "MD", "m.d.": "MD",
    "dfa": "DFA", "d.f.a.": "DFA", "doctor of fine arts": "DFA",
    "doctorate": "Doctorate", "doctoral": "Doctorate",
    # --- Diploma / certificate tier ---
    "diploma": "Diploma",
    "higher diploma": "Higher Diploma",
    "hnd": "HND", "higher national diploma": "HND",
    "certificate": "Certificate",
    "diploma/certificate": "Diploma",
    "pgdip": "PGDip", "postgraduate diploma": "PGDip",
    "pgcert": "PGCert", "postgraduate certificate": "PGCert",
    "foundation": "Foundation", "foundation diploma": "Foundation",
    "afa": "AFA", "a.f.a.": "AFA", "associate of fine arts": "AFA",
    # --- Combined-degree raw spellings ---
    # Whole-string aliases for combined patterns whose raw text differs from
    # the canonical "<part1>/<part2>" form (e.g. "BDesign/MArch" -> "BDes/MArch").
    "bdesign/march": "BDes/MArch",
    "bdesign / march": "BDes/MArch",
}

# Honours markers — stripped before canonical lookup, flag is set separately.
_HONOURS_RE = re.compile(
    r"\(\s*hons\.?\s*\)|\(\s*honou?rs\s*\)|\bhonou?rs\b|\bhons\b",
    re.IGNORECASE,
)

# Separators that signal a joint/double degree: "/", "+", " & ", " and ".
_SPLIT_RE = re.compile(r"\s*[\/+]\s*|\s+&\s+|\s+and\s+", re.IGNORECASE)

# Values that should collapse to None rather than being treated as a real degree.
_SENTINEL_VALUES = {"other", "unknown", "n/a", "na", "null", "none", "-", "—"}


def _collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _lookup_canonical(token: str) -> Optional[str]:
    """Return the canonical code for a single degree token, or None.

    Combined codes (containing "/") are not returned by this helper — they are
    only valid when emitted via the combined-degree path in `normalize_degree`.
    """
    stripped = _collapse_whitespace(token)
    if not stripped:
        return None
    low = stripped.lower()
    if low in _ALIASES:
        return _ALIASES[low]
    # Allow already-canonical forms to pass through (case-insensitive match).
    for canonical in _CANONICAL_FAMILY:
        if canonical.lower() == low:
            return canonical
    return None


def _empty_result() -> DegreeFields:
    return {"normalized_degree_type": None, "honours_flag": False}


def normalize_degree(raw: Optional[str]) -> DegreeFields:
    """Map a raw degree string to a controlled-vocabulary code + honours flag.

    Returns `{normalized_degree_type: None, honours_flag: False}` for sentinel
    inputs ("Other", null, "Unknown"…) and for unknown combined-degree
    patterns. Single-degree fallthroughs that we cannot map confidently also
    return None — these surface as NULLs in the table for human review.
    """
    if raw is None:
        return _empty_result()

    text = _collapse_whitespace(str(raw))
    if not text or text.lower() in _SENTINEL_VALUES:
        return _empty_result()

    honours = bool(_HONOURS_RE.search(text))
    cleaned = _collapse_whitespace(_HONOURS_RE.sub("", text))

    # Combined-degree path. Only emit a combined code when (a) every component
    # is individually canonical AND (b) the resulting compound is registered
    # in `_COMBINED_CATALOG`. New combinations must be added there explicitly.
    parts = [p for p in _SPLIT_RE.split(cleaned) if p.strip()]
    if len(parts) >= 2:
        canonical_parts = [_lookup_canonical(p) for p in parts]
        if all(canonical_parts):
            compound = "/".join(canonical_parts)  # type: ignore[arg-type]
            if compound in _COMBINED_CATALOG:
                return {"normalized_degree_type": compound, "honours_flag": honours}
            # Recognised parts but unregistered combination — fall through to
            # whole-string lookup (covers patterns like "BDesign/MArch" that
            # have a curated alias) and then to None for review.

    # Whole-string lookup. This handles non-split aliases ("Absolvent/Meisterschüler*in"
    # -> "Meisterschüler") and curated combined spellings ("BDesign/MArch" -> "BDes/MArch").
    direct = _ALIASES.get(cleaned.lower())
    if direct and (direct in _CANONICAL_FAMILY or direct in _COMBINED_CATALOG):
        return {"normalized_degree_type": direct, "honours_flag": honours}

    canonical = _lookup_canonical(cleaned)
    if canonical is not None and canonical in _CANONICAL_FAMILY:
        return {"normalized_degree_type": canonical, "honours_flag": honours}

    return {"normalized_degree_type": None, "honours_flag": honours}


def _combined_family(parts: list[str]) -> str:
    """Family for a combined code = highest-ranked family among its parts."""
    return max(
        (_CANONICAL_FAMILY[p] for p in parts),
        key=lambda f: _FAMILY_RANK[f],
    )


def iter_label_catalog() -> Iterator[DegreeLabel]:
    """Yield every controlled-vocabulary entry, single + combined.

    Used by `scripts/sync_degree_labels.py` to keep the `degree_labels`
    dictionary table aligned with this module. Single source of truth lives
    here; the table is a derived index.
    """
    for code, family in _CANONICAL_FAMILY.items():
        yield {
            "code": code,
            "family": family,
            "is_combined": False,
            "parts": None,
        }
    for code, parts in _COMBINED_CATALOG.items():
        yield {
            "code": code,
            "family": _combined_family(parts),
            "is_combined": True,
            "parts": list(parts),
        }
