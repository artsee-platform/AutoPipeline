"""Canonical normalization for `programs.raw_degree_type`.

Takes the free-text degree label (e.g. "B.Des.", "BA (Hons)", "BA/MArch",
"Licenciatura", "Ph.D.", "Other") and produces five structured fields:

    normalized_degree_type : canonical short label (BDes, PhD, BA, Master, ...)
                             — None when the raw text cannot be identified.
    degree_family          : Bachelor / Master / Doctorate / Diploma / Other
    honours_flag           : True when the raw text carries (Hons)/(Honours).
    combined_degree_flag   : True for joint/double degrees (e.g. "BA/MArch").
    combined_with          : Canonical parts of a combined degree, else None.

Design note: the raw text itself is kept in `raw_degree_type`. We never mutate
or discard it here — downstream analytics use the canonical fields, but the
original wording stays available for audits and edge-case review.

This mirrors the design described in the `programs.degree_type` cleanup plan:
preserve raw → add normalized + family + flags → so joint degrees, honours
markers, and national-system variants (Licenciatura / Licence) stop
contaminating a single mixed-layer column.
"""
from __future__ import annotations

import re
from typing import Optional, TypedDict


class DegreeFields(TypedDict):
    normalized_degree_type: Optional[str]
    degree_family: str
    honours_flag: bool
    combined_degree_flag: bool
    combined_with: Optional[list[str]]


BACHELOR = "Bachelor"
MASTER = "Master"
DOCTORATE = "Doctorate"
DIPLOMA = "Diploma"
OTHER = "Other"

# Highest-wins ordering for combined-degree family resolution.
_FAMILY_RANK = {OTHER: 0, DIPLOMA: 1, BACHELOR: 2, MASTER: 3, DOCTORATE: 4}

# Canonical label -> family. Keys are the values written to
# normalized_degree_type; add new canonicals here when the catalog grows.
_CANONICAL_FAMILY: dict[str, str] = {
    # Bachelor-level (specific)
    "BA": BACHELOR, "BSc": BACHELOR, "BS": BACHELOR, "BFA": BACHELOR,
    "BDes": BACHELOR, "BEng": BACHELOR, "BArch": BACHELOR, "BMus": BACHELOR,
    "BBA": BACHELOR, "LLB": BACHELOR,
    # Bachelor-level (specific, art/design long tail)
    "BEd": BACHELOR, "BEnvD": BACHELOR, "BAS": BACHELOR, "BAFT": BACHELOR,
    "BVA": BACHELOR, "BDI": BACHELOR, "BID": BACHELOR,
    # Bachelor-level (generic / national variants)
    "Bachelor": BACHELOR,
    "Licenciatura": BACHELOR,
    "Specialist": BACHELOR,  # 5-year post-Soviet undergrad, mirrors Licenciatura
    # Master-level (specific)
    "MA": MASTER, "MSc": MASTER, "MS": MASTER, "MFA": MASTER,
    "MDes": MASTER, "MArch": MASTER, "MEng": MASTER, "MPhil": MASTER,
    "MBA": MASTER, "MRes": MASTER, "MMus": MASTER, "LLM": MASTER,
    # Master-level (specific, art/design long tail)
    "MLitt": MASTER, "MPA": MASTER, "MVS": MASTER, "MDI": MASTER, "MID": MASTER,
    "Meisterschüler": MASTER,  # German art academy post-diploma qualification
    # Master-level (generic)
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

# Raw-variant (lowercased) -> canonical label. Punctuation and case are
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
}

# Honours markers — stripped before canonical lookup, flag is set separately.
_HONOURS_RE = re.compile(
    r"\(\s*hons\.?\s*\)|\(\s*honou?rs\s*\)|\bhonou?rs\b|\bhons\b",
    re.IGNORECASE,
)

# Separators that signal a joint/double degree: "/", "+", " & ", " and ".
_SPLIT_RE = re.compile(r"\s*[\/+]\s*|\s+&\s+|\s+and\s+", re.IGNORECASE)

# Values that should collapse to Other/None rather than being treated as a real degree.
_SENTINEL_VALUES = {"other", "unknown", "n/a", "na", "null", "none", "-", "—"}


def _empty_result() -> DegreeFields:
    return {
        "normalized_degree_type": None,
        "degree_family": OTHER,
        "honours_flag": False,
        "combined_degree_flag": False,
        "combined_with": None,
    }


def _collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _lookup_canonical(token: str) -> Optional[str]:
    """Return the canonical label for a single degree token, or None."""
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


def _infer_family_from_keywords(text: str) -> str:
    """Last-resort family inference when no canonical match is found."""
    low = text.lower()
    if "phd" in low or "doctor" in low or "ph.d" in low:
        return DOCTORATE
    if "master" in low:
        return MASTER
    if "bachelor" in low or "licenci" in low:
        return BACHELOR
    if "diploma" in low or "certific" in low or "hnd" in low or "foundation" in low:
        return DIPLOMA
    return OTHER


def normalize_degree(raw: Optional[str]) -> DegreeFields:
    """Map a raw degree string to the five-field canonical scheme.

    Unknown or sentinel inputs ("Other", null, empty) collapse to
    `{normalized: None, family: Other, flags: False, combined_with: None}`.
    """
    if raw is None:
        return _empty_result()

    text = _collapse_whitespace(str(raw))
    if not text or text.lower() in _SENTINEL_VALUES:
        return _empty_result()

    honours = bool(_HONOURS_RE.search(text))
    cleaned = _collapse_whitespace(_HONOURS_RE.sub("", text))

    # Combined / joint degree: split on / + & "and", require every part to be
    # recognised before committing to the combined path — otherwise we'd over-
    # match phrases like "Art and Design".
    parts = [p for p in _SPLIT_RE.split(cleaned) if p.strip()]
    if len(parts) >= 2:
        canonical_parts = [_lookup_canonical(p) for p in parts]
        if all(canonical_parts):
            top = max(canonical_parts, key=lambda c: _FAMILY_RANK[_CANONICAL_FAMILY[c]])
            return {
                "normalized_degree_type": "/".join(canonical_parts),
                "degree_family": _CANONICAL_FAMILY[top],
                "honours_flag": honours,
                "combined_degree_flag": True,
                "combined_with": canonical_parts,
            }

    canonical = _lookup_canonical(cleaned)
    if canonical is not None:
        return {
            "normalized_degree_type": canonical,
            "degree_family": _CANONICAL_FAMILY[canonical],
            "honours_flag": honours,
            "combined_degree_flag": False,
            "combined_with": None,
        }

    # No canonical hit — keep normalized as None so the row is easy to surface
    # for review, but still bucket it into a reasonable family.
    return {
        "normalized_degree_type": None,
        "degree_family": _infer_family_from_keywords(cleaned),
        "honours_flag": honours,
        "combined_degree_flag": False,
        "combined_with": None,
    }
