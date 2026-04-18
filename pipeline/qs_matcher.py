"""
Multi-stage school name matcher for QS rankings lookup.

Pipeline
--------
1. Normalize  — unicode-fold, lowercase, expand abbreviations
2. Tokenize   — split into generic vs informative (core) vs rare tokens
3. Alias      — exact alias-table override before any fuzzy work
4. Block      — narrow candidates by country index + rare-token index
5. Score      — multiple features: full-name fuzzy, core Jaccard, rare Jaccard,
                 subset bonus, acronym match, country match/conflict
6. Decide     — three bands: auto_match / manual_review / auto_reject
7. Report     — near-miss details for the caller to log

Design principles
-----------------
- Country conflict crushes the score regardless of name similarity.
- Token overlap alone on generic words ("art university of") scores near zero
  because those tokens are excluded from core/rare features.
- The alias table (data/qs_aliases.json) overrides everything; add entries
  there for abbreviations, translated names, and historical variants.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_DATA_DIR = Path(__file__).parent.parent / "data"
ALIAS_FILE = _DATA_DIR / "qs_aliases.json"

# ---------------------------------------------------------------------------
# Generic tokens — carry little discriminative power
# ---------------------------------------------------------------------------
GENERIC_TOKENS: frozenset[str] = frozenset({
    # Institution types
    "university", "college", "institute", "institution", "school", "academy",
    "faculty", "department", "center", "centre", "campus", "polytechnic",
    # Qualifiers
    "national", "international", "state", "public", "private", "federal",
    "technical", "technology", "technologies", "applied", "advanced",
    "higher", "graduate", "postgraduate", "royal",
    # Art-domain (appear in nearly every entry in the art subject sheets)
    "art", "arts", "design", "fine", "visual", "creative",
    # Prepositions / articles (all languages common in institution names)
    "of", "the", "and", "for", "in", "at", "a", "an",
    "de", "di", "du", "van", "der", "und", "et", "y", "la", "le", "los",
})

# ---------------------------------------------------------------------------
# Abbreviation expansion
# ---------------------------------------------------------------------------
_ABBREV: dict[str, str] = {
    "univ": "university",
    "univs": "university",
    "inst": "institute",
    "coll": "college",
    "tech": "technology",
    "natl": "national",
    "intl": "international",
    "sci": "science",
    "eng": "engineering",
}

# ---------------------------------------------------------------------------
# Country name normalisation
# ---------------------------------------------------------------------------
_COUNTRY_NORM: dict[str, str] = {
    # United States (Latin variants)
    "usa": "united states",
    "us": "united states",
    "u s": "united states",
    "united states of america": "united states",
    # United States (Chinese — DB uses region categories, all map to US)
    "美国": "united states",
    "南方与西南": "united states",   # Southern & Southwest US region
    "中西部旗舰": "united states",   # Midwest flagship
    "加州旗舰": "united states",     # California flagship
    "东北强校": "united states",     # Northeast
    "波多黎各": "united states",     # Puerto Rico (US territory)
    # United Kingdom
    "uk": "united kingdom",
    "england": "united kingdom",
    "great britain": "united kingdom",
    "britain": "united kingdom",
    "scotland": "united kingdom",
    "wales": "united kingdom",
    "英国": "united kingdom",
    # China — QS uses "China (Mainland)" → normalises to "china mainland"
    "china mainland": "china",
    "mainland china": "china",
    "p r china": "china",
    "peoples republic of china": "china",
    "people s republic of china": "china",
    "中国": "china",
    # Hong Kong — QS uses "Hong Kong SAR, China"
    "hong kong sar": "hong kong",
    "hong kong sar china": "hong kong",
    "hong kong s a r": "hong kong",
    # Macao
    "macao sar china": "macao",
    "macau": "macao",
    # Korea
    "south korea": "korea",
    "republic of korea": "korea",
    "韩国": "korea",
    # Other Chinese country names used in the DB
    "加拿大": "canada",
    "澳大利亚": "australia",
    "日本": "japan",
    "德国": "germany",
    "法国": "france",
    "荷兰": "netherlands",
    "意大利": "italy",
    "新加坡": "singapore",
    "新西兰": "new zealand",
    "巴西": "brazil",
    "阿根廷": "argentina",
    "墨西哥": "mexico",
    "尼日利亚": "nigeria",
    "埃及": "egypt",
    "南非": "south africa",
    "肯尼亚": "kenya",
    "加纳": "ghana",
    "苏丹": "sudan",
    "乌干达": "uganda",
    "埃塞俄比亚": "ethiopia",
    "坦桑尼亚": "tanzania",
    "津巴布韦": "zimbabwe",
    "尼加拉瓜": "nicaragua",
    "危地马拉": "guatemala",
    "洪都拉斯": "honduras",
    "萨尔瓦多": "el salvador",
    "哥斯达黎加": "costa rica",
    "巴拿马": "panama",
    "摩洛哥": "morocco",
    "突尼斯": "tunisia",
    "阿尔及利亚": "algeria",
    # Other QS-specific Latin variants
    "russian federation": "russia",
    "iran islamic republic of": "iran",
    "venezuela bolivarian republic of": "venezuela",
    "syrian arab republic": "syria",
    "brunei darussalam": "brunei",
    "viet nam": "vietnam",
    "turkiye": "turkey",
    "czechia": "czech republic",
    "uae": "united arab emirates",
    # Taiwan
    "republic of china": "taiwan",
}

# ---------------------------------------------------------------------------
# Confidence thresholds
# ---------------------------------------------------------------------------
CONF_AUTO_MATCH = 0.80     # above → use the rank
CONF_AUTO_REJECT = 0.45    # below → treat as not found
COUNTRY_CONFLICT_MULT = 0.12   # multiplier when countries are known and differ


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def parse_rank(raw) -> Optional[int]:
    """'1' | '51-100' | '=15' | '101+' → lower-bound int, or None."""
    s = str(raw).strip().replace("=", "").replace("+", "")
    s = s.split("-")[0]
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def normalize_name(name: str) -> str:
    """Unicode-fold → lowercase → strip punctuation → expand abbreviations."""
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower()
    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    tokens = [_ABBREV.get(t, t) for t in name.split()]
    return " ".join(tokens)


def _tokenize(norm: str) -> frozenset[str]:
    return frozenset(norm.split())


def _core(tokens: frozenset[str]) -> frozenset[str]:
    return tokens - GENERIC_TOKENS


def _extract_acronym(name: str) -> str:
    """'Massachusetts Institute of Technology (MIT)' → 'mit'."""
    m = re.search(r"\(([A-Z]{2,7})\)", name)
    if m:
        return m.group(1).lower()
    words = re.findall(r"\b[A-Z][a-z]+", name)
    if len(words) >= 3:
        return "".join(w[0] for w in words).lower()
    return ""


def norm_country(country: str) -> str:
    if not country:
        return ""
    # Try the raw value first for Chinese characters (NFKD leaves them unchanged)
    raw_stripped = country.strip()
    if raw_stripped in _COUNTRY_NORM:
        return _COUNTRY_NORM[raw_stripped]
    # Unicode-fold (handles Türkiye → Turkiye, accented Latin, etc.)
    c = unicodedata.normalize("NFKD", country)
    c = "".join(ch for ch in c if not unicodedata.combining(ch))
    c = c.lower().strip()
    c = re.sub(r"[^\w\s]", " ", c)   # strip parens, commas, dots
    c = re.sub(r"\s+", " ", c).strip()
    result = _COUNTRY_NORM.get(c, c)
    # If the result still contains non-ASCII it is an unrecognised / vague
    # category string (e.g. "其他非洲国家", "北欧") — treat as unknown so the
    # matcher does not apply a false country-conflict penalty.
    if not result.isascii():
        return ""
    return result


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class _Entry:
    original: str
    norm: str
    tokens: frozenset[str]
    core: frozenset[str]
    rare: frozenset[str]      # filled after full index is built
    country: str              # normalised
    rank: Optional[int]
    acronym: str


@dataclass
class MatchResult:
    rank: Optional[int]
    qs_name: Optional[str]
    confidence: float
    features: dict
    band: str   # "auto_match" | "manual_review" | "auto_reject"


# ---------------------------------------------------------------------------
# QSIndex
# ---------------------------------------------------------------------------

class QSIndex:
    """
    Inverted index over one QS rankings DataFrame.

    Parameters
    ----------
    df          : DataFrame with institution names + ranks (and optionally country)
    inst_col    : column containing institution name
    rank_col    : column containing rank value
    country_col : optional column containing country/territory
    """

    def __init__(
        self,
        df: pd.DataFrame,
        inst_col: str,
        rank_col: str,
        country_col: Optional[str] = None,
    ) -> None:
        self._entries: list[_Entry] = []
        self._country_idx: dict[str, list[int]] = {}
        self._token_idx: dict[str, list[int]] = {}
        self._alias_idx: dict[str, int] = {}   # normalised DB name → entry index

        # --- Pass 1: build entries (rare field populated in pass 2) ---
        for _, row in df.iterrows():
            raw = row.get(inst_col, "")
            if not raw or str(raw).lower() in ("nan", "none", ""):
                continue
            raw = str(raw)
            norm = normalize_name(raw)
            tokens = _tokenize(norm)
            core = _core(tokens)
            country = ""
            if country_col and country_col in df.columns:
                cv = row.get(country_col, "")
                if pd.notna(cv):
                    country = norm_country(str(cv))
            rank = parse_rank(row.get(rank_col)) if rank_col in df.columns else None
            acr = _extract_acronym(raw)

            idx = len(self._entries)
            self._entries.append(_Entry(
                original=raw, norm=norm, tokens=tokens, core=core,
                rare=frozenset(), country=country, rank=rank, acronym=acr,
            ))
            if country:
                self._country_idx.setdefault(country, []).append(idx)
            for t in tokens:
                self._token_idx.setdefault(t, []).append(idx)

        # --- Pass 2: compute rare tokens (appear in <5 % of entries) ---
        n = max(len(self._entries), 1)
        self._rare: frozenset[str] = frozenset(
            t for t, postings in self._token_idx.items()
            if len(postings) / n < 0.05 and t not in GENERIC_TOKENS
        )
        for e in self._entries:
            e.rare = e.tokens & self._rare

        # --- Load alias table ---
        self._load_aliases()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def match(self, name_en: str, school_country: str = "") -> MatchResult:
        """Return the best MatchResult for *name_en* against this index."""
        norm = normalize_name(name_en)

        # Alias override — highest priority
        if norm in self._alias_idx:
            e = self._entries[self._alias_idx[norm]]
            return MatchResult(
                rank=e.rank, qs_name=e.original, confidence=1.0,
                features={"alias_match": True}, band="auto_match",
            )

        tokens = _tokenize(norm)
        core = _core(tokens)
        rare = tokens & self._rare
        acr = _extract_acronym(name_en)
        country = norm_country(school_country)

        candidates = self._block(country, tokens, rare)

        best_conf = -1.0
        best_entry: Optional[_Entry] = None
        best_feats: dict = {}

        for idx in candidates:
            e = self._entries[idx]
            feats = self._score(norm, tokens, core, rare, acr, country, e)
            if feats["_confidence"] > best_conf:
                best_conf = feats["_confidence"]
                best_entry = e
                best_feats = feats

        if best_entry is None:
            return MatchResult(None, None, 0.0, {}, "auto_reject")

        band = (
            "auto_match"    if best_conf >= CONF_AUTO_MATCH  else
            "manual_review" if best_conf >= CONF_AUTO_REJECT else
            "auto_reject"
        )
        return MatchResult(
            rank=best_entry.rank if band == "auto_match" else None,
            qs_name=best_entry.original,
            confidence=min(1.0, round(best_conf, 4)),   # cap here for display
            features=best_feats,
            band=band,
        )

    # ------------------------------------------------------------------
    # Blocking
    # ------------------------------------------------------------------

    def _block(
        self,
        country: str,
        tokens: frozenset[str],
        rare: frozenset[str],
    ) -> set[int]:
        candidates: set[int] = set()

        # Same country
        if country and country in self._country_idx:
            candidates.update(self._country_idx[country])

        # Shared rare tokens
        for t in rare:
            if t in self._token_idx:
                candidates.update(self._token_idx[t])

        # Shared informative-but-not-rare tokens (skip if too frequent)
        informative = tokens - GENERIC_TOKENS - self._rare
        for t in informative:
            postings = self._token_idx.get(t, [])
            if len(postings) < 50:
                candidates.update(postings)

        # Fallback: compare all (only triggers when blocking returns nothing)
        if not candidates:
            candidates = set(range(len(self._entries)))

        return candidates

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _score(
        self,
        school_norm: str,
        school_tokens: frozenset[str],
        school_core: frozenset[str],
        school_rare: frozenset[str],
        school_acr: str,
        school_country: str,
        rec: _Entry,
    ) -> dict:
        # F1: full-name fuzzy (token_set handles superset names)
        full = fuzz.token_set_ratio(school_norm, rec.norm) / 100.0

        # F2: core-token Jaccard (ignores generic words entirely)
        c_inter = school_core & rec.core
        c_union = school_core | rec.core
        core_j = len(c_inter) / max(len(c_union), 1)

        # F3: rare-token Jaccard (rewards matching unique words)
        r_inter = school_rare & rec.rare
        r_union = school_rare | rec.rare
        rare_j = len(r_inter) / max(len(r_union), 1)

        # F4: subset bonus — one core is wholly contained in the other
        #     e.g. rec.core={"royal"} ⊆ school_core={"royal","london"}
        subset = bool(
            (school_core or rec.core)   # at least one side has core tokens
            and (school_core.issubset(rec.core) or rec.core.issubset(school_core))
        )

        # F5: acronym match
        acr_match = bool(school_acr and rec.acronym and school_acr == rec.acronym)

        # F6: country
        has_both = bool(school_country) and bool(rec.country)
        country_match    = has_both and school_country == rec.country
        country_conflict = has_both and school_country != rec.country

        # --- Adaptive weights based on core-set richness ---
        # When core/rare sets are empty or tiny, those features are unreliable
        # (e.g. "Royal College of Art" — every token is generic).
        # Shift weight onto full-name fuzzy score in those cases so that
        # city-name-only overlap cannot override full-name evidence.
        core_union_size = len(c_union)
        if core_union_size == 0:
            # Both names are entirely generic tokens.
            w_full, w_info, info = 0.85, 0.00, 0.0
        elif core_union_size <= 2:
            # Thin core (e.g. just a city). City alone must not dominate.
            w_full, w_info = 0.55, 0.25
            info = core_j * 0.50 + rare_j * 0.50
        else:
            # Rich core — discriminative tokens are plentiful.
            w_full, w_info = 0.28, 0.45
            info = core_j * 0.45 + rare_j * 0.55

        # When full_score is very high AND one name clearly subsumes the other
        # (e.g. "Royal College of Art London" ↔ "Royal College of Art"), trust
        # the fuzzy score more: raise w_full so this candidate beats rivals that
        # only share a city token (core_j=1.0 on a thin {"london"} set).
        strong_superset = full >= 0.90 and subset
        if strong_superset:
            w_full = max(w_full, 0.72)

        base = (
            w_full * full
            + w_info * info
            + (0.12 if strong_superset else 0.08 if subset else 0.0)
            + (0.07 if acr_match else 0.0)
        )

        if country_conflict:
            base *= COUNTRY_CONFLICT_MULT     # near-zero; only an exact name can survive
        elif country_match:
            base *= 1.10

        # NOTE: confidence is NOT capped here so that candidates with higher
        # raw scores rank above each other correctly (e.g. exact match beats a
        # near-match). The cap to [0, 1] is applied in match() before returning.
        return {
            "full_score":       round(full, 3),
            "core_jaccard":     round(core_j, 3),
            "rare_jaccard":     round(rare_j, 3),
            "core_tokens":      core_union_size,
            "subset_match":     subset,
            "country_match":    country_match,
            "country_conflict": country_conflict,
            "acr_match":        acr_match,
            "_confidence":      round(base, 4),
        }

    # ------------------------------------------------------------------
    # Alias loading
    # ------------------------------------------------------------------

    def _load_aliases(self) -> None:
        if not ALIAS_FILE.exists():
            return
        try:
            with open(ALIAS_FILE) as f:
                raw: dict = json.load(f)
        except (json.JSONDecodeError, OSError):
            return
        norm_entry_map = {e.norm: i for i, e in enumerate(self._entries)}
        for db_name, qs_name in raw.items():
            if db_name.startswith("_"):
                continue  # skip comment / metadata keys
            norm_qs = normalize_name(qs_name)
            idx = norm_entry_map.get(norm_qs)
            if idx is not None:
                self._alias_idx[normalize_name(db_name)] = idx
