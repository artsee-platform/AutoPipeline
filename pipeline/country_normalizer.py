"""Canonical normalization for `schools.country` (free-text) into two fields:

    country_code : ISO 3166-1 alpha-2 (e.g. "US", "GB", "CN") — FK to public.countries
    region_tag   : product-curated grouping (e.g. "us_california_flagship",
                   "nordics") — FK to public.region_tags

Why two fields?
---------------
The legacy `country` column conflated two semantic dimensions:

  * Real sovereign nations            — "加拿大", "英国", "中国"
  * Curated marketing/product buckets — "加州旗舰", "中西部旗舰", "北欧",
                                        "其他亚洲国家"

The buckets are useful for the front-end "recommend by region" UI, but they
are not countries. Splitting them lets us:

  * Have a clean ISO-coded `country_code` for filters, exports, integrations.
  * Keep the curated `region_tag` for product use without polluting the
    country dimension.

Some buckets imply a country (the four US-internal flagship buckets all live
in the US), so the resolver fills both fields when it can. The five
"其他XX国家" / "北欧" buckets cross multiple countries — those leave
country_code NULL for manual review.

Public API
----------
`resolve_country(raw)` returns `{country_code, region_tag}` for a raw schools
text label. Suitable for the backfill script and any future ingestion path.

`iter_country_catalog()` yields the controlled vocabulary for `countries`.
`iter_region_tag_catalog()` yields the controlled vocabulary for `region_tags`.
Both are consumed by `scripts/sync_country_dictionaries.py`.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Iterator, Optional, TypedDict


class CountryFields(TypedDict):
    country_code: Optional[str]
    region_tag: Optional[str]


class CountryRecord(TypedDict):
    code: str
    name_en: str
    name_zh: Optional[str]
    region_continent: Optional[str]
    sort_order: int


class RegionTagRecord(TypedDict):
    code: str
    name_en: str
    name_zh: Optional[str]
    scope: str                       # 'us_subregion' or 'multi_country_bucket'
    implied_country_code: Optional[str]  # filled when scope is single-country
    sort_order: int


# Continent buckets — used both for `countries.region_continent` and to drive
# UI groupings ("Asia", "Europe"…).
NA = "North America"
SA = "South America"
EU = "Europe"
AS = "Asia"
AF = "Africa"
OC = "Oceania"

# Master country list. Subset of ISO 3166-1 alpha-2: all 43 values currently
# present in `schools.country` plus the major art / design / architecture
# producing nations. Adding a new country = append a row here, then run
# `python -m scripts.sync_country_dictionaries`.
#
# Format: (iso_code, name_en, name_zh, continent)
# Sort order is derived from list position * 10 in `iter_country_catalog`.
_COUNTRY_DATA: list[tuple[str, str, Optional[str], str]] = [
    # --- North America ---
    ("US", "United States",         "美国",       NA),
    ("CA", "Canada",                "加拿大",     NA),
    ("MX", "Mexico",                "墨西哥",     NA),
    ("PR", "Puerto Rico",           "波多黎各",   NA),  # US territory; ISO assigns separate code
    ("CU", "Cuba",                  "古巴",       NA),
    ("DO", "Dominican Republic",    "多米尼加",   NA),
    ("HT", "Haiti",                 "海地",       NA),
    ("JM", "Jamaica",               "牙买加",     NA),
    ("BS", "Bahamas",               "巴哈马",     NA),
    ("TT", "Trinidad and Tobago",   "特立尼达和多巴哥", NA),
    ("BZ", "Belize",                "伯利兹",     NA),
    ("CR", "Costa Rica",            "哥斯达黎加", NA),
    ("GT", "Guatemala",             "危地马拉",   NA),
    ("HN", "Honduras",              "洪都拉斯",   NA),
    ("NI", "Nicaragua",             "尼加拉瓜",   NA),
    ("PA", "Panama",                "巴拿马",     NA),
    ("SV", "El Salvador",           "萨尔瓦多",   NA),
    # --- South America ---
    ("AR", "Argentina",             "阿根廷",     SA),
    ("BR", "Brazil",                "巴西",       SA),
    ("CL", "Chile",                 "智利",       SA),
    ("CO", "Colombia",              "哥伦比亚",   SA),
    ("EC", "Ecuador",               "厄瓜多尔",   SA),
    ("PE", "Peru",                  "秘鲁",       SA),
    ("UY", "Uruguay",               "乌拉圭",     SA),
    ("PY", "Paraguay",              "巴拉圭",     SA),
    ("BO", "Bolivia",               "玻利维亚",   SA),
    ("VE", "Venezuela",             "委内瑞拉",   SA),
    ("GY", "Guyana",                "圭亚那",     SA),
    ("SR", "Suriname",              "苏里南",     SA),
    # --- Europe ---
    ("GB", "United Kingdom",        "英国",       EU),
    ("IE", "Ireland",               "爱尔兰",     EU),
    ("FR", "France",                "法国",       EU),
    ("DE", "Germany",               "德国",       EU),
    ("IT", "Italy",                 "意大利",     EU),
    ("ES", "Spain",                 "西班牙",     EU),
    ("PT", "Portugal",              "葡萄牙",     EU),
    ("NL", "Netherlands",           "荷兰",       EU),
    ("BE", "Belgium",               "比利时",     EU),
    ("LU", "Luxembourg",            "卢森堡",     EU),
    ("CH", "Switzerland",           "瑞士",       EU),
    ("AT", "Austria",               "奥地利",     EU),
    ("DK", "Denmark",               "丹麦",       EU),
    ("SE", "Sweden",                "瑞典",       EU),
    ("NO", "Norway",                "挪威",       EU),
    ("FI", "Finland",               "芬兰",       EU),
    ("IS", "Iceland",               "冰岛",       EU),
    ("EE", "Estonia",               "爱沙尼亚",   EU),
    ("LV", "Latvia",                "拉脱维亚",   EU),
    ("LT", "Lithuania",             "立陶宛",     EU),
    ("PL", "Poland",                "波兰",       EU),
    ("CZ", "Czechia",               "捷克",       EU),
    ("SK", "Slovakia",              "斯洛伐克",   EU),
    ("HU", "Hungary",               "匈牙利",     EU),
    ("RO", "Romania",               "罗马尼亚",   EU),
    ("BG", "Bulgaria",              "保加利亚",   EU),
    ("GR", "Greece",                "希腊",       EU),
    ("MT", "Malta",                 "马耳他",     EU),
    ("CY", "Cyprus",                "塞浦路斯",   EU),
    ("HR", "Croatia",               "克罗地亚",   EU),
    ("SI", "Slovenia",              "斯洛文尼亚", EU),
    ("RS", "Serbia",                "塞尔维亚",   EU),
    ("BA", "Bosnia and Herzegovina","波黑",       EU),
    ("ME", "Montenegro",            "黑山",       EU),
    ("MK", "North Macedonia",       "北马其顿",   EU),
    ("AL", "Albania",               "阿尔巴尼亚", EU),
    ("UA", "Ukraine",               "乌克兰",     EU),
    ("BY", "Belarus",               "白俄罗斯",   EU),
    ("MD", "Moldova",               "摩尔多瓦",   EU),
    ("RU", "Russia",                "俄罗斯",     EU),
    ("TR", "Türkiye",               "土耳其",     EU),
    # --- Asia ---
    ("CN", "China",                 "中国",       AS),
    ("HK", "Hong Kong",             "中国香港",   AS),
    ("MO", "Macao",                 "中国澳门",   AS),
    ("TW", "Taiwan",                "中国台湾",   AS),
    ("JP", "Japan",                 "日本",       AS),
    ("KR", "South Korea",           "韩国",       AS),
    ("KP", "North Korea",           "朝鲜",       AS),
    ("SG", "Singapore",             "新加坡",     AS),
    ("MY", "Malaysia",              "马来西亚",   AS),
    ("TH", "Thailand",              "泰国",       AS),
    ("VN", "Vietnam",               "越南",       AS),
    ("PH", "Philippines",           "菲律宾",     AS),
    ("ID", "Indonesia",             "印度尼西亚", AS),
    ("MM", "Myanmar",               "缅甸",       AS),
    ("KH", "Cambodia",              "柬埔寨",     AS),
    ("LA", "Laos",                  "老挝",       AS),
    ("BN", "Brunei",                "文莱",       AS),
    ("TL", "Timor-Leste",           "东帝汶",     AS),
    ("IN", "India",                 "印度",       AS),
    ("PK", "Pakistan",              "巴基斯坦",   AS),
    ("BD", "Bangladesh",            "孟加拉国",   AS),
    ("LK", "Sri Lanka",             "斯里兰卡",   AS),
    ("NP", "Nepal",                 "尼泊尔",     AS),
    ("BT", "Bhutan",                "不丹",       AS),
    ("MV", "Maldives",              "马尔代夫",   AS),
    ("AF", "Afghanistan",           "阿富汗",     AS),
    ("IR", "Iran",                  "伊朗",       AS),
    ("IQ", "Iraq",                  "伊拉克",     AS),
    ("IL", "Israel",                "以色列",     AS),
    ("PS", "Palestine",             "巴勒斯坦",   AS),
    ("JO", "Jordan",                "约旦",       AS),
    ("LB", "Lebanon",               "黎巴嫩",     AS),
    ("SY", "Syria",                 "叙利亚",     AS),
    ("SA", "Saudi Arabia",          "沙特阿拉伯", AS),
    ("AE", "United Arab Emirates",  "阿联酋",     AS),
    ("QA", "Qatar",                 "卡塔尔",     AS),
    ("KW", "Kuwait",                "科威特",     AS),
    ("BH", "Bahrain",               "巴林",       AS),
    ("OM", "Oman",                  "阿曼",       AS),
    ("YE", "Yemen",                 "也门",       AS),
    ("MN", "Mongolia",              "蒙古",       AS),
    ("KZ", "Kazakhstan",            "哈萨克斯坦", AS),
    ("UZ", "Uzbekistan",            "乌兹别克斯坦", AS),
    ("KG", "Kyrgyzstan",            "吉尔吉斯斯坦", AS),
    ("TJ", "Tajikistan",            "塔吉克斯坦", AS),
    ("TM", "Turkmenistan",          "土库曼斯坦", AS),
    ("AM", "Armenia",               "亚美尼亚",   AS),
    ("AZ", "Azerbaijan",            "阿塞拜疆",   AS),
    ("GE", "Georgia",               "格鲁吉亚",   AS),
    # --- Africa ---
    ("ZA", "South Africa",          "南非",       AF),
    ("EG", "Egypt",                 "埃及",       AF),
    ("MA", "Morocco",               "摩洛哥",     AF),
    ("TN", "Tunisia",               "突尼斯",     AF),
    ("DZ", "Algeria",               "阿尔及利亚", AF),
    ("LY", "Libya",                 "利比亚",     AF),
    ("SD", "Sudan",                 "苏丹",       AF),
    ("SS", "South Sudan",           "南苏丹",     AF),
    ("ET", "Ethiopia",              "埃塞俄比亚", AF),
    ("ER", "Eritrea",               "厄立特里亚", AF),
    ("KE", "Kenya",                 "肯尼亚",     AF),
    ("UG", "Uganda",                "乌干达",     AF),
    ("TZ", "Tanzania",              "坦桑尼亚",   AF),
    ("RW", "Rwanda",                "卢旺达",     AF),
    ("BI", "Burundi",               "布隆迪",     AF),
    ("SO", "Somalia",               "索马里",     AF),
    ("DJ", "Djibouti",              "吉布提",     AF),
    ("NG", "Nigeria",               "尼日利亚",   AF),
    ("GH", "Ghana",                 "加纳",       AF),
    ("CI", "Côte d'Ivoire",         "科特迪瓦",   AF),
    ("SN", "Senegal",               "塞内加尔",   AF),
    ("ML", "Mali",                  "马里",       AF),
    ("BF", "Burkina Faso",          "布基纳法索", AF),
    ("NE", "Niger",                 "尼日尔",     AF),
    ("TD", "Chad",                  "乍得",       AF),
    ("CM", "Cameroon",              "喀麦隆",     AF),
    ("CF", "Central African Republic", "中非共和国", AF),
    ("CG", "Congo (Brazzaville)",   "刚果（布）", AF),
    ("CD", "Congo (DRC)",           "刚果（金）", AF),
    ("GA", "Gabon",                 "加蓬",       AF),
    ("GQ", "Equatorial Guinea",     "赤道几内亚", AF),
    ("BJ", "Benin",                 "贝宁",       AF),
    ("TG", "Togo",                  "多哥",       AF),
    ("LR", "Liberia",               "利比里亚",   AF),
    ("SL", "Sierra Leone",          "塞拉利昂",   AF),
    ("GN", "Guinea",                "几内亚",     AF),
    ("GW", "Guinea-Bissau",         "几内亚比绍", AF),
    ("GM", "Gambia",                "冈比亚",     AF),
    ("CV", "Cabo Verde",            "佛得角",     AF),
    ("MR", "Mauritania",            "毛里塔尼亚", AF),
    ("AO", "Angola",                "安哥拉",     AF),
    ("ZM", "Zambia",                "赞比亚",     AF),
    ("ZW", "Zimbabwe",              "津巴布韦",   AF),
    ("MZ", "Mozambique",            "莫桑比克",   AF),
    ("MW", "Malawi",                "马拉维",     AF),
    ("MG", "Madagascar",            "马达加斯加", AF),
    ("MU", "Mauritius",             "毛里求斯",   AF),
    ("SC", "Seychelles",            "塞舌尔",     AF),
    ("KM", "Comoros",               "科摩罗",     AF),
    ("NA", "Namibia",               "纳米比亚",   AF),
    ("BW", "Botswana",              "博茨瓦纳",   AF),
    ("LS", "Lesotho",               "莱索托",     AF),
    ("SZ", "Eswatini",              "斯威士兰",   AF),
    # --- Oceania ---
    ("AU", "Australia",             "澳大利亚",   OC),
    ("NZ", "New Zealand",           "新西兰",     OC),
    ("FJ", "Fiji",                  "斐济",       OC),
    ("PG", "Papua New Guinea",      "巴布亚新几内亚", OC),
    ("WS", "Samoa",                 "萨摩亚",     OC),
    ("TO", "Tonga",                 "汤加",       OC),
    ("VU", "Vanuatu",               "瓦努阿图",   OC),
    ("SB", "Solomon Islands",       "所罗门群岛", OC),
]

# Index for fast lookup. Built once at import time.
_COUNTRY_NAMES: dict[str, tuple[str, Optional[str], str]] = {
    code: (name_en, name_zh, continent)
    for code, name_en, name_zh, continent in _COUNTRY_DATA
}

# Variant → ISO code. Lowercased keys; Chinese kept as-is. Add aliases here
# whenever a new spelling appears in source data.
_COUNTRY_ALIASES: dict[str, str] = {
    # --- US ---
    "us": "US", "u.s.": "US", "u.s.a.": "US", "usa": "US",
    "united states": "US", "united states of america": "US", "america": "US",
    "美国": "US",
    # --- UK ---
    "gb": "GB", "uk": "GB", "u.k.": "GB",
    "united kingdom": "GB", "great britain": "GB", "britain": "GB",
    "england": "GB", "scotland": "GB", "wales": "GB", "northern ireland": "GB",
    "英国": "GB",
    # --- China + SARs + Taiwan ---
    "china": "CN", "p.r. china": "CN", "p r china": "CN", "prc": "CN",
    "people's republic of china": "CN", "peoples republic of china": "CN",
    "china (mainland)": "CN", "mainland china": "CN", "china mainland": "CN",
    "中国": "CN", "中国大陆": "CN",
    "hong kong": "HK", "hong kong sar": "HK", "hong kong sar china": "HK",
    "hk": "HK", "香港": "HK", "中国香港": "HK",
    "macao": "MO", "macau": "MO", "macao sar china": "MO",
    "澳门": "MO", "中国澳门": "MO",
    "taiwan": "TW", "republic of china": "TW", "chinese taipei": "TW",
    "台湾": "TW", "中国台湾": "TW",
    # --- Korea ---
    "korea": "KR", "south korea": "KR", "republic of korea": "KR",
    "韩国": "KR", "南韩": "KR",
    "north korea": "KP", "dprk": "KP", "朝鲜": "KP",
    # --- Other Asia ---
    "japan": "JP", "日本": "JP",
    "singapore": "SG", "新加坡": "SG",
    "malaysia": "MY", "马来西亚": "MY",
    "thailand": "TH", "泰国": "TH",
    "vietnam": "VN", "viet nam": "VN", "越南": "VN",
    "philippines": "PH", "菲律宾": "PH",
    "indonesia": "ID", "印度尼西亚": "ID", "印尼": "ID",
    "myanmar": "MM", "burma": "MM", "缅甸": "MM",
    "cambodia": "KH", "柬埔寨": "KH",
    "laos": "LA", "老挝": "LA",
    "brunei": "BN", "brunei darussalam": "BN", "文莱": "BN",
    "india": "IN", "印度": "IN",
    "pakistan": "PK", "巴基斯坦": "PK",
    "bangladesh": "BD", "孟加拉国": "BD", "孟加拉": "BD",
    "sri lanka": "LK", "斯里兰卡": "LK",
    "nepal": "NP", "尼泊尔": "NP",
    "afghanistan": "AF", "阿富汗": "AF",
    "iran": "IR", "iran islamic republic of": "IR", "伊朗": "IR",
    "iraq": "IQ", "伊拉克": "IQ",
    "israel": "IL", "以色列": "IL",
    "jordan": "JO", "约旦": "JO",
    "lebanon": "LB", "黎巴嫩": "LB",
    "syria": "SY", "syrian arab republic": "SY", "叙利亚": "SY",
    "saudi arabia": "SA", "沙特阿拉伯": "SA", "沙特": "SA",
    "uae": "AE", "u.a.e.": "AE", "united arab emirates": "AE", "阿联酋": "AE",
    "qatar": "QA", "卡塔尔": "QA",
    "kuwait": "KW", "科威特": "KW",
    "bahrain": "BH", "巴林": "BH",
    "oman": "OM", "阿曼": "OM",
    "mongolia": "MN", "蒙古": "MN",
    # --- Europe ---
    "ireland": "IE", "爱尔兰": "IE",
    "france": "FR", "法国": "FR",
    "germany": "DE", "deutschland": "DE", "德国": "DE",
    "italy": "IT", "意大利": "IT",
    "spain": "ES", "西班牙": "ES",
    "portugal": "PT", "葡萄牙": "PT",
    "netherlands": "NL", "the netherlands": "NL", "holland": "NL", "荷兰": "NL",
    "belgium": "BE", "比利时": "BE",
    "luxembourg": "LU", "卢森堡": "LU",
    "switzerland": "CH", "瑞士": "CH",
    "austria": "AT", "奥地利": "AT",
    "denmark": "DK", "丹麦": "DK",
    "sweden": "SE", "瑞典": "SE",
    "norway": "NO", "挪威": "NO",
    "finland": "FI", "芬兰": "FI",
    "iceland": "IS", "冰岛": "IS",
    "estonia": "EE", "爱沙尼亚": "EE",
    "latvia": "LV", "拉脱维亚": "LV",
    "lithuania": "LT", "立陶宛": "LT",
    "poland": "PL", "波兰": "PL",
    "czechia": "CZ", "czech republic": "CZ", "捷克": "CZ",
    "slovakia": "SK", "斯洛伐克": "SK",
    "hungary": "HU", "匈牙利": "HU",
    "romania": "RO", "罗马尼亚": "RO",
    "bulgaria": "BG", "保加利亚": "BG",
    "greece": "GR", "希腊": "GR",
    "malta": "MT", "马耳他": "MT",
    "cyprus": "CY", "塞浦路斯": "CY",
    "croatia": "HR", "克罗地亚": "HR",
    "slovenia": "SI", "斯洛文尼亚": "SI",
    "serbia": "RS", "塞尔维亚": "RS",
    "bosnia": "BA", "bosnia and herzegovina": "BA", "波黑": "BA",
    "montenegro": "ME", "黑山": "ME",
    "north macedonia": "MK", "macedonia": "MK", "北马其顿": "MK",
    "albania": "AL", "阿尔巴尼亚": "AL",
    "ukraine": "UA", "乌克兰": "UA",
    "belarus": "BY", "白俄罗斯": "BY",
    "moldova": "MD", "摩尔多瓦": "MD",
    "russia": "RU", "russian federation": "RU", "俄罗斯": "RU",
    "turkey": "TR", "türkiye": "TR", "turkiye": "TR", "土耳其": "TR",
    # --- North America (non-US) ---
    "canada": "CA", "加拿大": "CA",
    "mexico": "MX", "墨西哥": "MX",
    "puerto rico": "PR", "波多黎各": "PR",
    "cuba": "CU", "古巴": "CU",
    "dominican republic": "DO", "多米尼加": "DO", "多米尼加共和国": "DO",
    "haiti": "HT", "海地": "HT",
    "jamaica": "JM", "牙买加": "JM",
    "bahamas": "BS", "巴哈马": "BS",
    "trinidad and tobago": "TT", "特立尼达和多巴哥": "TT",
    "belize": "BZ", "伯利兹": "BZ",
    "costa rica": "CR", "哥斯达黎加": "CR",
    "guatemala": "GT", "危地马拉": "GT",
    "honduras": "HN", "洪都拉斯": "HN",
    "nicaragua": "NI", "尼加拉瓜": "NI",
    "panama": "PA", "巴拿马": "PA",
    "el salvador": "SV", "萨尔瓦多": "SV",
    # --- South America ---
    "argentina": "AR", "阿根廷": "AR",
    "brazil": "BR", "brasil": "BR", "巴西": "BR",
    "chile": "CL", "智利": "CL",
    "colombia": "CO", "哥伦比亚": "CO",
    "ecuador": "EC", "厄瓜多尔": "EC",
    "peru": "PE", "秘鲁": "PE",
    "uruguay": "UY", "乌拉圭": "UY",
    "paraguay": "PY", "巴拉圭": "PY",
    "bolivia": "BO", "玻利维亚": "BO",
    "venezuela": "VE", "venezuela bolivarian republic of": "VE", "委内瑞拉": "VE",
    "guyana": "GY", "圭亚那": "GY",
    "suriname": "SR", "苏里南": "SR",
    # --- Africa ---
    "south africa": "ZA", "南非": "ZA",
    "egypt": "EG", "埃及": "EG",
    "morocco": "MA", "摩洛哥": "MA",
    "tunisia": "TN", "突尼斯": "TN",
    "algeria": "DZ", "阿尔及利亚": "DZ",
    "libya": "LY", "利比亚": "LY",
    "sudan": "SD", "苏丹": "SD",
    "south sudan": "SS", "南苏丹": "SS",
    "ethiopia": "ET", "埃塞俄比亚": "ET",
    "eritrea": "ER", "厄立特里亚": "ER",
    "kenya": "KE", "肯尼亚": "KE",
    "uganda": "UG", "乌干达": "UG",
    "tanzania": "TZ", "坦桑尼亚": "TZ",
    "rwanda": "RW", "卢旺达": "RW",
    "burundi": "BI", "布隆迪": "BI",
    "somalia": "SO", "索马里": "SO",
    "djibouti": "DJ", "吉布提": "DJ",
    "nigeria": "NG", "尼日利亚": "NG",
    "ghana": "GH", "加纳": "GH",
    "côte d'ivoire": "CI", "cote d'ivoire": "CI", "ivory coast": "CI",
    "科特迪瓦": "CI",
    "senegal": "SN", "塞内加尔": "SN",
    "mali": "ML", "马里": "ML",
    "burkina faso": "BF", "布基纳法索": "BF",
    "niger": "NE", "尼日尔": "NE",
    "chad": "TD", "乍得": "TD",
    "cameroon": "CM", "喀麦隆": "CM",
    "central african republic": "CF", "中非共和国": "CF", "中非": "CF",
    "republic of the congo": "CG", "congo brazzaville": "CG", "刚果（布）": "CG",
    "刚果布": "CG", "刚果共和国": "CG",
    "democratic republic of the congo": "CD", "drc": "CD",
    "congo kinshasa": "CD", "congo (drc)": "CD",
    "刚果（金）": "CD", "刚果金": "CD", "刚果民主共和国": "CD",
    "gabon": "GA", "加蓬": "GA",
    "equatorial guinea": "GQ", "赤道几内亚": "GQ",
    "benin": "BJ", "贝宁": "BJ",
    "togo": "TG", "多哥": "TG",
    "liberia": "LR", "利比里亚": "LR",
    "sierra leone": "SL", "塞拉利昂": "SL",
    "guinea": "GN", "几内亚": "GN",
    "guinea-bissau": "GW", "几内亚比绍": "GW",
    "gambia": "GM", "冈比亚": "GM",
    "cabo verde": "CV", "cape verde": "CV", "佛得角": "CV",
    "mauritania": "MR", "毛里塔尼亚": "MR",
    "angola": "AO", "安哥拉": "AO",
    "zambia": "ZM", "赞比亚": "ZM",
    "zimbabwe": "ZW", "津巴布韦": "ZW",
    "mozambique": "MZ", "莫桑比克": "MZ",
    "malawi": "MW", "马拉维": "MW",
    "madagascar": "MG", "马达加斯加": "MG",
    "mauritius": "MU", "毛里求斯": "MU",
    "seychelles": "SC", "塞舌尔": "SC",
    "comoros": "KM", "科摩罗": "KM",
    "namibia": "NA", "纳米比亚": "NA",
    "botswana": "BW", "博茨瓦纳": "BW",
    "lesotho": "LS", "莱索托": "LS",
    "eswatini": "SZ", "swaziland": "SZ", "斯威士兰": "SZ",
    # --- Oceania ---
    "australia": "AU", "澳大利亚": "AU", "澳洲": "AU",
    "new zealand": "NZ", "新西兰": "NZ",
    "fiji": "FJ", "斐济": "FJ",
    "papua new guinea": "PG", "巴布亚新几内亚": "PG",
}


# ---------------------------------------------------------------------------
# Region tags — product-curated buckets. ABSOLUTELY NOT countries.
#
# 4 of these are US-only sub-regions (so they imply country_code='US').
# 5 are multi-country buckets ("北欧", "其他亚洲国家"…) which leave
# country_code NULL — the operator manually fills the real country later.
# ---------------------------------------------------------------------------

_REGION_TAGS: list[RegionTagRecord] = [
    {"code": "us_california_flagship",
     "name_en": "US — California Flagship",     "name_zh": "加州旗舰",
     "scope": "us_subregion", "implied_country_code": "US", "sort_order": 10},
    {"code": "us_midwest_flagship",
     "name_en": "US — Midwest Flagship",        "name_zh": "中西部旗舰",
     "scope": "us_subregion", "implied_country_code": "US", "sort_order": 20},
    {"code": "us_northeast_top",
     "name_en": "US — Northeast Top Schools",   "name_zh": "东北强校",
     "scope": "us_subregion", "implied_country_code": "US", "sort_order": 30},
    {"code": "us_south_southwest",
     "name_en": "US — South & Southwest",       "name_zh": "南方与西南",
     "scope": "us_subregion", "implied_country_code": "US", "sort_order": 40},
    {"code": "nordics",
     "name_en": "Nordic Countries",             "name_zh": "北欧",
     "scope": "multi_country_bucket", "implied_country_code": None, "sort_order": 50},
    {"code": "other_europe",
     "name_en": "Other European Countries",     "name_zh": "其他欧洲国家",
     "scope": "multi_country_bucket", "implied_country_code": None, "sort_order": 60},
    {"code": "other_asia",
     "name_en": "Other Asian Countries",        "name_zh": "其他亚洲国家",
     "scope": "multi_country_bucket", "implied_country_code": None, "sort_order": 70},
    {"code": "other_africa",
     "name_en": "Other African Countries",      "name_zh": "其他非洲国家",
     "scope": "multi_country_bucket", "implied_country_code": None, "sort_order": 80},
    {"code": "other_south_america",
     "name_en": "Other South American Countries", "name_zh": "其他南美国家",
     "scope": "multi_country_bucket", "implied_country_code": None, "sort_order": 90},
]

# Raw text in `schools.country` → region_tag code. Only the values currently
# present in the DB are listed; new buckets must be added both here and to
# `_REGION_TAGS` above.
_REGION_TAG_BY_RAW: dict[str, str] = {
    "加州旗舰":           "us_california_flagship",
    "中西部旗舰":         "us_midwest_flagship",
    "东北强校":           "us_northeast_top",
    "南方与西南":         "us_south_southwest",
    "北欧":               "nordics",
    "其他欧洲国家":       "other_europe",
    "其他亚洲国家":       "other_asia",
    "其他非洲国家":       "other_africa",
    "其他南美国家":       "other_south_america",
}


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

def _normalize_text(raw: str) -> str:
    """NFKD-fold, lowercase, strip punctuation, collapse whitespace.

    Chinese characters survive NFKD unchanged so the alias table can use them
    directly. Latin variants get cleaned up (Türkiye → turkiye, U.S.A. → usa).
    """
    text = unicodedata.normalize("NFKD", raw)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().strip()
    text = re.sub(r"[^\w\s\u4e00-\u9fff'’()]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _empty() -> CountryFields:
    return {"country_code": None, "region_tag": None}


def normalize_country_only(raw: Optional[str]) -> Optional[str]:
    """Return ISO code for a free-text country label, or None.

    Does NOT consider region_tag buckets. Use `resolve_country` for the full
    `schools.country` resolution path.
    """
    if not raw:
        return None
    stripped = str(raw).strip()
    if not stripped:
        return None
    # Direct ISO-code match (case-insensitive). Lets the resolver be reused
    # for already-canonical inputs without extra logic.
    upper = stripped.upper()
    if upper in _COUNTRY_NAMES:
        return upper
    # Try the raw stripped form first (handles Chinese-character keys directly).
    if stripped in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[stripped]
    folded = _normalize_text(stripped)
    if folded in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[folded]
    return None


def resolve_country(raw: Optional[str]) -> CountryFields:
    """Map a raw `schools.country` value to (country_code, region_tag).

    Order of resolution:
      1. Empty / null → both None.
      2. Region-tag bucket — set region_tag, plus country_code if implied
         (the four US-internal buckets imply country_code='US'; the five
         multi-country buckets leave country_code NULL for manual review).
      3. Otherwise treat as a country label and look up an ISO code.
    """
    if raw is None:
        return _empty()
    text = str(raw).strip()
    if not text:
        return _empty()

    # 2) region-tag bucket?
    tag_code = _REGION_TAG_BY_RAW.get(text)
    if tag_code:
        # implied country (only filled for the 4 US-subregion tags)
        implied = next(
            (t["implied_country_code"] for t in _REGION_TAGS if t["code"] == tag_code),
            None,
        )
        return {"country_code": implied, "region_tag": tag_code}

    # 3) country label
    return {"country_code": normalize_country_only(text), "region_tag": None}


# ---------------------------------------------------------------------------
# Catalog iterators (consumed by scripts/sync_country_dictionaries.py)
# ---------------------------------------------------------------------------

def iter_country_catalog() -> Iterator[CountryRecord]:
    """Yield every controlled-vocabulary country entry."""
    for idx, (code, name_en, name_zh, continent) in enumerate(_COUNTRY_DATA):
        yield {
            "code": code,
            "name_en": name_en,
            "name_zh": name_zh,
            "region_continent": continent,
            "sort_order": (idx + 1) * 10,
        }


def iter_region_tag_catalog() -> Iterator[RegionTagRecord]:
    """Yield every region_tag entry (deep-copy to keep the source list pristine)."""
    for entry in _REGION_TAGS:
        yield dict(entry)  # type: ignore[misc]
