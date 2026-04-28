-- P1 normalization: split the legacy `schools.country` text column into two
-- well-typed dimensions:
--
--   country_code : text → FK public.countries.code (ISO 3166-1 alpha-2)
--   region_tag   : text → FK public.region_tags.code (product-curated bucket)
--
-- Why two columns?
-- The legacy `country` mixed real sovereign nations ("加拿大") with curated
-- product groupings ("加州旗舰", "中西部旗舰", "北欧", "其他亚洲国家").
-- Splitting them lets the country dimension stay cleanly ISO-coded for
-- filters / exports, while the region_tag column holds the curated buckets
-- the front-end will use for "recommend by region" UX.
--
-- Backfill flow (do NOT roll into this migration — separate, idempotent step):
--   1) apply this SQL  → adds the two columns + dictionaries (NULL FK passes)
--   2) python -m scripts.backfill_country_and_region
--        → fills country_code + region_tag for every recognised raw value
--        → leaves country_code NULL on the 16 multi-country bucket rows
--          (北欧 / 其他XX国家) — those need manual entry afterwards
--   3) operator manually updates country_code on the remaining 16 rows
--   4) (future migration) add NOT NULL on country_code, rename / drop the
--      legacy `country` text column once the data is fully clean
--
-- Idempotent: safe to re-run.

BEGIN;

-- =============================================================
-- 1) countries dictionary
-- =============================================================
CREATE TABLE IF NOT EXISTS public.countries (
  code              text PRIMARY KEY,                    -- ISO 3166-1 alpha-2
  name_en           text NOT NULL,
  name_zh           text,
  region_continent  text,                                -- 'Asia' | 'Europe' | …
  sort_order        int  NOT NULL DEFAULT 0,
  is_active         boolean NOT NULL DEFAULT true,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT countries_code_format_chk
    CHECK (code ~ '^[A-Z]{2}$'),
  CONSTRAINT countries_continent_chk
    CHECK (region_continent IS NULL OR region_continent IN (
      'North America','South America','Europe','Asia','Africa','Oceania','Antarctica'
    ))
);

CREATE INDEX IF NOT EXISTS idx_countries_continent
  ON public.countries (region_continent);
CREATE INDEX IF NOT EXISTS idx_countries_active_sort
  ON public.countries (is_active, sort_order);

-- Seed. Snapshot generated from pipeline/country_normalizer.py via
-- iter_country_catalog(). To pick up new entries after editing the
-- normalizer, run:
--     python -m scripts.sync_country_dictionaries
INSERT INTO public.countries (code, name_en, name_zh, region_continent, sort_order) VALUES
  ('US', 'United States', '美国', 'North America', 10),
  ('CA', 'Canada', '加拿大', 'North America', 20),
  ('MX', 'Mexico', '墨西哥', 'North America', 30),
  ('PR', 'Puerto Rico', '波多黎各', 'North America', 40),
  ('CU', 'Cuba', '古巴', 'North America', 50),
  ('DO', 'Dominican Republic', '多米尼加', 'North America', 60),
  ('HT', 'Haiti', '海地', 'North America', 70),
  ('JM', 'Jamaica', '牙买加', 'North America', 80),
  ('BS', 'Bahamas', '巴哈马', 'North America', 90),
  ('TT', 'Trinidad and Tobago', '特立尼达和多巴哥', 'North America', 100),
  ('BZ', 'Belize', '伯利兹', 'North America', 110),
  ('CR', 'Costa Rica', '哥斯达黎加', 'North America', 120),
  ('GT', 'Guatemala', '危地马拉', 'North America', 130),
  ('HN', 'Honduras', '洪都拉斯', 'North America', 140),
  ('NI', 'Nicaragua', '尼加拉瓜', 'North America', 150),
  ('PA', 'Panama', '巴拿马', 'North America', 160),
  ('SV', 'El Salvador', '萨尔瓦多', 'North America', 170),
  ('AR', 'Argentina', '阿根廷', 'South America', 180),
  ('BR', 'Brazil', '巴西', 'South America', 190),
  ('CL', 'Chile', '智利', 'South America', 200),
  ('CO', 'Colombia', '哥伦比亚', 'South America', 210),
  ('EC', 'Ecuador', '厄瓜多尔', 'South America', 220),
  ('PE', 'Peru', '秘鲁', 'South America', 230),
  ('UY', 'Uruguay', '乌拉圭', 'South America', 240),
  ('PY', 'Paraguay', '巴拉圭', 'South America', 250),
  ('BO', 'Bolivia', '玻利维亚', 'South America', 260),
  ('VE', 'Venezuela', '委内瑞拉', 'South America', 270),
  ('GY', 'Guyana', '圭亚那', 'South America', 280),
  ('SR', 'Suriname', '苏里南', 'South America', 290),
  ('GB', 'United Kingdom', '英国', 'Europe', 300),
  ('IE', 'Ireland', '爱尔兰', 'Europe', 310),
  ('FR', 'France', '法国', 'Europe', 320),
  ('DE', 'Germany', '德国', 'Europe', 330),
  ('IT', 'Italy', '意大利', 'Europe', 340),
  ('ES', 'Spain', '西班牙', 'Europe', 350),
  ('PT', 'Portugal', '葡萄牙', 'Europe', 360),
  ('NL', 'Netherlands', '荷兰', 'Europe', 370),
  ('BE', 'Belgium', '比利时', 'Europe', 380),
  ('LU', 'Luxembourg', '卢森堡', 'Europe', 390),
  ('CH', 'Switzerland', '瑞士', 'Europe', 400),
  ('AT', 'Austria', '奥地利', 'Europe', 410),
  ('DK', 'Denmark', '丹麦', 'Europe', 420),
  ('SE', 'Sweden', '瑞典', 'Europe', 430),
  ('NO', 'Norway', '挪威', 'Europe', 440),
  ('FI', 'Finland', '芬兰', 'Europe', 450),
  ('IS', 'Iceland', '冰岛', 'Europe', 460),
  ('EE', 'Estonia', '爱沙尼亚', 'Europe', 470),
  ('LV', 'Latvia', '拉脱维亚', 'Europe', 480),
  ('LT', 'Lithuania', '立陶宛', 'Europe', 490),
  ('PL', 'Poland', '波兰', 'Europe', 500),
  ('CZ', 'Czechia', '捷克', 'Europe', 510),
  ('SK', 'Slovakia', '斯洛伐克', 'Europe', 520),
  ('HU', 'Hungary', '匈牙利', 'Europe', 530),
  ('RO', 'Romania', '罗马尼亚', 'Europe', 540),
  ('BG', 'Bulgaria', '保加利亚', 'Europe', 550),
  ('GR', 'Greece', '希腊', 'Europe', 560),
  ('MT', 'Malta', '马耳他', 'Europe', 570),
  ('CY', 'Cyprus', '塞浦路斯', 'Europe', 580),
  ('HR', 'Croatia', '克罗地亚', 'Europe', 590),
  ('SI', 'Slovenia', '斯洛文尼亚', 'Europe', 600),
  ('RS', 'Serbia', '塞尔维亚', 'Europe', 610),
  ('BA', 'Bosnia and Herzegovina', '波黑', 'Europe', 620),
  ('ME', 'Montenegro', '黑山', 'Europe', 630),
  ('MK', 'North Macedonia', '北马其顿', 'Europe', 640),
  ('AL', 'Albania', '阿尔巴尼亚', 'Europe', 650),
  ('UA', 'Ukraine', '乌克兰', 'Europe', 660),
  ('BY', 'Belarus', '白俄罗斯', 'Europe', 670),
  ('MD', 'Moldova', '摩尔多瓦', 'Europe', 680),
  ('RU', 'Russia', '俄罗斯', 'Europe', 690),
  ('TR', 'Türkiye', '土耳其', 'Europe', 700),
  ('CN', 'China', '中国', 'Asia', 710),
  ('HK', 'Hong Kong', '中国香港', 'Asia', 720),
  ('MO', 'Macao', '中国澳门', 'Asia', 730),
  ('TW', 'Taiwan', '中国台湾', 'Asia', 740),
  ('JP', 'Japan', '日本', 'Asia', 750),
  ('KR', 'South Korea', '韩国', 'Asia', 760),
  ('KP', 'North Korea', '朝鲜', 'Asia', 770),
  ('SG', 'Singapore', '新加坡', 'Asia', 780),
  ('MY', 'Malaysia', '马来西亚', 'Asia', 790),
  ('TH', 'Thailand', '泰国', 'Asia', 800),
  ('VN', 'Vietnam', '越南', 'Asia', 810),
  ('PH', 'Philippines', '菲律宾', 'Asia', 820),
  ('ID', 'Indonesia', '印度尼西亚', 'Asia', 830),
  ('MM', 'Myanmar', '缅甸', 'Asia', 840),
  ('KH', 'Cambodia', '柬埔寨', 'Asia', 850),
  ('LA', 'Laos', '老挝', 'Asia', 860),
  ('BN', 'Brunei', '文莱', 'Asia', 870),
  ('TL', 'Timor-Leste', '东帝汶', 'Asia', 880),
  ('IN', 'India', '印度', 'Asia', 890),
  ('PK', 'Pakistan', '巴基斯坦', 'Asia', 900),
  ('BD', 'Bangladesh', '孟加拉国', 'Asia', 910),
  ('LK', 'Sri Lanka', '斯里兰卡', 'Asia', 920),
  ('NP', 'Nepal', '尼泊尔', 'Asia', 930),
  ('BT', 'Bhutan', '不丹', 'Asia', 940),
  ('MV', 'Maldives', '马尔代夫', 'Asia', 950),
  ('AF', 'Afghanistan', '阿富汗', 'Asia', 960),
  ('IR', 'Iran', '伊朗', 'Asia', 970),
  ('IQ', 'Iraq', '伊拉克', 'Asia', 980),
  ('IL', 'Israel', '以色列', 'Asia', 990),
  ('PS', 'Palestine', '巴勒斯坦', 'Asia', 1000),
  ('JO', 'Jordan', '约旦', 'Asia', 1010),
  ('LB', 'Lebanon', '黎巴嫩', 'Asia', 1020),
  ('SY', 'Syria', '叙利亚', 'Asia', 1030),
  ('SA', 'Saudi Arabia', '沙特阿拉伯', 'Asia', 1040),
  ('AE', 'United Arab Emirates', '阿联酋', 'Asia', 1050),
  ('QA', 'Qatar', '卡塔尔', 'Asia', 1060),
  ('KW', 'Kuwait', '科威特', 'Asia', 1070),
  ('BH', 'Bahrain', '巴林', 'Asia', 1080),
  ('OM', 'Oman', '阿曼', 'Asia', 1090),
  ('YE', 'Yemen', '也门', 'Asia', 1100),
  ('MN', 'Mongolia', '蒙古', 'Asia', 1110),
  ('KZ', 'Kazakhstan', '哈萨克斯坦', 'Asia', 1120),
  ('UZ', 'Uzbekistan', '乌兹别克斯坦', 'Asia', 1130),
  ('KG', 'Kyrgyzstan', '吉尔吉斯斯坦', 'Asia', 1140),
  ('TJ', 'Tajikistan', '塔吉克斯坦', 'Asia', 1150),
  ('TM', 'Turkmenistan', '土库曼斯坦', 'Asia', 1160),
  ('AM', 'Armenia', '亚美尼亚', 'Asia', 1170),
  ('AZ', 'Azerbaijan', '阿塞拜疆', 'Asia', 1180),
  ('GE', 'Georgia', '格鲁吉亚', 'Asia', 1190),
  ('ZA', 'South Africa', '南非', 'Africa', 1200),
  ('EG', 'Egypt', '埃及', 'Africa', 1210),
  ('MA', 'Morocco', '摩洛哥', 'Africa', 1220),
  ('TN', 'Tunisia', '突尼斯', 'Africa', 1230),
  ('DZ', 'Algeria', '阿尔及利亚', 'Africa', 1240),
  ('LY', 'Libya', '利比亚', 'Africa', 1250),
  ('SD', 'Sudan', '苏丹', 'Africa', 1260),
  ('SS', 'South Sudan', '南苏丹', 'Africa', 1270),
  ('ET', 'Ethiopia', '埃塞俄比亚', 'Africa', 1280),
  ('ER', 'Eritrea', '厄立特里亚', 'Africa', 1290),
  ('KE', 'Kenya', '肯尼亚', 'Africa', 1300),
  ('UG', 'Uganda', '乌干达', 'Africa', 1310),
  ('TZ', 'Tanzania', '坦桑尼亚', 'Africa', 1320),
  ('RW', 'Rwanda', '卢旺达', 'Africa', 1330),
  ('BI', 'Burundi', '布隆迪', 'Africa', 1340),
  ('SO', 'Somalia', '索马里', 'Africa', 1350),
  ('DJ', 'Djibouti', '吉布提', 'Africa', 1360),
  ('NG', 'Nigeria', '尼日利亚', 'Africa', 1370),
  ('GH', 'Ghana', '加纳', 'Africa', 1380),
  ('CI', 'Côte d''Ivoire', '科特迪瓦', 'Africa', 1390),
  ('SN', 'Senegal', '塞内加尔', 'Africa', 1400),
  ('ML', 'Mali', '马里', 'Africa', 1410),
  ('BF', 'Burkina Faso', '布基纳法索', 'Africa', 1420),
  ('NE', 'Niger', '尼日尔', 'Africa', 1430),
  ('TD', 'Chad', '乍得', 'Africa', 1440),
  ('CM', 'Cameroon', '喀麦隆', 'Africa', 1450),
  ('CF', 'Central African Republic', '中非共和国', 'Africa', 1460),
  ('CG', 'Congo (Brazzaville)', '刚果（布）', 'Africa', 1470),
  ('CD', 'Congo (DRC)', '刚果（金）', 'Africa', 1480),
  ('GA', 'Gabon', '加蓬', 'Africa', 1490),
  ('GQ', 'Equatorial Guinea', '赤道几内亚', 'Africa', 1500),
  ('BJ', 'Benin', '贝宁', 'Africa', 1510),
  ('TG', 'Togo', '多哥', 'Africa', 1520),
  ('LR', 'Liberia', '利比里亚', 'Africa', 1530),
  ('SL', 'Sierra Leone', '塞拉利昂', 'Africa', 1540),
  ('GN', 'Guinea', '几内亚', 'Africa', 1550),
  ('GW', 'Guinea-Bissau', '几内亚比绍', 'Africa', 1560),
  ('GM', 'Gambia', '冈比亚', 'Africa', 1570),
  ('CV', 'Cabo Verde', '佛得角', 'Africa', 1580),
  ('MR', 'Mauritania', '毛里塔尼亚', 'Africa', 1590),
  ('AO', 'Angola', '安哥拉', 'Africa', 1600),
  ('ZM', 'Zambia', '赞比亚', 'Africa', 1610),
  ('ZW', 'Zimbabwe', '津巴布韦', 'Africa', 1620),
  ('MZ', 'Mozambique', '莫桑比克', 'Africa', 1630),
  ('MW', 'Malawi', '马拉维', 'Africa', 1640),
  ('MG', 'Madagascar', '马达加斯加', 'Africa', 1650),
  ('MU', 'Mauritius', '毛里求斯', 'Africa', 1660),
  ('SC', 'Seychelles', '塞舌尔', 'Africa', 1670),
  ('KM', 'Comoros', '科摩罗', 'Africa', 1680),
  ('NA', 'Namibia', '纳米比亚', 'Africa', 1690),
  ('BW', 'Botswana', '博茨瓦纳', 'Africa', 1700),
  ('LS', 'Lesotho', '莱索托', 'Africa', 1710),
  ('SZ', 'Eswatini', '斯威士兰', 'Africa', 1720),
  ('AU', 'Australia', '澳大利亚', 'Oceania', 1730),
  ('NZ', 'New Zealand', '新西兰', 'Oceania', 1740),
  ('FJ', 'Fiji', '斐济', 'Oceania', 1750),
  ('PG', 'Papua New Guinea', '巴布亚新几内亚', 'Oceania', 1760),
  ('WS', 'Samoa', '萨摩亚', 'Oceania', 1770),
  ('TO', 'Tonga', '汤加', 'Oceania', 1780),
  ('VU', 'Vanuatu', '瓦努阿图', 'Oceania', 1790),
  ('SB', 'Solomon Islands', '所罗门群岛', 'Oceania', 1800)
ON CONFLICT (code) DO UPDATE
  SET name_en          = EXCLUDED.name_en,
      name_zh          = EXCLUDED.name_zh,
      region_continent = EXCLUDED.region_continent,
      sort_order       = EXCLUDED.sort_order,
      updated_at       = now();

-- =============================================================
-- 2) region_tags dictionary
-- =============================================================
CREATE TABLE IF NOT EXISTS public.region_tags (
  code                  text PRIMARY KEY,
  name_en               text NOT NULL,
  name_zh               text,
  scope                 text NOT NULL,    -- 'us_subregion' | 'multi_country_bucket'
  implied_country_code  text,             -- only set when scope is single-country
  sort_order            int  NOT NULL DEFAULT 0,
  is_active             boolean NOT NULL DEFAULT true,
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT region_tags_scope_chk
    CHECK (scope IN ('us_subregion', 'multi_country_bucket')),
  CONSTRAINT region_tags_implied_country_chk
    CHECK (
      (scope = 'us_subregion'         AND implied_country_code IS NOT NULL)
      OR
      (scope = 'multi_country_bucket' AND implied_country_code IS NULL)
    ),
  CONSTRAINT region_tags_implied_country_fkey
    FOREIGN KEY (implied_country_code) REFERENCES public.countries (code)
    ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_region_tags_scope
  ON public.region_tags (scope);
CREATE INDEX IF NOT EXISTS idx_region_tags_active_sort
  ON public.region_tags (is_active, sort_order);

INSERT INTO public.region_tags
  (code, name_en, name_zh, scope, implied_country_code, sort_order) VALUES
  ('us_california_flagship', 'US — California Flagship',     '加州旗舰',        'us_subregion',          'US', 10),
  ('us_midwest_flagship',    'US — Midwest Flagship',        '中西部旗舰',      'us_subregion',          'US', 20),
  ('us_northeast_top',       'US — Northeast Top Schools',   '东北强校',        'us_subregion',          'US', 30),
  ('us_south_southwest',     'US — South & Southwest',       '南方与西南',      'us_subregion',          'US', 40),
  ('nordics',                'Nordic Countries',             '北欧',            'multi_country_bucket',  NULL, 50),
  ('other_europe',           'Other European Countries',     '其他欧洲国家',    'multi_country_bucket',  NULL, 60),
  ('other_asia',             'Other Asian Countries',        '其他亚洲国家',    'multi_country_bucket',  NULL, 70),
  ('other_africa',           'Other African Countries',      '其他非洲国家',    'multi_country_bucket',  NULL, 80),
  ('other_south_america',    'Other South American Countries','其他南美国家',   'multi_country_bucket',  NULL, 90)
ON CONFLICT (code) DO UPDATE
  SET name_en              = EXCLUDED.name_en,
      name_zh              = EXCLUDED.name_zh,
      scope                = EXCLUDED.scope,
      implied_country_code = EXCLUDED.implied_country_code,
      sort_order           = EXCLUDED.sort_order,
      updated_at           = now();

-- =============================================================
-- 3) Add the two new columns to schools (nullable, FK enforced).
--
-- Both columns start NULL on every existing row and get populated by the
-- backfill script. Keeping the legacy `country` text column in place for now
-- as an audit trail / safety net — a follow-up migration will rename it to
-- `raw_country` (or drop it) once the new columns are fully populated.
-- =============================================================
ALTER TABLE public.schools
  ADD COLUMN IF NOT EXISTS country_code text,
  ADD COLUMN IF NOT EXISTS region_tag   text;

CREATE INDEX IF NOT EXISTS idx_schools_country_code ON public.schools (country_code);
CREATE INDEX IF NOT EXISTS idx_schools_region_tag   ON public.schools (region_tag);

ALTER TABLE public.schools
  DROP CONSTRAINT IF EXISTS schools_country_code_fkey;

ALTER TABLE public.schools
  ADD CONSTRAINT schools_country_code_fkey
  FOREIGN KEY (country_code)
  REFERENCES public.countries (code)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

ALTER TABLE public.schools
  DROP CONSTRAINT IF EXISTS schools_region_tag_fkey;

ALTER TABLE public.schools
  ADD CONSTRAINT schools_region_tag_fkey
  FOREIGN KEY (region_tag)
  REFERENCES public.region_tags (code)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

COMMIT;

-- =============================================================
-- Post-migration step (run separately):
--
--   python -m scripts.backfill_country_and_region
--
-- After that, you'll have ~16 schools rows whose region_tag is one of the
-- 5 multi-country buckets (北欧 / 其他XX国家) and country_code is still NULL.
-- The script prints those rows; resolve them manually with:
--
--   UPDATE public.schools SET country_code = 'XX' WHERE id = '...';
-- =============================================================
