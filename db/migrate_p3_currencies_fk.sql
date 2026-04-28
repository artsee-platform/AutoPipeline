-- P3: ISO 4217 currency dictionary + FK on program_fees.currency_code
--
-- Source of truth for labels / sort_order: pipeline/currency_catalog.py — keep in
-- sync via:
--     python -m scripts.sync_currencies
--
-- This file seeds the same rows so the FK can be applied in a clean checkout.
-- Idempotent (single transaction).

BEGIN;

CREATE TABLE IF NOT EXISTS public.currencies (
  code            text PRIMARY KEY,
  name_en         text NOT NULL,
  name_zh         text,
  sort_order      int  NOT NULL DEFAULT 0,
  is_active       boolean NOT NULL DEFAULT true,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT currencies_code_iso_chk
    CHECK (char_length(code) = 3 AND code = upper(code) AND code ~ '^[A-Z]{3}$')
);

CREATE INDEX IF NOT EXISTS idx_currencies_active_sort
  ON public.currencies (is_active, sort_order);

INSERT INTO public.currencies (code, name_en, name_zh, sort_order) VALUES
  ('USD', 'US Dollar', '美元', 10),
  ('EUR', 'Euro', '欧元', 20),
  ('GBP', 'British Pound', '英镑', 30),
  ('JPY', 'Japanese Yen', '日元', 40),
  ('CNY', 'Chinese Yuan', '人民币', 50),
  ('AUD', 'Australian Dollar', '澳元', 60),
  ('CAD', 'Canadian Dollar', '加拿大元', 70),
  ('CHF', 'Swiss Franc', '瑞士法郎', 80),
  ('SGD', 'Singapore Dollar', '新加坡元', 90),
  ('HKD', 'Hong Kong Dollar', '港元', 100),
  ('KRW', 'South Korean Won', '韩元', 110),
  ('NZD', 'New Zealand Dollar', '新西兰元', 120),
  ('SEK', 'Swedish Krona', '瑞典克朗', 130),
  ('NOK', 'Norwegian Krone', '挪威克朗', 140),
  ('DKK', 'Danish Krone', '丹麦克朗', 150),
  ('MXN', 'Mexican Peso', '墨西哥比索', 160),
  ('INR', 'Indian Rupee', '印度卢比', 170),
  ('IDR', 'Indonesian Rupiah', '印尼盾', 180),
  ('THB', 'Thai Baht', '泰铢', 190),
  ('ZAR', 'South African Rand', '南非兰特', 200),
  ('NGN', 'Nigerian Naira', '尼日利亚奈拉', 210),
  ('GHS', 'Ghanaian Cedi', '加纳塞地', 220),
  ('KES', 'Kenyan Shilling', '肯尼亚先令', 230),
  ('EGP', 'Egyptian Pound', '埃及镑', 240),
  ('TZS', 'Tanzanian Shilling', '坦桑尼亚先令', 250),
  ('UGX', 'Ugandan Shilling', '乌干达先令', 260),
  ('ZMW', 'Zambian Kwacha', '赞比亚克瓦查', 270),
  ('PLN', 'Polish Zloty', '波兰兹罗提', 280),
  ('CZK', 'Czech Koruna', '捷克克朗', 290),
  ('HUF', 'Hungarian Forint', '匈牙利福林', 300),
  ('RON', 'Romanian Leu', '罗马尼亚列伊', 310),
  ('BRL', 'Brazilian Real', '巴西雷亚尔', 320),
  ('ARS', 'Argentine Peso', '阿根廷比索', 330),
  ('CLP', 'Chilean Peso', '智利比索', 340),
  ('COP', 'Colombian Peso', '哥伦比亚比索', 350),
  ('PEN', 'Peruvian Sol', '秘鲁索尔', 360),
  ('AED', 'UAE Dirham', '阿联酋迪拉姆', 370),
  ('SAR', 'Saudi Riyal', '沙特里亚尔', 380),
  ('MYR', 'Malaysian Ringgit', '马来西亚林吉特', 390),
  ('PHP', 'Philippine Peso', '菲律宾比索', 400),
  ('VND', 'Vietnamese Dong', '越南盾', 410),
  ('TRY', 'Turkish Lira', '土耳其里拉', 420),
  ('RUB', 'Russian Ruble', '俄罗斯卢布', 430),
  ('ILS', 'Israeli Shekel', '以色列新谢克尔', 440),
  ('ISK', 'Icelandic Krona', '冰岛克朗', 450),
  ('TWD', 'New Taiwan Dollar', '新台币', 460),
  ('PKR', 'Pakistani Rupee', '巴基斯坦卢比', 470),
  ('BDT', 'Bangladeshi Taka', '孟加拉塔卡', 480),
  ('LKR', 'Sri Lankan Rupee', '斯里兰卡卢比', 490),
  ('MAD', 'Moroccan Dirham', '摩洛哥迪拉姆', 500),
  ('MUR', 'Mauritian Rupee', '毛里求斯卢比', 505)
ON CONFLICT (code) DO UPDATE
  SET name_en    = EXCLUDED.name_en,
      name_zh    = EXCLUDED.name_zh,
      sort_order = EXCLUDED.sort_order,
      updated_at = now();

-- Normalise codes before FK (align with Stage 5 default GBP for empty strings)
UPDATE public.program_fees
SET currency_code = 'GBP'
WHERE currency_code IS NOT NULL AND trim(currency_code) = '';

UPDATE public.program_fees
SET currency_code = upper(trim(currency_code))
WHERE currency_code IS NOT NULL
  AND currency_code <> upper(trim(currency_code));

DO $$
DECLARE
  missing text;
BEGIN
  SELECT string_agg(DISTINCT pf.currency_code, ', ' ORDER BY pf.currency_code)
    INTO missing
  FROM public.program_fees pf
  LEFT JOIN public.currencies c ON c.code = pf.currency_code
  WHERE pf.currency_code IS NOT NULL
    AND pf.currency_code <> ''
    AND c.code IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION
      'program_fees.currency_code contains codes not seeded in currencies: %. Add them to db/migrate_p3_currencies_fk.sql and pipeline/currency_catalog.py, then re-run.',
      missing;
  END IF;
END$$;

ALTER TABLE public.program_fees
  DROP CONSTRAINT IF EXISTS program_fees_currency_code_fkey;

ALTER TABLE public.program_fees
  ADD CONSTRAINT program_fees_currency_code_fkey
  FOREIGN KEY (currency_code)
  REFERENCES public.currencies (code)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_program_fees_currency_code
  ON public.program_fees (currency_code);

COMMIT;
