-- P10 — Lightweight media health flags kept out of the main schools profile.
-- `schools` remains the product-facing canonical profile; this table is only
-- used by the enrichment pipeline to decide which media rows need another pass.

CREATE TABLE IF NOT EXISTS public.school_media_status (
  school_id uuid PRIMARY KEY
    REFERENCES public.schools (id) ON UPDATE CASCADE ON DELETE CASCADE,
  logo_status text NOT NULL DEFAULT 'missing',
  campus_image_status text NOT NULL DEFAULT 'missing',
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT school_media_status_logo_chk
    CHECK (logo_status IN ('ok', 'missing', 'broken', 'low_quality', 'wrong_type')),
  CONSTRAINT school_media_status_campus_chk
    CHECK (campus_image_status IN ('ok', 'missing', 'broken', 'low_quality', 'wrong_type'))
);

CREATE INDEX IF NOT EXISTS idx_school_media_status_logo
  ON public.school_media_status (logo_status);

CREATE INDEX IF NOT EXISTS idx_school_media_status_campus
  ON public.school_media_status (campus_image_status);

COMMENT ON TABLE public.school_media_status IS
  'Lightweight logo/campus image health flags for media refresh decisions.';
