-- Rebuild public.art_categories as a 2-level taxonomy (7 L1 + 104 L2 = 111 rows).
-- Drops and recreates the table with a new schema; wipes program_art_categories links
-- and re-creates the FK to the new art_categories(id).
--
-- Safe to re-run: entire script runs in a single transaction. Back up first if you
-- already have manually curated rows in program_art_categories.
--
-- Run in Supabase SQL Editor.

BEGIN;

-- 1. Wipe the link table first (fresh start; Stage 5 --fill-art-categories will refill).
TRUNCATE TABLE public.program_art_categories;

-- 2. Drop old art_categories (CASCADE removes the FK constraint on program_art_categories,
--    but preserves the column and its data, which is already empty after the TRUNCATE above).
DROP TABLE IF EXISTS public.art_categories CASCADE;

-- 3. Create new art_categories with explicit manual id strategy (1..7 for L1, parent*100+N for L2).
CREATE TABLE public.art_categories (
    id                    integer      PRIMARY KEY,
    code                  text         NOT NULL UNIQUE,
    level                 smallint     NOT NULL,
    parent_id             integer      NULL REFERENCES public.art_categories(id) ON DELETE RESTRICT,
    name_zh               text         NOT NULL,
    name_en               text         NOT NULL,
    aliases               text[]       NOT NULL DEFAULT '{}',
    is_interdisciplinary  boolean      NOT NULL DEFAULT false,
    description_zh        text         NULL,
    description_en        text         NULL,
    display_order         smallint     NOT NULL DEFAULT 0,
    color_hex             text         NULL,
    icon_slug             text         NULL,
    is_active             boolean      NOT NULL DEFAULT true,
    created_at            timestamptz  NOT NULL DEFAULT now(),
    updated_at            timestamptz  NOT NULL DEFAULT now(),

    CONSTRAINT art_categories_level_chk        CHECK (level IN (1, 2)),
    CONSTRAINT art_categories_tree_shape_chk   CHECK (
        (level = 1 AND parent_id IS NULL) OR
        (level = 2 AND parent_id IS NOT NULL)
    ),
    CONSTRAINT art_categories_sibling_name_uq  UNIQUE (parent_id, name_zh)
);

CREATE INDEX art_categories_parent_idx
    ON public.art_categories (parent_id);

CREATE INDEX art_categories_level_active_idx
    ON public.art_categories (level)
    WHERE is_active;

-- Auto-update updated_at on row change.
CREATE OR REPLACE FUNCTION public.art_categories_touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$fn$;

DROP TRIGGER IF EXISTS art_categories_touch_updated_at ON public.art_categories;
CREATE TRIGGER art_categories_touch_updated_at
    BEFORE UPDATE ON public.art_categories
    FOR EACH ROW
    EXECUTE FUNCTION public.art_categories_touch_updated_at();

-- 4. Insert the 7 level-1 faculties (id 1..7). Colors approximate the source diagram; edit freely.
INSERT INTO public.art_categories
    (id, code, level, parent_id, name_zh, name_en, display_order, color_hex, is_interdisciplinary)
VALUES
    (1, 'fashion',                1, NULL, '时尚科系',       'Fashion',                    1, '#E95D7B', false),
    (2, 'visual_communication',   1, NULL, '视觉传达科系',   'Visual Communication',       2, '#F5D02C', false),
    (3, 'illustration_fine_arts', 1, NULL, '插画纯艺科系',   'Illustration & Fine Arts',   3, '#5DB875', false),
    (4, 'game_animation',         1, NULL, '游戏动画科系',   'Game & Animation',           4, '#E8823C', false),
    (5, 'space',                  1, NULL, '空间科系',       'Space & Architecture',       5, '#26A4A8', false),
    (6, 'industrial_interaction', 1, NULL, '工业交互科系',   'Industrial & Interaction',   6, '#515B66', false),
    (7, 'film_media',             1, NULL, '影视传媒科系',   'Film & Media',               7, '#8A5BB0', false);

-- 5. Insert level-2 disciplines. id = parent_id*100 + local_order. display_order mirrors the diagram.

-- ---------- 1. Fashion (101..115) ----------
INSERT INTO public.art_categories
    (id, code, level, parent_id, name_zh, name_en, aliases, display_order, is_interdisciplinary)
VALUES
    (101, 'fashion.apparel_design',        2, 1, '服装设计',   'Apparel Design',          ARRAY['时装设计','Fashion Design'],       1,  false),
    (102, 'fashion.jewellery_design',      2, 1, '珠宝设计',   'Jewellery Design',        ARRAY['Jewelry Design'],                   2,  false),
    (103, 'fashion.textile_design',        2, 1, '纺织品设计', 'Textile Design',          ARRAY[]::text[],                           3,  false),
    (104, 'fashion.fabric_design',         2, 1, '面料设计',   'Fabric / Material Design',ARRAY['Textile Materials'],                4,  false),
    (105, 'fashion.fashion_management',    2, 1, '时尚管理',   'Fashion Management',      ARRAY['Fashion Business'],                 5,  false),
    (106, 'fashion.luxury_management',     2, 1, '奢侈品管理', 'Luxury Brand Management', ARRAY['Luxury Management'],                6,  false),
    (107, 'fashion.design_management',     2, 1, '设计管理',   'Design Management',       ARRAY[]::text[],                           7,  false),
    (108, 'fashion.fashion_buying',        2, 1, '时尚买手',   'Fashion Buying',          ARRAY['Fashion Buyer','Fashion Buying & Merchandising'], 8, false),
    (109, 'fashion.fashion_communication', 2, 1, '时尚传播',   'Fashion Communication',   ARRAY['Fashion Media','Fashion PR'],       9,  false),
    (110, 'fashion.fashion_styling',       2, 1, '时尚造型',   'Fashion Styling',         ARRAY['Styling'],                          10, false),
    (111, 'fashion.costume_design',        2, 1, '戏服设计',   'Costume Design',          ARRAY['Theatre Costume','Stage Costume'],  11, false),
    (112, 'fashion.fashion_photography',   2, 1, '时尚摄影',   'Fashion Photography',     ARRAY[]::text[],                           12, false),
    (113, 'fashion.menswear',              2, 1, '男装设计',   'Menswear Design',         ARRAY['Menswear'],                         13, false),
    (114, 'fashion.womenswear',            2, 1, '女装设计',   'Womenswear Design',       ARRAY['Womenswear'],                       14, false),
    (115, 'fashion.footwear_design',       2, 1, '鞋靴设计',   'Footwear Design',         ARRAY['Shoe Design'],                      15, false);

-- ---------- 2. Visual Communication (201..211) ----------
INSERT INTO public.art_categories
    (id, code, level, parent_id, name_zh, name_en, aliases, display_order, is_interdisciplinary)
VALUES
    (201, 'visual_communication.graphic_design',             2, 2, '平面设计',           'Graphic Design',              ARRAY[]::text[],                               1,  false),
    (202, 'visual_communication.visual_communication_design',2, 2, '视觉传达设计',       'Visual Communication Design', ARRAY[]::text[],                               2,  false),
    (203, 'visual_communication.visual_arts',                2, 2, '视觉艺术',           'Visual Arts',                 ARRAY[]::text[],                               3,  false),
    (204, 'visual_communication.multimedia_design',          2, 2, '多媒体设计',         'Multimedia Design',           ARRAY[]::text[],                               4,  false),
    (205, 'visual_communication.digital_arts_media',         2, 2, '数字艺术与媒体设计', 'Digital Arts and Media Design',ARRAY[]::text[],                              5,  false),
    (206, 'visual_communication.new_media_design',           2, 2, '新媒体设计',         'New Media Design',            ARRAY[]::text[],                               6,  false),
    (207, 'visual_communication.digital_media',              2, 2, '数字媒体',           'Digital Media',               ARRAY[]::text[],                               7,  false),
    (208, 'visual_communication.healing_design',             2, 2, '治愈设计',           'Healing Design',              ARRAY['Therapeutic Design','治愈设计(交叉学科)'], 8,  true),
    (209, 'visual_communication.foundation_design',          2, 2, '基础设计',           'Foundation Design',           ARRAY['Basic Design'],                         9,  false),
    (210, 'visual_communication.packaging_design',           2, 2, '包装设计',           'Packaging Design',            ARRAY[]::text[],                               10, false),
    (211, 'visual_communication.branding_design',            2, 2, '品牌设计',           'Branding Design',             ARRAY['Brand Design','Brand Identity'],        11, false);

-- ---------- 3. Illustration & Fine Arts (301..319) ----------
INSERT INTO public.art_categories
    (id, code, level, parent_id, name_zh, name_en, aliases, display_order, is_interdisciplinary)
VALUES
    (301, 'illustration_fine_arts.illustration_design',        2, 3, '插画设计',       'Illustration Design',               ARRAY['Illustration'],                                     1,  false),
    (302, 'illustration_fine_arts.childrens_illustration',     2, 3, '儿童插画',       'Children''s Book Illustration',     ARRAY['Children''s Illustration'],                         2,  false),
    (303, 'illustration_fine_arts.fine_art',                   2, 3, '纯艺',           'Fine Art',                          ARRAY['Fine Arts'],                                        3,  false),
    (304, 'illustration_fine_arts.installation_art',           2, 3, '装置艺术',       'Installation Art',                  ARRAY[]::text[],                                           4,  false),
    (305, 'illustration_fine_arts.printmaking_painting',       2, 3, '版画/油画/绘画', 'Printmaking / Painting / Drawing',  ARRAY['版画','油画','绘画','Printmaking','Painting','Drawing','Oil Painting'], 5, false),
    (306, 'illustration_fine_arts.sculpture',                  2, 3, '雕塑',           'Sculpture',                         ARRAY[]::text[],                                           6,  false),
    (307, 'illustration_fine_arts.curating',                   2, 3, '策展',           'Curating',                          ARRAY['Curatorial Practice','Curation'],                   7,  false),
    (308, 'illustration_fine_arts.glass_art',                  2, 3, '玻璃艺术',       'Glass Art',                         ARRAY[]::text[],                                           8,  false),
    (309, 'illustration_fine_arts.wood_art',                   2, 3, '木艺',           'Wood Art',                          ARRAY['Woodworking'],                                      9,  false),
    (310, 'illustration_fine_arts.lacquer_art',                2, 3, '漆艺',           'Lacquer Art',                       ARRAY[]::text[],                                           10, false),
    (311, 'illustration_fine_arts.ceramics',                   2, 3, '陶瓷',           'Ceramics',                          ARRAY[]::text[],                                           11, false),
    (312, 'illustration_fine_arts.photography',                2, 3, '摄影/写真',      'Photography',                       ARRAY['写真','Photo'],                                     12, false),
    (313, 'illustration_fine_arts.commercial_photography',     2, 3, '商业摄影',       'Commercial Photography',            ARRAY[]::text[],                                           13, false),
    (314, 'illustration_fine_arts.art_therapy',                2, 3, '艺术疗愈',       'Art Therapy',                       ARRAY[]::text[],                                           14, false),
    (315, 'illustration_fine_arts.arts_management_education',  2, 3, '艺术管理/教育',  'Arts Management / Arts Education',  ARRAY['艺术管理','艺术教育','Arts Management','Arts Education'], 15, false),
    (316, 'illustration_fine_arts.contemporary_art_practice',  2, 3, '当代艺术实践',   'Contemporary Art Practice',         ARRAY[]::text[],                                           16, false),
    (317, 'illustration_fine_arts.avant_garde_expression',     2, 3, '前端艺术表现',   'Avant-garde Artistic Expression',   ARRAY['前卫艺术表现','Avant-garde Art'],                   17, false),
    (318, 'illustration_fine_arts.japanese_painting',          2, 3, '日本画',         'Japanese Painting',                 ARRAY['Nihonga'],                                          18, false),
    (319, 'illustration_fine_arts.art_history',                2, 3, '艺术史/美术史',  'Art History',                       ARRAY['美术史','History of Art'],                          19, false);

-- ---------- 4. Game & Animation (401..414) ----------
INSERT INTO public.art_categories
    (id, code, level, parent_id, name_zh, name_en, aliases, display_order, is_interdisciplinary)
VALUES
    (401, 'game_animation.game_design',            2, 4, '游戏设计',           'Game Design',                 ARRAY[]::text[],                                 1,  false),
    (402, 'game_animation.game_art',               2, 4, '游戏美术',           'Game Art',                    ARRAY[]::text[],                                 2,  false),
    (403, 'game_animation.entertainment_design',   2, 4, '娱乐设计',           'Entertainment Design',        ARRAY[]::text[],                                 3,  false),
    (404, 'game_animation.concept_art',            2, 4, '概念美术设计',       'Concept Art Design',          ARRAY['Concept Art'],                            4,  false),
    (405, 'game_animation.key_art_design',         2, 4, '原画设计',           'Key Art / Original Painting', ARRAY['原画','Key Art'],                         5,  false),
    (406, 'game_animation.computer_animation',     2, 4, '计算机动画',         'Computer Animation',          ARRAY[]::text[],                                 6,  false),
    (407, 'game_animation.experimental_animation', 2, 4, '实验动画',           'Experimental Animation',      ARRAY[]::text[],                                 7,  false),
    (408, 'game_animation.character_animation',    2, 4, '角色动画',           'Character Animation',         ARRAY[]::text[],                                 8,  false),
    (409, 'game_animation.animation_2d_3d',        2, 4, '2D/3D动画',          '2D / 3D Animation',           ARRAY['2D Animation','3D Animation'],            9,  false),
    (410, 'game_animation.stop_motion',            2, 4, '定格动画',           'Stop-motion Animation',       ARRAY['Stop Motion'],                            10, false),
    (411, 'game_animation.acg_cartoon_comic',      2, 4, 'ACG/卡通设计/漫画',  'ACG / Cartoon / Comic',       ARRAY['ACG','卡通设计','漫画','Cartoon','Comic','Manga'], 11, false),
    (412, 'game_animation.vfx_design',             2, 4, '特效设计',           'VFX Design',                  ARRAY['Visual Effects','特效'],                  12, false),
    (413, 'game_animation.game_production',        2, 4, '游戏策划',           'Game Production',             ARRAY['Game Planner','Game Design (Production)'],13, false),
    (414, 'game_animation.game_programming',       2, 4, '电脑游戏编程',       'Computer Game Programming',   ARRAY['Game Programming'],                       14, false);

-- ---------- 5. Space & Architecture (501..515) ----------
INSERT INTO public.art_categories
    (id, code, level, parent_id, name_zh, name_en, aliases, display_order, is_interdisciplinary)
VALUES
    (501, 'space.architectural_design',      2, 5, '建筑设计',         'Architectural Design',       ARRAY['Architecture'],                           1,  false),
    (502, 'space.landscape_design',          2, 5, '景观设计',         'Landscape Design',           ARRAY[]::text[],                                 2,  false),
    (503, 'space.landscape_architecture',    2, 5, '景观建筑',         'Landscape Architecture',     ARRAY[]::text[],                                 3,  false),
    (504, 'space.landscape_urbanism',        2, 5, '景观城市',         'Landscape Urbanism',         ARRAY[]::text[],                                 4,  false),
    (505, 'space.landscape_management',      2, 5, '景观管理',         'Landscape Management',       ARRAY[]::text[],                                 5,  false),
    (506, 'space.landscape_studies',         2, 5, '景观研究',         'Landscape Studies',          ARRAY[]::text[],                                 6,  false),
    (507, 'space.interior_design',           2, 5, '室内设计',         'Interior Design',            ARRAY[]::text[],                                 7,  false),
    (508, 'space.urban_design',              2, 5, '城市设计',         'Urban Design',               ARRAY[]::text[],                                 8,  false),
    (509, 'space.urban_planning',            2, 5, '城市规划',         'Urban Planning',             ARRAY[]::text[],                                 9,  false),
    (510, 'space.environmental_architecture',2, 5, '环境建筑',         'Environmental Architecture', ARRAY[]::text[],                                 10, false),
    (511, 'space.urban_health',              2, 5, '城市健康发展',     'Urban Health Development',   ARRAY['Healthy Urban Development'],              11, false),
    (512, 'space.sustainable_urban_design',  2, 5, '可持续城市设计',   'Sustainable Urban Design',   ARRAY[]::text[],                                 12, false),
    (513, 'space.stage_design',              2, 5, '舞台设计',         'Stage Design',               ARRAY['Scenography'],                            13, false),
    (514, 'space.display_design',            2, 5, '陈列设计',         'Exhibition / Display Design',ARRAY['Exhibition Design','Visual Merchandising'],14, false),
    (515, 'space.environmental_design',      2, 5, '环境设计',         'Environmental Design',       ARRAY[]::text[],                                 15, false);

-- ---------- 6. Industrial & Interaction (601..617) ----------
INSERT INTO public.art_categories
    (id, code, level, parent_id, name_zh, name_en, aliases, display_order, is_interdisciplinary)
VALUES
    (601, 'industrial_interaction.industrial_design',            2, 6, '工业设计',         'Industrial Design',                ARRAY[]::text[],                                 1,  false),
    (602, 'industrial_interaction.interaction_design',           2, 6, '交互设计',         'Interaction Design',               ARRAY[]::text[],                                 2,  false),
    (603, 'industrial_interaction.product_design',               2, 6, '产品设计',         'Product Design',                   ARRAY[]::text[],                                 3,  false),
    (604, 'industrial_interaction.hci',                          2, 6, '人机交互',         'Human-Computer Interaction',       ARRAY['HCI'],                                    4,  false),
    (605, 'industrial_interaction.service_design',               2, 6, '服务设计',         'Service Design',                   ARRAY[]::text[],                                 5,  false),
    (606, 'industrial_interaction.furniture_design',             2, 6, '家具设计',         'Furniture Design',                 ARRAY[]::text[],                                 6,  false),
    (607, 'industrial_interaction.accessory_design',             2, 6, '配饰设计',         'Accessory Design',                 ARRAY[]::text[],                                 7,  false),
    (608, 'industrial_interaction.wearable_design',              2, 6, '穿戴产品设计',     'Wearable Product Design',          ARRAY['Wearable Tech','Wearables'],              8,  false),
    (609, 'industrial_interaction.ux_design',                    2, 6, '用户体验设计',     'User Experience Design',           ARRAY['UX Design','UX'],                         9,  false),
    (610, 'industrial_interaction.transportation_design',        2, 6, '交通工具设计',     'Transportation Design',            ARRAY['Vehicle Design'],                         10, false),
    (611, 'industrial_interaction.automotive_design',            2, 6, '汽车设计',         'Automotive Design',                ARRAY['Car Design'],                             11, false),
    (612, 'industrial_interaction.product_design_installation',  2, 6, '产品/设计装置',    'Product / Design Installation',    ARRAY['设计装置','Design Installation'],         12, false),
    (613, 'industrial_interaction.creative_computing',           2, 6, '创意计算机',       'Creative Computing',               ARRAY[]::text[],                                 13, false),
    (614, 'industrial_interaction.info_experience_design',       2, 6, '信息体验设计',     'Information Experience Design',    ARRAY['IED'],                                    14, false),
    (615, 'industrial_interaction.interactive_media_practice',   2, 6, '交互媒体实践',     'Interactive Media Practice',       ARRAY[]::text[],                                 15, false),
    (616, 'industrial_interaction.information_design',           2, 6, '情报设计',         'Information Design',               ARRAY['Info Design','情报设计(交叉学科)'],        16, true),
    (617, 'industrial_interaction.integrated_design',            2, 6, '统合设计',         'Integrated Design',                ARRAY['统合设计(交叉学科)'],                      17, true);

-- ---------- 7. Film & Media (701..713) ----------
INSERT INTO public.art_categories
    (id, code, level, parent_id, name_zh, name_en, aliases, display_order, is_interdisciplinary)
VALUES
    (701, 'film_media.film_production',    2, 7, '电影制作',        'Film Production',               ARRAY['Filmmaking'],                           1,  false),
    (702, 'film_media.film_studies',       2, 7, '电影研究',        'Film Studies',                  ARRAY[]::text[],                               2,  false),
    (703, 'film_media.tv_film_production', 2, 7, '影视制作',        'Film & Television Production',  ARRAY['TV Production'],                        3,  false),
    (704, 'film_media.producing',          2, 7, '制片人',          'Producing',                     ARRAY['Producer','Film Producing'],            4,  false),
    (705, 'film_media.sound_design',       2, 7, '声效',            'Sound Design',                  ARRAY['Sound','Sound for Film'],               5,  false),
    (706, 'film_media.editing',            2, 7, '剪辑',            'Film Editing',                  ARRAY['Editing','Post-production Editing'],    6,  false),
    (707, 'film_media.videography',        2, 7, '摄像',            'Videography',                   ARRAY['Camera Operation'],                     7,  false),
    (708, 'film_media.lighting_design',    2, 7, '灯光照明设计',    'Lighting Design',               ARRAY['Lighting for Film'],                    8,  false),
    (709, 'film_media.screenwriting',      2, 7, '剧本/脚本',       'Screenwriting',                 ARRAY['剧本','脚本','Script','Scriptwriting'], 9,  false),
    (710, 'film_media.directing',          2, 7, '导演/监督',       'Directing',                     ARRAY['Director','监督','Film Directing'],     10, false),
    (711, 'film_media.cinematography',     2, 7, '电影摄影',        'Cinematography',                ARRAY[]::text[],                               11, false),
    (712, 'film_media.documentary',        2, 7, '电影制作-纪录片', 'Documentary Filmmaking',        ARRAY['纪录片','Documentary'],                 12, false),
    (713, 'film_media.creative_producing', 2, 7, '创意制作-电影',   'Creative Producing (Film)',     ARRAY['Creative Film Producing'],              13, false);

-- 6. Re-create the FK from program_art_categories.category_id to the new art_categories(id).
ALTER TABLE public.program_art_categories
    DROP CONSTRAINT IF EXISTS program_art_categories_category_id_fkey;

ALTER TABLE public.program_art_categories
    ADD CONSTRAINT program_art_categories_category_id_fkey
    FOREIGN KEY (category_id) REFERENCES public.art_categories(id) ON DELETE RESTRICT;

COMMIT;

-- 7. Sanity counts — verify visually in the SQL Editor results pane.
--    Expected: level1_rows = 7, level2_rows = 104, total_rows = 111.
SELECT
    (SELECT count(*) FROM public.art_categories WHERE level = 1) AS level1_rows,
    (SELECT count(*) FROM public.art_categories WHERE level = 2) AS level2_rows,
    (SELECT count(*) FROM public.art_categories)                 AS total_rows;
