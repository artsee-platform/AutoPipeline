# Final May 17 Duplicate Cleanup - 2026-05-19

## Summary

- Source table: `public.schools`
- Duplicate candidate rows reviewed: 21
- Rows created on 2026-05-17 remaining after cleanup: 0
- Total schools after cleanup: 212
- Full row/document backups were kept locally and are not committed to Git.

## Cleanup Stages

1. First pass deleted 2 `pending` May 17 school rows with no dependent rows.
2. Second pass found 17 remaining `pending` May 17 school rows with only `school_documents` dependents.
3. The 17 `school_documents` rows were backed up locally, then deleted with their parent duplicate school rows.
4. Two candidate IDs were already absent from `schools` before the final pass.

## Deleted Schools

- `b5e9f9c0-3214-4b48-9912-5648c895cd04` | Durban University of Technology / 德班理工大学
- `f1d4b8ae-3cfe-40a0-8282-94deeaf0f0df` | University of Dar es Salaam / 达累斯萨拉姆大学
- `946e886e-2297-4f91-9af3-c2a71d376c96` | University of Alberta / 阿尔伯塔大学
- `e1de020f-22da-45ea-a925-f3b3d8903c6b` | Auckland University of Technology / 奥克兰理工大学
- `b00e577b-2ca6-4bd3-8545-181c88d1f127` | Central Academy of Fine Arts / 中央美术学院
- `7408a8aa-e814-47c0-aa24-101bcc68ec9f` | Ecole Nationale Superieure des Beaux-Arts / 巴黎美术学院
- `d2d3b3bb-2c87-4a95-91ce-9e4d33dee33d` | Faculty of Fine Arts Cairo / 开罗美术学院
- `9739879c-f2e9-4493-9a9e-b05b4e6bb5bd` | Otis College of Art and Design / 奥蒂斯艺术与设计学院
- `78c07336-b8e8-451b-a230-3fd9732ca518` | Tsinghua Academy of Fine Arts / 清华大学美术学院
- `f825f393-1a5a-48df-860c-338645cdb40d` | Universidad Nacional de las Artes / 阿根廷国家艺术大学
- `9747d46e-8d64-4d07-bd17-790bc671744a` | University of Aberdeen / 阿伯丁大学
- `f012bc1e-a94f-42d0-97f6-8bd8aadcdc58` | University of Algiers / 阿尔及尔大学
- `7bb08f59-5c0f-464f-91be-7feff4c899d0` | University of Applied Arts Vienna / 维也纳应用艺术大学
- `228076db-1937-4dc8-810c-6836cb1db84f` | UC Berkeley Art Practice / 伯克利艺术实践系
- `52d3cf39-5cd4-47d7-a266-ab7791b0ed77` | University of Alberta / 阿尔伯塔大学
- `bbaf2fed-8e2c-48aa-8dda-916b1ab400d7` | UNC Chapel Hill / 北卡罗来纳大学教堂山分校
- `b2ab0a41-07ad-445a-b90a-de6db08f6006` | University of Panama / 巴拿马大学
- `06785c0e-c372-4fa0-8423-67ce606469c1` | Emily Carr University of Art and Design / 艾米丽卡尔艺术与设计大学
- `c860ea14-4abc-4294-8d75-2be687e4d948` | OCAD University / 安大略艺术设计大学

## Already Absent Before Final Pass

- `b21b93bb-5432-4876-a0b9-9af84fb6ad8c` | National Institute of Arts Bamako / 巴马科国家艺术学院
- `dd8045ba-2f9b-4bde-a1d8-2f62243b1201` | National School of Arts Dakar / 达喀尔国家艺术学校

## Notes

- The deleted rows were the newer May 17 `pending` duplicates.
- Older `active` or `done` rows with fuller data, official websites, and program references were retained.
- Backup files remain under `data/media_review/` locally, but the directory is ignored by Git by default.
