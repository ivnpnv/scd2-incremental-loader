/*
SCD2 Incremental Load Procedure
Handles inserts, updates, and deletes with full history tracking
*/

BEGIN;
-- 1. Очистка staging areas
TRUNCATE TABLE punov_stg;
TRUNCATE TABLE punov_stg_del;

-- 2. Захват инкрементальных данных
INSERT INTO ""punov_stg (id, val, update_dt)
SELECT id, val, update_dt 
FROM punov_source
WHERE update_dt > (
    SELECT max_update_dt 
    FROM punov_meta 
    WHERE schema_name = 'detn' AND table_name = 'punov_source'
);

-- 3. Захват ID для проверки удалений
INSERT INTO punov_stg_del (id)
SELECT id FROM punov_source;

-- 4. Обработка новых записей (INSERT)
INSERT INTO punov_target_hist (id, val, effective_from, effective_to, deleted_flg)
SELECT
    stg.id, 
    stg.val, 
    stg.update_dt, 
    '2999-12-31'::TIMESTAMP,
    'N'
FROM punov_stg stg
LEFT JOIN punov_target_hist tgt
    ON stg.id = tgt.id 
    AND tgt.effective_to = '2999-12-31'::TIMESTAMP
    AND tgt.deleted_flg = 'N'
WHERE tgt.id IS NULL;

-- 5. Обработка изменений (UPDATE)
-- Закрытие текущей активной версии
UPDATE punov_target_hist
SET effective_to = stg.update_dt - INTERVAL '1 second'
FROM punov_stg stg
WHERE punov_target_hist.id = stg.id
    AND punov_target_hist.effective_to = '2999-12-31'::TIMESTAMP
    AND punov_target_hist.deleted_flg = 'N'
    AND (stg.val != punov_target_hist.val 
         OR (stg.val IS NULL AND punov_target_hist.val IS NOT NULL) 
         OR (stg.val IS NOT NULL AND punov_target_hist.val IS NULL));

-- Создание новой версии
INSERT INTO punov_target_hist (id, val, effective_from, effective_to, deleted_flg)
SELECT
    stg.id, 
    stg.val, 
    stg.update_dt, 
    '2999-12-31'::TIMESTAMP,
    'N'
FROM punov_stg stg
INNER JOIN punov_target_hist tgt
    ON stg.id = tgt.id
    AND tgt.effective_to = stg.update_dt - INTERVAL '1 second'
    AND tgt.deleted_flg = 'N';

-- 6. Обработка удалений (DELETE)
-- Помечаем удаленные записи
INSERT INTO punov_target_hist (id, val, effective_from, effective_to, deleted_flg)
SELECT
    tgt.id, 
    tgt.val, 
    CURRENT_TIMESTAMP,
    '2999-12-31'::TIMESTAMP,
    'Y'
FROM punov_target_hist tgt
WHERE tgt.id IN (
    SELECT tgt.id
    FROM punov_target_hist tgt
    LEFT JOIN punov_stg_del stg ON tgt.id = stg.id
    WHERE stg.id IS NULL
        AND tgt.effective_to = '2999-12-31'::TIMESTAMP
        AND tgt.deleted_flg = 'N'
) AND tgt.effective_to = '2999-12-31'::TIMESTAMP
    AND tgt.deleted_flg = 'N';

-- Закрываем активные версии удаленных записей
UPDATE punov_target_hist
SET effective_to = CURRENT_TIMESTAMP - INTERVAL '1 second'
WHERE id IN (
    SELECT tgt.id
    FROM punov_target_hist tgt
    LEFT JOIN punov_stg_del stg ON tgt.id = stg.id
    WHERE stg.id IS NULL
        AND tgt.effective_to = '2999-12-31'::TIMESTAMP
        AND tgt.deleted_flg = 'N'
) AND effective_to = '2999-12-31'::TIMESTAMP
    AND deleted_flg = 'N';

-- 7. Обновление метаданных
UPDATE punov_meta 
SET 
    max_update_dt = COALESCE((SELECT MAX(update_dt) FROM punov_stg), max_update_dt),
    last_processed_dt = CURRENT_TIMESTAMP
WHERE schema_name = 'detn' AND table_name = 'punov_source';

COMMIT;