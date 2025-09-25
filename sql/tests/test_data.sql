-- Добавление метаданных для таблицы punov_source
INSERT INTO detn.punov_meta(schema_name, table_name, max_update_dt)
VALUES('detn', 'punov_source', '1900-01-01'::TIMESTAMP);

-- Приведены тесты для проверки работы SCD2 загрузчика
-- Тесты можно запускать по отдельности для проверки отдельных частей загрузки
-- или все вместе для проверки полной функциональности

--1. Заполнение исходной таблицы данными
INSERT INTO detn.punov_source (id, val, update_dt) VALUES 
(1, 'A', CURRENT_TIMESTAMP),
(2, 'B', CURRENT_TIMESTAMP);

--2. Обновление и добавление новых записей
UPDATE detn.punov_source SET val = 'A_updated', update_dt = CURRENT_TIMESTAMP WHERE id = 1;
INSERT INTO detn.punov_source (id, val, update_dt) VALUES (3, 'C', CURRENT_TIMESTAMP);

--3. Удаление записей
DELETE FROM detn.punov_source WHERE id = 2;