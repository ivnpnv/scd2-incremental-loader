
-- Source table (имитация внешней системы)
CREATE TABLE detn.punov_source( 
    id INTEGER PRIMARY KEY,
    val VARCHAR(50),
    update_dt TIMESTAMP(0) NOT NULL
);

-- Staging table для новых/измененных данных
CREATE TABLE detn.punov_stg( 
    id INTEGER,
    val VARCHAR(50),
    update_dt TIMESTAMP(0),
    processed_dt TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP
);

-- Staging table для удаленных записей
CREATE TABLE detn.punov_stg_del( 
    id INTEGER PRIMARY KEY
);

-- Target table с историей изменений (SCD2)
CREATE TABLE detn.punov_target_hist (
    id INTEGER,
    val VARCHAR(50),
    effective_from TIMESTAMP(0) NOT NULL,
    effective_to TIMESTAMP(0) NOT NULL,
    deleted_flg CHAR(1) DEFAULT 'N',
    processed_dt TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_punov_target_hist PRIMARY KEY (id, effective_from)
);

-- Metadata table для отслеживания последней загрузки
CREATE TABLE detn.punov_meta(
    schema_name VARCHAR(30) NOT NULL,
    table_name VARCHAR(30) NOT NULL,
    max_update_dt TIMESTAMP(0) NOT NULL,
    last_processed_dt TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_punov_meta PRIMARY KEY (schema_name, table_name)
);