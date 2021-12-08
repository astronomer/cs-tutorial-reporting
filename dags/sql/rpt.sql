-- CREATE SCHEMA IF NOT EXISTS rpt;
DROP TABLE IF EXISTS rpt.dag CASCADE;
DROP TABLE IF EXISTS rpt.dag_run CASCADE;
DROP TABLE IF EXISTS rpt.task_instance CASCADE;

CREATE TABLE IF NOT EXISTS rpt.dag
(
    dag_id character varying(250) COLLATE pg_catalog."default" NOT NULL,
    is_paused boolean,
    is_subdag boolean,
    is_active boolean,
    fileloc character varying(2000) COLLATE pg_catalog."default",
    file_token character varying(2000) COLLATE pg_catalog."default",
    owners character varying(2000) COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    root_dag_id text COLLATE pg_catalog."default",
    schedule_interval text COLLATE pg_catalog."default",
    CONSTRAINT dag_pkey PRIMARY KEY (dag_id)
);

CREATE TABLE IF NOT EXISTS rpt.dag_run
(
    dag_id character varying(250) COLLATE pg_catalog."default" NOT NULL,
    dag_run_id character varying(250) COLLATE pg_catalog."default" NOT NULL,
    end_date timestamp with time zone,
    execution_date timestamp with time zone NOT NULL,
    external_trigger boolean,
    logical_date timestamp with time zone NOT NULL,
    start_date timestamp with time zone,
    state character varying(50) COLLATE pg_catalog."default",

    CONSTRAINT dag_run_pkey PRIMARY KEY (dag_run_id),
    CONSTRAINT dag_run_dag_id_execution_date_key UNIQUE (dag_id, execution_date),
    CONSTRAINT dag_run_dag_id_run_id_key UNIQUE (dag_id, dag_run_id)
);

CREATE TABLE IF NOT EXISTS rpt.task_instance
(
    dag_id character varying(250) COLLATE pg_catalog."default" NOT NULL,
    task_id character varying(250) COLLATE pg_catalog."default" NOT NULL,
    execution_date timestamp with time zone,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    duration double precision,
    state character varying(20) COLLATE pg_catalog."default",
    try_number integer,
    max_tries integer,
    hostname character varying(1000) COLLATE pg_catalog."default",
    unixname character varying(1000) COLLATE pg_catalog."default",
    pool character varying(256) COLLATE pg_catalog."default" NOT NULL,
    pool_slots integer,
    queue character varying(256) COLLATE pg_catalog."default",
    priority_weight integer,
    operator character varying(1000) COLLATE pg_catalog."default",
    queued_when timestamp with time zone,
    pid integer,
    executor_config bytea
--     CONSTRAINT task_instance_pkey PRIMARY KEY (dag_id, task_id, execution_date),
--     CONSTRAINT task_instance_dag_run_fkey FOREIGN KEY (execution_date, dag_id)
--         REFERENCES rpt.dag_run (execution_date, dag_id) MATCH SIMPLE
--         ON UPDATE NO ACTION
--         ON DELETE CASCADE
)


