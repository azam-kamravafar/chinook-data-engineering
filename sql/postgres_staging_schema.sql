--
-- PostgreSQL database dump
--

\restrict aF2Q7l23HBX1Q1SRygZfQCaFHlD2S4oqC4tCOYKaqRQkoRf5R94Nzwnl8A1Wdmg

-- Dumped from database version 16.16
-- Dumped by pg_dump version 16.16

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: staging; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA staging;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: customer_change_events; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.customer_change_events (
    event_id bigint NOT NULL,
    entity_name text NOT NULL,
    entity_id integer NOT NULL,
    operation_type text NOT NULL,
    before_data jsonb,
    after_data jsonb,
    source_created_at_utc timestamp with time zone NOT NULL,
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    ingested_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT valid_operation CHECK ((operation_type = ANY (ARRAY['INSERT'::text, 'UPDATE'::text, 'DELETE'::text])))
);


--
-- Name: event_processing; Type: TABLE; Schema: staging; Owner: -
--

CREATE TABLE staging.event_processing (
    event_id bigint NOT NULL,
    entity_name text NOT NULL,
    entity_id integer NOT NULL,
    status text DEFAULT 'PROCESSING'::text NOT NULL,
    old_customer_key bigint,
    new_customer_key bigint,
    attempt_count integer DEFAULT 1 NOT NULL,
    started_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at_utc timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at_utc timestamp with time zone,
    error_message text,
    CONSTRAINT applied_requires_completion CHECK (((status <> 'APPLIED'::text) OR (completed_at_utc IS NOT NULL))),
    CONSTRAINT event_processing_attempt_count_check CHECK ((attempt_count >= 1)),
    CONSTRAINT valid_processing_status CHECK ((status = ANY (ARRAY['PROCESSING'::text, 'APPLIED'::text, 'FAILED'::text])))
);


--
-- Name: customer_change_events customer_change_events_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.customer_change_events
    ADD CONSTRAINT customer_change_events_pkey PRIMARY KEY (event_id);


--
-- Name: event_processing event_processing_pkey; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.event_processing
    ADD CONSTRAINT event_processing_pkey PRIMARY KEY (event_id);


--
-- Name: customer_change_events unique_kafka_position; Type: CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.customer_change_events
    ADD CONSTRAINT unique_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset);


--
-- Name: idx_event_processing_status; Type: INDEX; Schema: staging; Owner: -
--

CREATE INDEX idx_event_processing_status ON staging.event_processing USING btree (status, event_id);


--
-- Name: event_processing event_processing_event_id_fkey; Type: FK CONSTRAINT; Schema: staging; Owner: -
--

ALTER TABLE ONLY staging.event_processing
    ADD CONSTRAINT event_processing_event_id_fkey FOREIGN KEY (event_id) REFERENCES staging.customer_change_events(event_id);


--
-- PostgreSQL database dump complete
--

\unrestrict aF2Q7l23HBX1Q1SRygZfQCaFHlD2S4oqC4tCOYKaqRQkoRf5R94Nzwnl8A1Wdmg
