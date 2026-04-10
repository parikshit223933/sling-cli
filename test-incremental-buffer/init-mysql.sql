-- Matches LSQ Prospect_Base shape closely: datetime(0) for the update_key,
-- timestamp(3) as a secondary column, VARCHAR stage. Session TZ is IST
-- (set in docker-compose command) to mirror the LSQ replica.

CREATE DATABASE IF NOT EXISTS test_src;
USE test_src;

DROP TABLE IF EXISTS events;

CREATE TABLE events (
  id            INT PRIMARY KEY,
  stage         VARCHAR(50)  NOT NULL,
  note          VARCHAR(255) NULL,
  modified_on   DATETIME     NOT NULL,
  last_modified TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  INDEX idx_mod (modified_on)
) ENGINE=InnoDB;

-- Separate table for the integer-update-key edge case test.
DROP TABLE IF EXISTS events_intkey;

CREATE TABLE events_intkey (
  id       BIGINT PRIMARY KEY AUTO_INCREMENT,
  stage    VARCHAR(50) NOT NULL
) ENGINE=InnoDB;
