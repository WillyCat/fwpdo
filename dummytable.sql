CREATE DATABASE IF NOT EXISTS dummydb;
USE dummydb;
CREATE TABLE IF NOT EXISTS dummytable ( `id` INT(11) UNSIGNED NOT NULL , `col1` VARCHAR(128) NULL , PRIMARY KEY (`id`)) ENGINE = InnoDB;
