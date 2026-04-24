-- This SQL script defines the schema for a database named "Team8" that contains two tables: "variant_table" and "patient_table"
-- The "variant_table" is designed to store information about genetic variants, while the "patient_table" is intended to store information about patients and their associated genetic variants
-- The script includes commands to drop existing tables if they exist and then create new tables with specified columns and data types

-- define the database to be used
USE Team8;

-- drop existing tables if they exist to avoid conflicts when creating new tables
DROP TABLE IF EXISTS patient_table;
DROP TABLE IF EXISTS variant_table;

-- Create the variant_table to store information about genetic variants
CREATE TABLE variant_table (
    variant_id INT NOT NULL,
    gene VARCHAR(100),
    transcript VARCHAR(100),
    hgvsc VARCHAR(255),
    protein_change VARCHAR(255),
    algorithm_prediction VARCHAR(255),
    taiwan_biobank_af VARCHAR(50),
    gnomad_exome_east_af VARCHAR(50),
    gnomad_all_af VARCHAR(50),
    gnomad_genome_east_af VARCHAR(50),
    variant_change_type VARCHAR(100),
    PRIMARY KEY (variant_id)
) ENGINE=InnoDB;

-- Create the patient_table to store information about patients and their associated variants
CREATE TABLE patient_table (
    sample_id INT NOT NULL,
    sex VARCHAR(10),
    birthday DATE,
    diagnosis VARCHAR(100),
    inheritancepattern VARCHAR(100),
    variant1_id INT NULL,
    variant1_zygosity VARCHAR(50),
    variant2_id INT NULL,
    variant2_zygosity VARCHAR(50),
    PRIMARY KEY (sample_id)
) ENGINE=InnoDB;

--  ignore 1 rows is to skip the header row in the CSV file
LOAD DATA LOCAL INFILE '/home/students_26/nobma/final_proj/variant_table.csv'
INTO TABLE variant_table
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
--  ignore 1 rows is to skip the header row in the CSV file
IGNORE 1 ROWS
(variant_id,
    gene,
    transcript,
    hgvsc,
    protein_change,
    algorithm_prediction,
    taiwan_biobank_af,
    gnomad_exome_east_af,
    gnomad_all_af,
    gnomad_genome_east_af,
 variant_change_type);

-- Import data into patient_table from a CSV file, handling date formatting and potential empty fields for variant IDs
-- variant1_id and variant2_id are set to NULL if the corresponding CSV fields are empty
-- birthday is converted from a string to a DATE format

LOAD DATA LOCAL INFILE '/home/students_26/nobma/final_proj/patient_table.csv'
INTO TABLE patient_table
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
-- Use user-defined variables to temporarily hold the raw data from the CSV file for fields that require special handling (birthday and variant IDs)
(sample_id,
    sex,
    @birthday_raw, -- YYYY/MM/DD from CSV
    @birthday_year_raw, -- ignored
    @current_age_raw, -- ignored
    diagnosis,
    inheritancepattern, -- on CSV: inheritance_pattern
    @variant1_raw,
    variant1_zygosity,
    @variant2_raw,
    variant2_zygosity
    )
SET birthday = CASE
                WHEN @birthday_raw = '' THEN NULL
                ELSE STR_TO_DATE(@birthday_raw, '%Y/%m/%d')
            END,
    variant1_id = NULLIF(@variant1_raw, ''),
    variant2_id = NULLIF(@variant2_raw, ''); 
-- Use CASE statements to handle the conversion of the birthday field and to set variant IDs to NULL if they are empty strings


-- sanity check once the data is imported
SELECT sample_id, sex, birthday
FROM patient_table
LIMIT 10;

SELECT COUNT(*) AS v1_nulls
FROM patient_table
WHERE variant1_id IS NULL;

SELECT COUNT(*) AS v2_nulls
FROM patient_table
WHERE variant2_id IS NULL;

# check that every non-NULL variant ids exists

-- Check variant1_id
SELECT p.sample_id, p.variant1_id
FROM patient_table p
LEFT JOIN variant_table v ON p.variant1_id = v.variant_id
WHERE p.variant1_id IS NOT NULL AND v.variant_id IS NULL
LIMIT 20;

-- Check variant2_id
SELECT p.sample_id, p.variant2_id
FROM patient_table p
LEFT JOIN variant_table v ON p.variant2_id = v.variant_id
WHERE p.variant2_id IS NOT NULL AND v.variant_id IS NULL
LIMIT 20;

-- Foreign key constraints
-- Add foreign key constraints to ensure that variant1_id and variant2_id in patient_table reference valid variant_id entries in variant_table
-- ON UPDATE CASCADE ensures that if a variant_id in variant_table is updated, the corresponding variant1_id or variant2_id in patient_table will also be updated
-- ON DELETE RESTRICT prevents deletion of a variant_id in variant_table if it is referenced by any patient in patient_table, ensuring data integrity

ALTER TABLE patient_table
ADD CONSTRAINT fk_patient_variant1
FOREIGN KEY (variant1_id)
REFERENCES variant_table (variant_id)
ON UPDATE CASCADE
ON DELETE RESTRICT;

ALTER TABLE patient_table
ADD CONSTRAINT fk_patient_variant2
FOREIGN KEY (variant2_id)
REFERENCES variant_table (variant_id)
ON UPDATE CASCADE
ON DELETE RESTRICT;

-- create view the dynamic age column
-- this gives a “virtual” patient table with an always‑current currentage column for the website

CREATE OR REPLACE VIEW patient_with_age AS
SELECT
    sample_id,
    sex,
    birthday,
    TIMESTAMPDIFF(YEAR, birthday, CURDATE()) AS currentage,
    diagnosis,
    inheritancepattern,
    variant1_id,
    variant1_zygosity,
    variant2_id,
    variant2_zygosity
FROM patient_table;

-- check the age view table to ensure that the currentage column is calculated correctly and that the view is functioning as expected
SELECT * FROM patient_with_age
LIMIT 20;