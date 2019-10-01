-- Ce script permet de créer un index
-- sur la colonne adresse_id

CREATE INDEX s3ic_adresse_id_idx
ON etl.s3ic_with_version (adresse_id)