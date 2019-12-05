-- Ce script permet de créer un index
-- sur la colonne adresse_id

CREATE INDEX basias_adresse_id_idx
ON etl.basias_sites_with_version (adresse_id)