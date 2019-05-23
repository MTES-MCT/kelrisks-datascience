SELECT
  a.identifiant,
  a.adresse,
  a.raison_sociale,
  a.nom,
  b.code_postal,
  b.code_insee,
  b.nom_commune,
  ST_X(ST_CENTROID(a.geog)) as longitude,
  ST_Y(ST_CENTROID(a.geog)) as latitude
FROM kelrisks.basias as a
LEFT JOIN kelrisks.adresse_commune as b
ON a.commune = b.code_insee
WHERE a.commune like '91%'