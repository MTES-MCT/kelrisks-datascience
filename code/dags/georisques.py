# -*- coding=utf-8 -*-


def get_s3ic_shp_file_url(departement):
    """
    Géorisques permet de récupérer les fichiers installations classées
    au format shapefile pour chaque département séparément

    https://www.georisques.gouv.fr/dossiers/telechargement
    """

    url = "http://mapsref.brgm.fr/wxs/georisques/georisques_dl?" + \
        "&service=wfs&version=2.0.0&request=getfeature&" + \
        "typename=InstallationsClassees_France&outputformat=shapezip" +\
        "&srsname=EPSG:4326&Filter=<Filter><PropertyIsEqualTo" + \
        "><PropertyName>num_dep</PropertyName><Literal" + \
        ">{dep}</Literal></PropertyIsEqualTo></Filter>".format(dep=departement)

    return url
