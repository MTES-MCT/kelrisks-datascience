<table>
  <tr>
    <th>&nbsp;</th>
    <th>s3ic</th>
    <th>Basias</th>
    <th>Basol</th>
    <th>SIS</th>
  </tr>
  <tr>
    <td>Fichiers sources</td>
    <td>
      <ul>
        <li>
          <a href=https://drive.google.com/drive/folders/191KiAzktNSn7eKlzDQ4BbXXACaLb-u2S)>Export IDF Laurent
          </a>
        </li>
        <li>
        <a href=http://www.georisques.gouv.fr/dossiers/telechargement>Shapefile Géorisques pour le reste de la France</a>
        </li>
      </ul>
    </td>
    <td>
      <ul>
        <li>
          <a href=https://drive.google.com/file/d/1KUUncAf3p4hkrd9dFxaKG1KqblMpoPcM/view?usp=sharing>Shapefile Géorisques</a>
        </li>
        <li>
          <a href=https://drive.google.com/open?id=1GJR_tRju5LS9XgW_l_n4JNwTIy2H7udD>Dump SQL Géorisques 20190305_basias_@BRGM.sql</a>
        </li>
      </ul>
    </td>
    <td>
      <a href=https://drive.google.com/open?id=14idc18pOupjgiQnxjJXzC10EnIO4Fypu>
      Dump SQL Géorisques 20190305_basol_@BRGM.sql</a>
    </td>
    <td>
      <a href=https://drive.google.com/open?id=1sJeY7gdLE-U4ZumDYynFQfnhTA7DNi8b>Dump SQL Géorisques 20190221_SIS_@BRGM.sql</a>
    </td>
  </tr>
  <tr>
    <td>Nombre d'enregistrements</td>
    <!-- SIIIC -->
    <td>
      {{ s3ic_count }} (Ile de France)
    </td>
    <!-- BASIAS -->
    <td>
      290768
    </td>
    <!-- BASOL -->
    <td>
      {{ basol_count }}
    </td>
    <!-- SIS -->
    <td>
      {{ sis_count }}
    </td>
  </tr>
  <tr>
    <td>Complétude des données</td>
    <!-- SIIIC -->
    <td>
      <table>
        <tr>
          <td>Ile-de-France</td>
          <td>100%</td>
        </tr>
        <tr>
          <td>Reste de la France</td>
          <td>~ 10%</td>
        </tr>
      </table>
    </td>
    <!-- BASIAS -->
    <td>100%</td>
    <!-- BASOL -->
    <td>100%</td>
    <!-- SIS -->
    <td>100%</td>
  </tr>
  <tr>
    <td>Précision des données</td>
    <!-- SIIIC -->
    <td>
      <ul>
        <li> Ile-de-France
        <table>
          <tr>
            <th>Précision</th>
            <th>Nombre</th>
            <th>Pourcentage</th>
          </tr>
          <tr>
            <td>Parcelle</td>
            <td>{{ s3ic_accurate_coordinates_count }}</td>
            <td>{{ "%.1f %%" % ((s3ic_accurate_coordinates_count / s3ic_count) * 100) }}</td>
          </tr>
          <tr>
            <td>Numéro de rue</td>
            <td></td>
            <td></td>
          </tr>
          <tr>
            <td>Rue</td>
            <td></td>
            <td></td>
          </tr>
          <tr>
            <td>Localité</td>
            <td></td>
            <td></td>
          </tr>
          <tr>
            <td>Commune</td>
            <td> {{ s3ic_centroide_commune_count}} </td>
            <td>{{ "%.1f %%" % ((s3ic_centroide_commune_count / s3ic_count) * 100)}}</td>
          </tr>
        </table>
        </li>
      </ul>
    </td>
    <!-- BASIAS -->
    <td>
      TODO
    </td>
    <!-- BASOL -->
    <td>
      <table>
        <tr>
          <th>Précision</th>
          <th>Nombre</th>
          <th>Pourcentage</th>
        </tr>
        <tr>
          <td>Parcelle</td>
          <td>{{ basol_parcelle_count }}</td>
          <td>{{ "%.1f %%" % ((basol_parcelle_count / basol_count) * 100) }}</td>
        </tr>
        <tr>
          <td>Numéro de rue</td>
          <td>{{ basol_housenumber_count }}</td>
          <td>{{"%.1f %%" % ((basol_housenumber_count / basol_count) * 100)}}</td>
        </tr>
        <tr>
          <td>Rue</td>
          <td>{{ basol_street_count }}</td>
          <td>{{"%.1f %%" % ((basol_street_count / basol_count) * 100)}}</td>
        </tr>
        <tr>
          <td>Localité</td>
          <td></td>
          <td></td>
        </tr>
        <tr>
          <td>Commune</td>
          <td>{{ basol_municipality_count }}</td>
          <td>{{"%.1f %%" % ((basol_municipality_count / basol_count) * 100)}}</td>
        </tr>
      </table>
    </td>
    <!-- SIS -->
    <td>
      <table>
        <tr>
          <th>Précision</th>
          <th>Nombre</th>
          <th>Pourcentage</th>
        </tr>
        <tr>
          <td>Parcelle</td>
          <td>{{sis_count}}</td>
          <td>100%</td>
        </tr>
        <tr>
          <td>Numéro de rue</td>
          <td></td>
          <td></td>
        </tr>
        <tr>
          <td>Rue</td>
          <td></td>
          <td></td>
        </tr>
        <tr>
          <td>Localité</td>
          <td></td>
          <td></td>
        </tr>
        <tr>
          <td>Commune</td>
          <td></td>
          <td></td>
        </tr>
      </table>
    </td>
  </tr>
  <tr>
    <td>Prise en compte de la précision</td>
    <!-- SIIIC -->
    <td>Oui</td>
    <!-- BASIAS -->
    <td>Non</td>
    <!-- BASOL -->
    <td>Non</td>
    <td>
      NA (100% de géoloc précise)
    </td>
  </tr>
  <tr>
    <td>Géocodage des adresses</td>
    <!-- SIIIC -->
    <td>
      <ul>
        <li> Ile-de-France
          <table>
            <tr>
              <th>Précision</th>
              <th>Nombre</th>
              <th>Pourcentage</th>
            </tr>
            <tr>
              <td>Parcelle</td>
              <td></td>
              <td></td>
            </tr>
            <tr>
              <td>Numéro de rue</td>
              <td>{{ s3ic_geocoded_housenumber_count }}</td>
              <td>{{"%.1f %%" % ((s3ic_geocoded_housenumber_count / s3ic_count) * 100)}}</td>
            </tr>
            <tr>
              <td>Rue</td>
              <td>{{ s3ic_geocoded_street_count }}</td>
              <td>{{"%.1f %%" % ((s3ic_geocoded_street_count / s3ic_count) * 100)}}</td>
            </tr>
            <tr>
              <td>Localité</td>
              <td>{{ s3ic_geocoded_locality_count }}</td>
              <td>{{"%.1f %%" % ((s3ic_geocoded_locality_count / s3ic_count) * 100)}}</td>
            </tr>
            <tr>
              <td>Commune</td>
              <td>{{ s3ic_geocoded_municipality_count }}</td>
              <td>{{"%.1f %%" % ((s3ic_geocoded_municipality_count / s3ic_count) * 100)}}</td>
            </tr>
          </table>
        </li>
      </ul>
    </td>
    <!-- BASIAS -->
    <td>
      TODO
    </td>
    <!-- BASOL -->
    <td>
      <table>
        <tr>
          <th>Précision</th>
          <th>Nombre</th>
          <th>Pourcentage</th>
        </tr>
        <tr>
          <td>Parcelle</td>
          <td></td>
          <td></td>
        </tr>
        <tr>
          <td>Numéro de rue</td>
          <td>{{ basol_geocoded_housenumber_count }}</td>
          <td>{{"%.1f %%" % ((basol_geocoded_housenumber_count / basol_count) * 100)}}</td>
        </tr>
        <tr>
          <td>Rue</td>
          <td>{{ basol_geocoded_street_count }}</td>
          <td>{{"%.1f %%" % ((basol_geocoded_street_count / basol_count) * 100)}}</td>
        </tr>
        <tr>
          <td>Localité</td>
          <td>{{ basol_geocoded_locality_count }}</td>
          <td>{{"%.1f %%" % ((basol_geocoded_locality_count / basol_count) * 100)}}</td>
        </tr>
        <tr>
          <td>Commune</td>
          <td>{{ basol_geocoded_municipality_count }}</td>
          <td>{{"%.1f %%" % ((basol_geocoded_municipality_count / basol_count) * 100)}}</td>
        </tr>
      </table>
    </td>
    <!-- SIS -->
    <td></td>
  </tr>
  <tr>
    <td> KPI redressement des données </td>
    <!-- SIIIC -->
    <td>
      <ul>
        <li> Ile-de-France
          <p> Précision des données intiales: {{ "%.1f %%" % s3ic_initial_precision }} </p>
          <p> Précision des données après géocodage: {{ "%.1f %%" % s3ic_precision_after_geocodage }} </p>
        </li>
      </ul>
    </td>
    <!-- BASIAS -->
    <td>TODO</td>
    <!-- BASOL -->
    <td>
       <p> Précision des données intiales: {{ "%.1f %%" % basol_initial_precision }} </p>
       <p> Précision des données après prise en compte des parcelles: {{ "%.1f %%" % basol_precision_after_parcelle }} </p>
       <p> Précision des données après géocodage: {{ "%.1f %%" % basol_precision_after_geocodage }} </p>
    </td>
    <!-- SIS -->
    <td>  </td>
  </tr>
</table>
