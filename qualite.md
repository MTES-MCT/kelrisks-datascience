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
    <td>
      <ul>
        <li>Ile de France: 26286</li>
        <li>Reste de la France: 48221</li>
      </ul>
    </td>
    <td>
      290768
    </td>
    <td>
      6998
    </td>
    <td>
      659
    </td>
    <td></td>
  </tr>
  <tr>
    <td>Données manquantes</td>
    <td> Environ 450 000 établissements en régime déclaratif </td>
    <td></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td>Géoréférencement</td>
    <td>
      <ul>
        <li> Ile-de-France
          <ul>
            <li>13027 coordonnées précises </li>
            <li>13259 centroïdes communes </li>
          </ul>
        </li>
        <li> Reste de la France
          <ul>
            <li>40140 coordonnées précises </li>
            <li>8081 centroïdes communes </li>
          </ul>
        </li>
      </ul>
    </td>
    <td>
      <ul>
        <li>228273 géoréférencés
          <ul>
            <li>dont 35979 avec une précision de type `rue`</li>
          </ul>
        </li>
        <li>62495 non géoréférencés </li>
      </ul>
    </td>
    <td>
      <ul>
        <li>
          100% des sites géolocalisés dont
          <ul>
            <li>2064 au numéro de rue</li>
            <li>993 à la rue</li>
            <li>1286 à la commune</li>
            <li>428 autre ?</li>
            <li>2227 dont la précision est inconnue</li>
          </ul>
        </li>
        <li>
          5058 références cadastrales
        </li>
      </ul>
    </td>
    <td>100% des sites géoréférencées</td>
  </tr>
  <tr>
    <td>Autres problèmes</td>
    <td></td>
    <td>
      Le shapefile ne comporte pas d'information de précision
      mais le dump sql si. Il semblerait que certains sites
      soient localisés au niveau du "centre" de la rue
    </td>
    <td>
      Information de précision non prise en compte
    </td>
    <td>
      Non intégré en prod ?
    </td>
  </tr>
  <tr>
    <td>Géocodage des adresses</td>
    <td>
      <ul>
        <li> Ile-de-France
          <ul>
            <li>5233 coordonnées de type `housenumber` retrouvées</li>
            <li>778 coordonnées de type `street` retrouvées </li>
            <li>Reste 7248 enregistrements géoréférencés à la commune (~ 27%)</li>
          </ul>
        </li>
        <li> Reste de la France
          <ul>
            <li> TODO </li>
          </ul>
        </li>
      </ul>
    </td>
    <td>
      <ul>
        <li>Pour l'Ile-de-France (2827 enregistrements non géoréférencés)</li>
        <ul>
          <li>21 coordonnées de type `housenumber` retrouvées</li>
          <li>39 coordonnées de type `street` retrouvées</li>
        </ul>
        <li>France Entière
          <ul>
            <li>TODO</li>
          </ul>
        </li>
    </td>
    <td>TODO</td>
    <td></td>
  </tr>
  <tr>
    <td>Données redressées en prod ?</td>
    <td> Table prête à être copiée en prod pour l'Ile-de-France </td>
    <td> pas encore </td>
    <td> pas encore </td>
    <td> pas encore </td>
  </tr>
</table>
