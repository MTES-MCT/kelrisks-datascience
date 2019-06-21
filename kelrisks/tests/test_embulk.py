
import os
from unittest import mock

from .helpers import BaseTestCase

from ..embulk import load_s3ic
from ..models import S3IC_source
from ..connections import test_db_name

from ..config import ROOT_DIR


class EmbulkTestCase(BaseTestCase):

    @mock.patch('kelrisks.embulk.POSTGRES_DB', test_db_name)
    @mock.patch('kelrisks.embulk.embulk_input_path')
    def test_load_s3ic(self, mock_input_path):
        """ it should load s3ic data to table s3ic_source """
        mock_input_path.return_value = os.path.join(
            ROOT_DIR, 'kelrisks', 'tests', 'files', 's3ic.csv')
        load_s3ic()
        count = S3IC_source.select().count()
        self.assertEqual(count, 9)
