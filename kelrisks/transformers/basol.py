
from .base import PostgresPythonTransformer


class CreateGeometryTransformer(PostgresPythonTransformer):

    def transform(self):
        """ create geometry fields from x, y coordinates """
        pass