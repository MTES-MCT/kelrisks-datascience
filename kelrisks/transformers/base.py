
from ..models import get_models
from ..connections import get_database

models = get_models()


class PostgresPythonTransformer(object):
    """
    Base class for applying a transformation
    between two Postgres tables using Python
    """

    @property
    def input_model(self):
        return models[self.input_table]

    @property
    def output_model(self):
        return models[self.output_table]

    def select(self):
        return self.input_model.select().dicts()

    # def load(self, data):
    #     self.output_model.drop_table()
    #     self.output_model.create_table()
    #     instances = [self.output_model(**record) for record in data]
    #     self.output_model.bulk_create(instances, batch_size=100)

    def load(self, data):
        self.output_model.drop_table()
        self.output_model.create_table()
        for record in data:
            try:
                self.output_model.create(**record)
            except:
                print(record)

    def transform(self, data):
        raise NotImplementedError()

    def transform_load(self):
        data = self.select()
        transformed = self.transform(data)
        self.load(transformed)


class PostgresSQLTransformer(object):
    """
    Base class for applying a transformation
    between two Postgres tables using SQL
    """

    def transform_load(self):
        sql = self.query()
        db = get_database()
        db.execute_sql(sql)
