
def row2dict(row):
    """ Convert an SQL Alchemy mapped instance into a dictionary """
    d = {}
    for column in row.__table__.columns:
        d[column.name] = getattr(row, column.name)
    return d


def merge_dtype(columns):
    """
    Ensure column names are unique in a list of columns
    This is useful when performing a join and the join key
    appears twice
    """
    seen = set()
    merged = [
        seen.add(column.name) or column
        for column in columns
        if column.name not in seen]
    return merged
