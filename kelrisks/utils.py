
from io import StringIO
import csv


def spreadsheet2array(spreadsheet, offset=0):
    """ Convert an Excel table to a Python array """
    headers = spreadsheet.row_values(offset)
    rows = []
    for r in range(offset + 1, spreadsheet.nrows):
        row = spreadsheet.row_values(r)
        rows.append(dict(zip(headers, row)))
    return rows


def csv2dicts(filelike, **kwargs):
    """ transform a csv-like text to an array of dicts records """
    reader = csv.DictReader(filelike, **kwargs)
    return list([dict(row) for row in reader])


def dicts2csv(dicts, **kwargs):
    """ transform an array of dicts to a csv-like text """
    filelike = StringIO()
    fieldnames = list(dicts[0].keys())
    writer = csv.DictWriter(filelike, fieldnames=fieldnames, **kwargs)
    writer.writeheader()
    for record in dicts:
        writer.writerow(record)
    filelike.seek(0)
    return filelike


def is_float(s):
    """ check if a string can be represented as a number """
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def chunks(l, n):
    """ Yield successive n-sized chunks from l. """
    for i in range(0, len(l), n):
        yield l[i:i + n]