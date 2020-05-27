"""try me"""

import csv
import os
import gzip
import google

from google.cloud import storage
from google.cloud import bigquery
from enum import Enum
import tempfile

class MThTable():
    
    pass



def get_coltype(column_descriptor: str) -> type:
    if('char' in column_descriptor):
        return str
    elif('int' in column_descriptor):
        return int
    elif('numeric' in column_descriptor):
        return float
    else:
        return str
    
class Mth(object):
    """Sits over a metathesaurus directory to access contents"""
    
    def __init__(self, path: str):
        """Sits over a metathesaurus directory to access contents"""
        
        self.path = path
        self._tables = {}
        cols = {}
        with open(os.path.join(path, 'MRCOLS.RRF')) as csvfile:
            reader = csv.reader(csvfile, delimiter="|")
            for row in reader:
                row.append(get_coltype(row[7]))
                cols[row[0]] = row
        self.columns = cols
        with open(os.path.join(path, 'MRFILES.RRF')) as csvfile:
            reader = csv.reader(csvfile,delimiter='|')
            for row in reader:
                row[2] = row[2].split(',')
                self._tables[row[0]] = row

    @property
    def tables(self) -> dict:
        return self._tables

    def table_columns(self, tblname: str):
        colnames = self.tables[tblname][2] # TODO: set up class for table records
        ret = []
        for i in colnames:
            ret.append(self.columns[i])
        return ret
    
    def iterate_table_rows(self, tblname: str):
        # list of column names for the given table
        cols = self.tables[tblname][2] # TODO: set up class for table records
        fpath = os.path.join(self.path, tblname)
        if(os.path.exists(fpath)):
            csvfile = open(fpath,'rt')
        if(os.path.exists(fpath+'.gz')):
            csvfile = gzip.open(fpath+'.gz', 'rt')
        reader = csv.reader(csvfile, delimiter = '|', quoting=csv.QUOTE_NONE)
        ret = []
        number_of_cols = len(self.table_columns(tblname))
        for row in reader:
            if(len(row)<=number_of_cols):
                row = row + ''*(number_of_cols-len(row)+1)
            yield(row[:-1])
        csvfile.close()

    def write_table(self, tblname: str, fobj):
        writer = csv.writer(fobj)
        writer.writerows(self.iterate_table_rows(tblname))

    def bigquery_schema_for_table(self, tblname: str):
        schema = []
        for col in self.table_columns(tblname):
            if(col[9]==str):
                schema.append(bigquery.SchemaField(name = col[0], field_type='STRING',description = col[1]))
            if(col[9]==int):
                schema.append(bigquery.SchemaField(name = col[0], field_type='INTEGER',description = col[1]))
            if(col[9]==float):
                schema.append(bigquery.SchemaField(name = col[0], field_type='FLOAT',description = col[1]))
        return(schema)
        
def upload_csv(mth: Mth, tblname: str):
    project = 'isb-cgc-01-0006'
    dataset_id = 'omicidx_etl'
    bucket_name = 'temp-testing'
    storage_client = storage.Client(project=project)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"umls/{tblname}")
    f = tempfile.NamedTemporaryFile(mode='wt')
    mth.write_table(tblname,f)
    blob.upload_from_filename(f.name)
    uri = f'gs://{bucket_name}/{blob.name}'
    
    bq_client = bigquery.Client(project=project)
    dataset_ref = bq_client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = mth.bigquery_schema_for_table(tblname)
    job_config.skip_leading_rows = 0
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV

    tblname = tblname.replace('.','_')
    
    load_job = bq_client.load_table_from_uri(
        uri, dataset_ref.table(tblname), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = bq_client.get_table(dataset_ref.table(tblname))
    print("Loaded {} rows.".format(destination_table.num_rows))
