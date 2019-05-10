from __future__ import print_function
from boto3 import Session  as bototSession
from boto3 import resource
from botocore.client import Config
import time

from s3fs import S3FileSystem
import pyarrow         as pa
import pyarrow.parquet as pq
from pyarrow.compat import guid
from logging import RootLogger

class S3:
    def __init__(self,log : RootLogger,boto: bototSession):
        self.log     = log
        self.__s3    = boto.client('s3',config=Config(signature_version='s3v4'))
        self.__s3Res = boto.resource('s3')
        self._s3fs = S3FileSystem()

    def getBucketNKeyTuple(self,uriStr: str) -> (str,str)  :
        splt = uriStr.replace("s3://","").split("/")
        bkt = splt.pop(0)
        key = "/".join(splt)
        return (bkt,key)

    def isFolderPresent(self,uri: str) -> bool:
        (bkt,key) = self.getBucketNKeyTuple(uri)
        self.log.debug("bkt:%s - key:%s" % (bkt,key))
        objs = list(self.__s3Res.Bucket(bkt).objects.filter(Prefix=key))
        return len(objs) > 1

    def isFilePresent(self,uri: str) -> bool:
        '''Returns T/F whether the file exists.'''
        (bkt,key) = self.getBucketNKeyTuple(uri)
        objs = list(self.__s3Res.Bucket(bkt).objects.filter(Prefix=key))
        return len(objs) == 1 and objs[0].key == key


    def deleteObject(self, s3UriStr: str, recursive: bool = False) :
        (bkt,key) = self.getBucketNKeyTuple(s3UriStr)

        if recursive  and  not key.endswith("/")  :
            key = "%s/" % (key)
        elif (not recursive) and key.endswith("/") :
            key = key[:-1]

        keyLst = self.__s3Res.Bucket(bkt).objects.filter(Prefix="%s" % (key))
        for i,k in enumerate(keyLst):
            self.log.debug("%3d Deleting s3 Object: %s" % (i,k))
            self.__s3.delete_object(Bucket=k.bucket_name,Key=k.key)

    def mkdir(self,path):
        if not self._s3fs.exists(path):
            try:
               self._s3fs.mkdir(path)
               self.deleteObject("%s/" % path)

            except OSError:
                assert self._s3fs.exists(path)


    def waitForFile(self,uri:str, maxReTry : int = 10, sleepFor : int = 10) -> bool:
        '''
        Wait for the file (full s3uri).
        parms:
            uri     : Path to the s3 file. Needs full uri
            maxReTry: Max time we should check for the file before we quit (default 10)
            sleepFor: Number of seconds for which we need to wait between each re-try (default 10 secs).

        Return,
            Found : True if file found else False.
        '''
        cnt = -1
        found  = False
        self.log.info("Waiting for the file '%s'." % (uri))
        while found == False and cnt < maxReTry :
            cnt += 1
            time.sleep(sleepFor)
            if self.isFilePresent(uri) :
                found = True
            else:
                cnt += 1
                self.log.DEBUG("Still waiting for the file.")
        self.log.info("File found.")

    def df2parquet(self,pandasDF, bucket:str,folder:str, file:str, overwrite:bool = False,
                     engine           : str  = 'auto',
                     compression      : str  = 'snappy',
                     use_dictionary   : bool = False,
                     coerce_timestamps: str  = 'ms',
                     partition_cols   : list = None,
                     row_group_size   : int  = None,
                     **kwargs
                    ):
        s3Path =      "s3://%s/%s/%s" % (bucket, folder, file) if folder != None \
                 else "s3://%s/%s" % (bucket, file)

        pq.write_to_dataset(table                     = pa.Table.from_pandas(pandasDF),
                            root_path                 = s3Path,
                            partition_cols            = partition_cols,
                            filesystem                = self._s3fs,
                            preserve_index            = False,
                            compression               = compression,
                            flavor                    = 'spark',           #Enable Spark compatibility
                            coerce_timestamps         = coerce_timestamps, #Limit the timestamp to miliseconds
                            allow_truncated_timestamps=True,               #Don't raise exception during truncation
                            use_dictionary            = use_dictionary,
                            version                   = '2.0'
        )


    def pandas2Parquet(self,pandasDF, bucket:str,folder:str, file:str, overwrite:bool = False,
                     engine           : str  = 'auto',
                     compression      : str  = 'snappy',
                     use_dictionary   : bool = False,
                     coerce_timestamps: str  = 'ms',
                     partition_cols   : list = None,
                     row_group_size   : int  = None,
                     **kwargs
                    ):
        s3Path =      "s3://%s/%s/%s" % (bucket, folder, file) if folder != None \
                 else "s3://%s/%s" % (bucket, file)
        self.log.info("Writing the Pandas DF to S3 path %s" % (s3Path))

        if overwrite and self.isFolderPresent(s3Path):
            self.deleteObject(s3Path)

        if partition_cols is not None and len(partition_cols) > 0:
            part_keys = [pandasDF[col] for col in partition_cols]
            data_cols = pandasDF.columns.drop(partition_cols)
            if len(data_cols) == 0:
                raise ValueError('No data left to save outside partition columns')
            table = pa.Table.from_pandas(pandasDF)
            subschema = table.schema
            for col in table.schema.names:
                if (col.startswith('__index_level_') or col in partition_cols):
                    subschema = subschema.remove(subschema.get_field_index(col))

            for keys, subgroup in pandasDF.groupby(part_keys):
                if not isinstance(keys, tuple):
                    keys = (keys,)

                subdir = '/'.join(
                    ['{colname}={value}'.format(colname=name, value=val)
                    for name, val in zip(partition_cols, keys)])

                subtable = pa.Table.from_pandas(df            = subgroup,
                                                schema        = subschema,
                                                preserve_index= False,
                                                safe          = False,
                                                nthreads      = 5)
                prefix = '/'.join([s3Path, subdir])

                if (not overwrite) and self.isFolderPresent(prefix):
                    self.deleteObject(prefix)

                outfile   = "pyarow-%s.%s.parquet" % (guid() ,compression)
                full_path = '/'.join([prefix, outfile])
                self.log.debug("Creating the file: %s" % (full_path))
                self.mkdir(prefix)
                with self._s3fs.open(full_path, 'wb') as f:
                    pq.write_table(table                     = subtable,
                                   where                     = f,
                                   compression               = compression,
                                   flavor                    = 'spark',           #Enable Spark compatibility
                                   coerce_timestamps         = coerce_timestamps, #Limit the timestamp to miliseconds
                                   allow_truncated_timestamps= True,              #Don't raise exception during truncation
                                   use_dictionary            = use_dictionary,
                                   row_group_size            = row_group_size,
                                   version                   = '2.0',
                                   **kwargs)
        else:
            outfile   = "pyarow-single-%s.%s.parquet" % ( guid() ,compression)
            full_path = '/'.join([prefix, outfile])
            self.log.debug("Creating the file: %s" % (full_path))
            with self._s3fs.open(full_path, 'wb') as f:
                pq.write_table(table                     = pa.Table.from_pandas(df = pandasDF,
                                                                                preserve_index=False,
                                                                                nthreads = 5
                                                                                ),
                                where                     =  f,
                                compression               = compression,
                                flavor                    = 'spark',           #Enable Spark compatibility
                                coerce_timestamps         = coerce_timestamps, #Limit the timestamp to miliseconds
                                allow_truncated_timestamps= True,              #Don't raise exception during truncation
                                use_dictionary            = use_dictionary,
                                row_group_size            = row_group_size,
                                version                   = '2.0',
                                **kwargs)

        #df.to_parquet(fname          = s3Path,
        #              compression    = compression,
        #              partition_cols = partition_cols
        #            )
        #pq.write_table(table            = pa.Table.from_pandas(df),
        #               where            = s3Path,
        #               filesystem       = self._s3fs,
        #               compression      = compression,
        #               flavor           ='spark',        #Enable Spark compatibility
        #               coerce_timestamps='ms',          #Limit the timestamp to miliseconds
        #               allow_truncated_timestamps=True  #Don't raise exception during truncation
        #               )
