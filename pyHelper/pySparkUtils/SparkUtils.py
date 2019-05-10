from __future__ import print_function
import sys, re, logging
from typing                import List
from logging               import RootLogger
from boto3                 import Session as bototSession

from pyspark               import SparkConf, SparkContext
from pyspark.sql           import SparkSession, SQLContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types     import StructType

from pyspark.storagelevel  import StorageLevel

from pyHelper.awsUtils.s3  import S3
class SparkUtils:
    def __init__(self,log        : RootLogger   = None,
                      parms      : dict         = None,
                      botoSession: bototSession = None,
                      appName    : str          = None
                ):
        self.log         = log
        self.__parms     = parms or {}
        self.__runEnv    = self.__parms.get("--runEnv", "local")
        if (self.__runEnv == "aws"):
            self.__boto      = botoSession
            self.__s3        = S3(log,self.__boto)

        self.__initFlags()
        self.__setupSparkSession__(appName)

        self.__dfltRDDParts = \
                int(self.__spark.conf.get("spark.executor.instances", "20")) * \
                int(self.__spark.conf.get("spark.executor.cores", "4")) * 2


    def __initFlags(self):
        '''
        Init the job level parameters needed by this class
        '''
        self.__parms["--saveDFAs"] = self.__parms.get("--saveDFAs", "NONE")

        self.__explainDF  = True if "-explainDF"  in self.__parms else False
        self.__printcount = True if "-printCount" in self.__parms else False
        self.__useHist    = True if "-useHint"    in self.__parms else False
        self.__saveDF     = True if self.__parms["--saveDFAs"] != "NONE" else False

        self.__fileFmt    = self.__parms.get("--fileFormat","parquet")

        if (self.__runEnv == "aws"):
            self.__tempS3     = self.__parms.get("--tempS3","hdfs:///temp/s3")
        if (self.__runEnv != "local" ):
            self.__tempHDFS   = self.__parms.get("--tempHDFS","hdfs:///temp")
            self.log.warn("For persist type 'S3', 'HDFS' will be used as the --runEnv != 'aws'")

    def __setupSparkSession__(self,appName : str = None):
        '''
        Init the Spark environemnt with few default configurations and start the spark session.
        '''
        self.__conf = SparkConf()
        hmConf = {
            "spark.rps.askTimeout"             : "1200",
            "spark.network.timeout"            :  "1200",
            "spark.broadcast.blockSize"        : "16m",
            "spark.sql.broadcastTimeout"       : "1200",
            "spark.broadcast.compress"         : "true",
            "spark.rdd.compress"               : "true",
            "fs.s3.enableServerSideEncryption" : "true",
            "spark.kryo.unsafe"                : "false",
            "spark.kryoserializer.buffer"      :"10240",
            "spark.kryoserializer.buffer.max"  :"2040m",
            "spark.io.compression.codec"       : "org.apache.spark.io.SnappyCompressionCodec",
            "spark.serializer"                 : "org.apache.spark.serializer.KryoSerializer",
            "mapreduce.fileoutputcommitter.algorithm.version"              :"2",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" :"2",

        }
        self.__conf.setAll(hmConf)
        SparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        SparkContext.setSystemProperty("com.amazonaws.services.s3.enforceV4", "true")
        self.__spark = SparkSession \
                        .builder \
                        .config(conf=self.__conf) \
                        .appName(appName or "PySparkApp") \
                        .enableHiveSupport() \
                        .getOrCreate()
        self.__sc = self.__spark.sparkContext
        self.sqlC = SQLContext(self.__sc)
        self.__sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        self.__sc.setSystemProperty("com.amazonaws.services.s3.enforceV4", "true")
        self.__sc.setLogLevel(self.__parms.get("--logLevel", "INFO"))

        hdpCnf = self.__sc.hadoopConfiguration
        hdpCnf.setAll({
                "io.file.buffer.size"                             : "65536",
                "mapreduce.fileoutputcommitter.algorithm.version" : "2",
                "fs.s3a.endpoint"                                 : "%s.amazonaws.com" % ( self.__parms.get("--awsRegion",'s3.us-east-1' ))
            })
        if (self.__parms.get("--runEnv", "AWS") == "AWS"):
            from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher
            provider = InstanceMetadataProvider(iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2))
            creds = provider.load()
            hdpCnf.setAll({
                "fs.s3a.access.key"                       : creds.access_key,
                "fs.s3a.secret.key"                       : creds.secret_key,
                "fs.s3a.server-side-encryption-algorithm" : "SSE-KMS",
                "fs.s3.enableServerSideEncryption"        : "true",
                "fs.s3.impl"                              : "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.impl"                             : "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.endpoint"                         : "s3.%s.amazonaws.com" % (self.__parms.get("--awsRegion", "us-east-1"))
            })

    def sql(self, dfName : str,
                  query : str,
                  partitions : int =0,
                  persistType : str = None
           ):
        '''
        Runs the inpult SQL, partitions the resulting Dataframe and persists the Dataframe if needed.

        Supported persistType: In addition to the pySpark native persist types, this function supports
        HIVE, HDFS, S3

        '''
        if persistType == None :
            _df = self.__spark.sql(self.handleHints(query))
            if partitions == 0 :
                df = _df
            elif df.rdd.getNumPartitions < partitions :
                df = _df.repartition(partitions)
            else:
                df = _df.coalesce(partitions)
            return df
        else:
            df = self.storeDF(
                df            = self.sql(query,partitions),
                dfName        = dfName,
                persistType   = persistType,
                partitions    = partitions,
                partitionCols = self.getPartitionColumnsFromSQL(query)
            )

        if dfName:
            df.createOrReplaceTempView(dfName)

        if self.__printcount:
            self.log.info("Number of Records in DF '%s' : %d " % (dfName,df.count()))

    def storeDF(self, df            : DataFrame,
                      dfName        : str,
                      persistType   : str,
                      partitions    : int,
                      partitionCols : List[str]
               ):
        '''
        Store the input dataframe, read the persisted datafrme and return the new one.
        If Memory/Disk persistance requested, we run take(1) on the datafrme to force persist.
        '''
        if self.__explainDF or \
            "NULL|NONE".index(persistType.toUpperCase()) < 0 :
            self.log.info("Execution pland for building the DF '%s'" % (dfName))
            df.explain()
            self.log.info("\n\n\n")

        saveType = self.__parms["--saveDFAs"] \
            if self.__saveDF and \
               "HIVE|NULL".index(persistType.toUpperCase()) < 0 \
            else \
                persistType.toUpperCase()

        if saveType == "S3" and self.__runEnv == "aws":
            saveType = "HDFS"
            self.log.debug("Resetting the persist type to 'HDFS' as the --runEnv != 'aws'")

        df1 = df if saveType != "HDFS" and \
                    saveType != "HIVE" and \
                    saveType != "S3" \
                 else self.repartitionDF(dataFrame= df, partitions = partitions)

        if  saveType == "NULL" or saveType == "NONE":
            return df1
        elif saveType == "HDFS":
            return self.persistExternal( self.__tempHDFS, dfName, df, partitionCols)
        elif saveType == "S3":
            return self.persistExternal( self.__tempS3, dfName, df, partitionCols)
        elif saveType == "":
            return self.persist2Hive(dfName,df,partitionCols)
        elif saveType == "CHECK_POINT":
            return df.cache().checkpoint(eager=True)
        else:
            return self.persistLocal(dfName, df, persistType)

    def persistExternal(self, parentDirURI  : str,
                              fileName      : str,
                              df            : DataFrame,
                              partitionCols : List[str] = None,
                              overwrite     : bool      = True,
                              fileFormat    : str       = None, **kwargs):

        fullPath = "%s%s"  % (parentDirURI,fileName or "") if parentDirURI.endswith("/") else \
                   "%s/%s" % (parentDirURI,fileName or "")
        fullPath = fullPath.replace("//", "/")
        schma = df.schema()
        fileFormat    = fileFormat or self.__fileFmt
        self.write2ExtrFile(fullPath      = fullPath,
                            fileFormat    = fileFormat,
                            df            = df,
                            partitionCols = partitionCols,
                            overwrite     = overwrite, **kwargs)
        df.unpersist()
        if fileFormat == "parquet" :
            return self.readParquet( uri = fullPath, schema = schma,  **kwargs)
        elif fileFormat == "orc" :
            return self.readOrc( uri = fullPath, schema = schma, **kwargs)
        elif fileFormat == "csv" :
            return self.readCSV( uri = fullPath, schema = schma, **kwargs)
        else :
            return self.readParquet( uri = fullPath, schema = schma, **kwargs)

    def readParquet(self,uriString : str,
                         schema:StructType = None,
                         mergeSchema: bool = False,**kwargs):
        self.log.info("Reading the parquet file '%s'" % uriString)
        rdr = self.__spark.read.format("parquet")
        if mergeSchema :
           rdr.option("mergeSchema", "true"  )
        if schema:
            rdr.schema(schema)
        return rdr.load(uriString)

    def readOrc(self,uriString : str, schema: StructType, **kwargs):
        self.log.info("Reading the ORC file in '%s'" % uriString)
        pass ##TODO

    def readCSV(self,uriString : str, schema: StructType, **kwargs):
        self.log.info("Reading the CSV file in '%s'" % uriString)
        pass ##TODO

    def write2ExtrFile(self,
                       fileFormat    : str,
                       fullPath      : str,
                       df            : DataFrame,
                       partitionCols : List[str] = None,
                       overwrite     : bool      = True, **kwargs ):

        if fullPath.startswith("s3") :
           self.__s3.waitForFile("%s/_SUCCESS" % (fullPath))

         #TODO:Yet to Implement




    def persist2Hive(self, table         : str,
                           df            : DataFrame,
                           partitionCols : List[str]
                    ):
        pass #TODO:Yet to Implement

    def persistLocal(self, dfName      : str,
                           df          : DataFrame,
                           persistType : str
                    ):
        ''' Persist the input Datafrmae locally (memory/disk/none) and runs `df.take(1)` to force persist.
        '''
        lvl = self.getSparkPersistType(persistType.toUpperCase())
        if lvl:
            df.persist()

        if (self.__printcount == None) :
            df.take(1)

    def getSparkPersistType(self, persistTypStr: str) -> StorageLevel :
        '''
            Converts the String representation to the StorageLevel Object.
            If invalid string received, it will return the `StorageLevel.NONE`
            Supported,
                `StorageLevel.NONE`
                `StorageLevel.DISK_ONLY`
                `StorageLevel.DISK_ONLY_2`
                `StorageLevel.MEMORY_ONLY`
                `StorageLevel.MEMORY_ONLY_2`
                `StorageLevel.MEMORY_AND_DISK`
                `StorageLevel.MEMORY_AND_DISK_2`
                `StorageLevel.OFF_HEAP`
        '''

        if   persistTypStr == "NONE"              : return None
        elif persistTypStr == "DISK_ONLY"         : return StorageLevel.DISK_ONLY
        elif persistTypStr == "DISK_ONLY_2"       : return StorageLevel.DISK_ONLY_2
        elif persistTypStr == "MEMORY_ONLY"       : return StorageLevel.MEMORY_ONLY
        elif persistTypStr == "MEMORY_ONLY_2"     : return StorageLevel.MEMORY_ONLY_2
        elif persistTypStr == "MEMORY_AND_DISK"   : return StorageLevel.MEMORY_AND_DISK
        elif persistTypStr == "MEMORY_AND_DISK_2" : return StorageLevel.MEMORY_AND_DISK_2
        elif persistTypStr == "OFF_HEAP"          : return StorageLevel.OFF_HEAP
        else:
            self.log.warn("Invalid Persist Type %s received. Defaulting to NONE" % (persistTypStr))
            return None

    def repartitionDF(self,dataFrame  : DataFrame,
                           partitions : int = 0):
        '''
            Repartition the inuput dataframe

            parms: df          -> dataframe
                   partitions  -> new partitions count. Defaulted to 0 i.e Don't partition

            logic,
                if partitions = 0 , Don't repartitions
                if partitions = -1, Repartions to the default number (NumOfExecutors * ExecutorCores * 2)
                if partitions > 0 , Repartition/coalesce to the input number
        '''
        curParts = dataFrame.rdd.getNumPartitions
        finalParts = min(curParts, partitions)

        if curParts == partitions or partitions == 0:
            finalParts = -1
        elif partitions == -1:
            finalParts = self.__dfltRDDParts
        elif partitions > 0:
            finalParts = partitions
        else :
            pass #finalParts is pre-populated.

        self.log.debug("Current Partitions: %d , Requested: %d,  Final: %d " % (curParts,partitions,finalParts) )

        if finalParts != -1:
            return  dataFrame
        elif curParts > finalParts:
            return dataFrame.coalesce(finalParts)
        else :
            return dataFrame.repartition(finalParts)


    def handleHints(self,query : str):
        '''
            Removes the SparkSQL hints if the -useHist parm is not set.

            Example:- If sql = 'select /* hists */ cols.. from ..'
               if -useHist is not set,
                  return 'select cols.. from ..'
               else
                  return 'select /* hists */ cols.. from ..'
        '''
        if self.__useHist:
            return query
        else:
            return  re.sub(r'/\*+.*\*/', '', query)

    @staticmethod
    def getPartitionColumnsFromSQL(query):
        s = query.toLowerCase().strip().replace("\n", " ")
        inx = s.index(" cluster ")
        lst = []
        if inx > 0:
            lst.extend((map(lambda x: x.strip(), s[inx + 12 :].split(","))))
        else :
            frm = s.index(" distribute ")
            to = s.index(" sort ", frm + 15) if frm > 0 else 0
            if to > frm:
                lst.extend((map(lambda x: x.strip(), s[frm + 15: to ].split(","))))
            else:
                lst.extend((map(lambda x: x.strip(), s[frm + 15:].split(","))))
        return lst
