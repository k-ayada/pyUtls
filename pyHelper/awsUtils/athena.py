from __future__ import print_function # Python 2/3 compatibility
import time,uuid
#from  sts import STS  --> uncomment this line if you are running with main()

class Athena:
    def __init__(self, botoSession, tempS3: str, sse : dict ):
        self.__athena = botoSession.client('athena')
        self.__tempS3 = tempS3
        self.__sseTyp = 'SSE_S3'
        if sse :
            self.__sseTyp = sse['type']

        if self.__sseTyp != 'SSE_S3' :
            self.__KmsKey = sse['KmsKey']

    def query(self,sql_str: str, retainHeader: bool = True) -> [(tuple)] :

        queryId = str(uuid.uuid4())

        tempS3 = self.__tempS3 + queryId if self.__tempS3.endswith('/') \
                 else self.__tempS3 + '/' + queryId

        resConf = {'OutputLocation' : tempS3,
                   'EncryptionConfiguration' : {'EncryptionOption' : self.__sseTyp}}

        if self.__sseTyp != 'SSE_S3' :
            resConf['EncryptionConfiguration']['KmsKey'] = self.__KmsKey

        query_id = self.__athena.start_query_execution(
            QueryString         = sql_str,
            ClientRequestToken  = queryId,
            ResultConfiguration = resConf
            )['QueryExecutionId']

        query_status = None
        while query_status == 'QUEUED' or \
              query_status == 'RUNNING' or \
              query_status is None:
            query_status = self.__athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']
            if query_status['State']   == 'FAILED' :
                raise Exception('Athena query with the string "{}" failed'.format(sql_str))
            elif query_status['State'] ==  'CANCELLED':
                raise Exception('Athena query with the string "{}" was cancelled'.format(sql_str))
            time.sleep(10)


        results_iter = self.__athena.get_paginator('get_query_results').paginate(
            QueryExecutionId=query_id,
            PaginationConfig={
                'PageSize': 1000
            }
        )
        results = []
        data_list = []
        for results_page in results_iter:
            for row in results_page['ResultSet']['Rows']:
                data_list.append(row['Data'])
        for datum in data_list[0 if retainHeader else 1:]:
            results.append([x['VarCharValue'] for x in datum])
        return [tuple(x) for x in results]
'''
def main():

    boto = STS(roleArn ="arn:aws:iam::426625017959:role/Prod-BigDataETLProdSupp",
               useSAML=True).assume_role()
    athena = Athena(boto,
                    's3://prod-gwf-datalake-temp-us-east-1/ga_load/athena_res',
                    {"type" : "SSE_KMS",
                     "KmsKey" : "arn:aws:kms:us-east-1:426625017959:key/f826ca73-d3fa-4f91-be18-0b97ebde2f29",
                    })

    res = athena.query('select max(date) as max_dt from google_analytics_parquet.ga_data',True)

    print(res)

if __name__ == "__main__":
    main()
'''
