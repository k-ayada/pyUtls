from __future__ import print_function
import psycopg2
from sys import exc_info
from .sts import STS

class Redshift:

    def __init__(self, boto, region,clusterId, dbName,dbUser):
        self.__boto          = boto
        self.__region        = region or 'us-east1'
        self.__clusterId     = clusterId
        self.__dbName        = dbName
        self.__dbUser        = dbUser
        self.__rsClient      = self.__boto.client('redshift', region_name=self.__region)
        self.__rsConn        = None
        self.__rsCurr        = None
        self.__host          = ''
        self.__port          = ''
        self.__iamUsr        = ''
        self.__iamPwd        = ''

        self.__continue()

    def __del__(self):
        self.close()

    def close(self):
        ''' Close the cursor and the connection '''
        if self.__rsConn:
           self.__rsCurr.close()
           self.__rsConn.close()

    def __continue(self):
        ''' Initialize the class variables '''
        desc = self.__rsClient.describe_clusters(ClusterIdentifier=self.__clusterId)
        self.__host   = desc['Clusters'][0]['Endpoint']['Address']
        self.__port   = desc['Clusters'][0]['Endpoint']['Port']

        cred = self.__rsClient.get_cluster_credentials(
                ClusterIdentifier= self.__clusterId,
                DbName           = self.__dbName,
                DbUser           = self.__dbUser,
                DurationSeconds=3600)
        self.__iamUsr = cred['DbUser']
        self.__iamPwd = cred['DbPassword']

    def connect(self):
        ''' Connect to the Redshift cluster  '''
        if self.__rsConn == None:
            try:
                self.__rsConn = psycopg2.connect(
                    dbname   = self.__dbName,
                    host     = self.__host,
                    port     = self.__port,
                    user     = self.__iamUsr,
                    password = self.__iamPwd)
                self.__rsCurr = self.__rsConn.cursor()
            except (Exception, psycopg2.Error) as error :
                print("Failed to connect to RS. Error : ", error)
                raise

            self.__rsConn
        else :
            print("self.__rsConn  is already initialized")

    def runSQL(self,sql):
        ''' execute a select query and return the results as list'''
        try:
            if self.__rsCurr != None :
                self.__rsCurr = self.__rsConn.cursor()

            self.__rsCurr.execute(sql)
        except (Exception, psycopg2.Error) as error :
            print ("Error while fetching data from RS", error)
            if self.__rsCurr:
                self.__rsCurr.close()

        if self.__rsCurr:
            res = self.__rsCurr.fetchall()
        else:
            res = None

        self.__rsCurr.close()
        return res
'''
def main():

    boto = STS(roleArn ="arn:aws:iam::426625017959:role/Prod-BigDataETLProdSupp",
                  useSAML=True).assume_role()
    rs   = Redshift(boto,boto.region_name,"redshift-dl-prod", "p_dl", "admin_user")
    rs.connect()
    res = rs.runSQL("select sk_plan as res from prod_dw.plan_dim where deleted_ind = 'N'")
    # fetch results from the query
    for row in res :
        print(row[0])

    res = rs.runSQL("select distinct sk_plan as res from prod_dw.participant_dim where deleted_ind = 'N'")
    # fetch results from the query
    for row in res :
        print(row[0])

    # close the Amazon Redshift connection
    rs.close()

if __name__ == "__main__":
    main()
'''
