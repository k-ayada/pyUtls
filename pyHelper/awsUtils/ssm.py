from __future__ import print_function # Python 2/3 compatibility
import time,uuid
#from  sts import STS    # --> uncomment this line if you are running with main()


class SSM:
    def __init__(self, botoSession):
        self.__ssm = botoSession.client('ssm')


    def get_parm(self,names) :
        if isinstance(names,list) :
            res = self.__ssm.get_parameters(Names=names, WithDecryption=True)
        else:
            res = self.__ssm.get_parameters(Names= [names], WithDecryption=True)

        r = {}

        for x in res['Parameters']:
            r [x['Name'] ] = {'Type' : x['Type'], 'Value': x['Value'] }

        return r


'''
def main():

    boto = STS(roleArn ="arn:aws:iam::968074321407:role/Dev-BigDataPowerDeveloper",
               useSAML=True).assume_role()
    ssm = SSM(boto)

    res = ssm.get_parm(["/datalake/ga/incremental/bigqueryCredentials"])

    print(res)

if __name__ == "__main__":
    main()
'''
