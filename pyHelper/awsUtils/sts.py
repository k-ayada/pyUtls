import os,sys, time,requests, re, getpass,base64
from boto3 import client as botoClient
from boto3 import Session as botoSession
from botocore.session import get_session
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from xml.etree import ElementTree as ET
from botocore.credentials import RefreshableCredentials
try:
    import configParser
except:
    from six.moves import configparser

class STS:
    def __init__(self, region=None, roleArn=None, duration=3600,
                 user=None, pwd=None, authType=None, idpentryurl=None):
        self.__region                = region or "us-east-1"
        self.__duration              = duration
        self.__authType              = authType or 'InstanceProfile'
        if roleArn != None:
            self.__roleArn = roleArn
        elif self.__authType == 'InstanceProfile' :
            self.__roleArn = botoSession(region_name=self.__region)

        self.__payload               = {}
        self.__credentials           = {}
        self.__counter               = 0
        self.__init_time             = time.strftime('%Y%m%d%H%M%S')
        self.__idpentryurl           = idpentryurl or 'https://adfs.gwl.com/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=urn:amazon:webservices'
        self.__idpauthformsubmiturl  = ''
        self.__principalArn          = ''
        self.__user                  = user
        self.__pwd                   = pwd
        self.__assertion             = ''
        self.__params                = {"RoleArn": self.__roleArn,"DurationSeconds": self.__duration}
        self.__useSAML               = True if authType == 'SAML' else False
        self.__awsconfigfile         = os.path.expanduser("~") + '/.aws/credentials' if self.__useSAML else ''
        self.__req_session = requests.Session() # Initiate session handler
        self.__stsClient             = botoClient("sts", region_name=self.__region)

        if self.__useSAML :
            if user == None:
                print("\nUsername (domain\\userId):", end=''),
                self.__user = input()
                self.__pwd = getpass.getpass()
                print("")
            if self.__pwd == None:
                print("\nPlease enter the credentials for the user {}".format(self.__user))
                self.__pwd = getpass.getpass()
                print("")

    def __call__(self, assume=True,region=None):
        self.assume_role()

    def __setIDPAuthUrlAndPayload__(self):
        ''' Using the base URL, derive the final IDP Authorization Submit URL'''
        # Programmatically get the SAML assertion
        formresponse = self.__req_session.get(self.__idpentryurl, verify=True)
        self.__idpauthformsubmiturl = formresponse.url    #final url after all the 302s

        # Parse the response and extract all the necessary values
        # in order to build a dictionary of all of the form values the IdP expects
        formsoup = BeautifulSoup(formresponse.text, "lxml")
        self.__payload = {}
        for inputtag in formsoup.find_all(re.compile('(INPUT|input)')):
            name = inputtag.get('name','')
            value = inputtag.get('value','')
            if "user" in name.lower():
                self.__payload[name] = self.__user #Make an educated guess that this is the right field for the username
            elif "email" in name.lower():
                self.__payload[name] = self.__user #Some IdPs also label the username field as 'email'
            elif "pass" in name.lower():  #Make an educated guess that this is the right field for the password
                self.__payload[name] = self.__pwd
            else:
                self.__payload[name] = value #Simply populate the parameter with the existing value (picks up hidden fields in the login form)

        # Some IdPs don't explicitly set a form action, but if one is set we should
        # build the idpauthformsubmiturl by combining the scheme and hostname
        # from the entry url with the form action target
        # If the action tag doesn't exist, we just stick with the
        # idpauthformsubmiturl above
        for inputtag in formsoup.find_all(re.compile('(FORM|form)')):
            action = inputtag.get('action')
            loginid = inputtag.get('id')
            if (action and loginid == "loginForm"):
                parsedurl = urlparse(self.__idpentryurl)
                self.__idpauthformsubmiturl = parsedurl.scheme + "://" + parsedurl.netloc + action

    def __setSAMLAssertion__(self):
        # Performs the submission of the IdP login form with the above post data
        response = self.__req_session.post(
            self.__idpauthformsubmiturl, data=self.__payload, verify=True)

        # Decode the response and extract the SAML assertion
        soup = BeautifulSoup(response.text, "lxml")

        # Look for the SAMLResponse attribute of the input tag (determined by
        # analyzing the debug print lines above)
        for inputtag in soup.find_all('input'):
            if(inputtag.get('name') == 'SAMLResponse'):
                self.__assertion = inputtag.get('value')

        # Better error handling is required for production use.
        if (self.__assertion == ''):
            #TODO: Insert valid error checking/handling
            print('Response did not contain a valid SAML assertion')
            sys.exit(0)

    def __decideThePrincipalArn__(self):
        ''' Parse the input assertion and extract the PrincipalArn for the roleArn '''
        awsroles = []
        root = ET.fromstring(base64.b64decode(self.__assertion))
        for saml2attribute in root.iter('{urn:oasis:names:tc:SAML:2.0:assertion}Attribute'):
            if (saml2attribute.get('Name') == 'https://aws.amazon.com/SAML/Attributes/Role'):
                for saml2attributevalue in saml2attribute.iter('{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue'):
                    txt = saml2attributevalue.text
                    awsroles.append(txt)
                    if self.__roleArn in txt:
                        chunks = txt.split(',')
                        self.__principalArn = chunks[0] if 'saml-provider' in chunks[0] else chunks[1]

        if self.__principalArn == '':
            #TODO: Insert valid error checking/handling
            print('Not able to find the RoleArn entered in the authorised list of RoleArns')
            print("RoleArn Requestead {}".format(self.__roleArn))
            print("List of Authorised roles,")
            i = 1
            for r in awsroles:
                chunks = r.text.split(',')
                if 'saml-provider' in  chunks[0]:
                    print("\t{} - {} - {}".format(i,chunks[1],chunks[0]))
                else:
                    print("\t{} - {} - {}".format(i,chunks[0],chunks[1]))
                i += 1
            sys.exit(0)

    def __storeSAMLToken__(self):
        ''' Write the AWS STS token into the AWS credential file '''
        # Read in the existing config file
        config = configparser.RawConfigParser()
        config.read(self.__awsconfigfile)
        # Put the credentials into a saml specific section instead of clobbering
        # the default credentials
        if not config.has_section('saml'):
            config.add_section('saml')

        config.set('saml', 'output', 'json')
        config.set('saml', 'region', self.__region )
        config.set('saml', 'aws_access_key_id', self.__credentials["access_key"])
        config.set('saml', 'aws_secret_access_key',  self.__credentials["secret_key"])
        config.set('saml', 'aws_session_token', self.__credentials["token"])

        # Write the updated config file
        with open(self.__awsconfigfile, 'w+') as configfile:
            config.write(configfile)

        configfile.close

    def __refreshCred__(self):
        ''' Refresh the token by calling assume_role '''

        if self.__useSAML==True:
            self.__params["SAMLAssertion"] = self.__assertion
            self.__params["PrincipalArn"]  = self.__principalArn
            creds = self.__stsClient.assume_role_with_saml(**self.__params)["Credentials"]
        else:
            roleSessionName = "{}_{}_{}".format(self.__user,self.__init_time, self.__counter)
            self.__params["RoleSessionName"] = roleSessionName
            creds = self.__stsClient.assume_role(**self.__params)["Credentials"]

        self.__credentials = {
                "access_key": creds["AccessKeyId"],
                "secret_key": creds["SecretAccessKey"],
                "token": creds["SessionToken"],
                "expiry_time": creds["Expiration"].isoformat(),
            }
        if self.__useSAML == True:
            self.__storeSAMLToken__()

        return self.__credentials

    def assume_role(self):
        ''' Assume the role. Uses SAML if requested. '''
        if self.__authType == 'InstanceProfile' :
            return botoSession(region_name=self.__region)
        elif self.__useSAML == True:
            self.__setIDPAuthUrlAndPayload__()
            self.__setSAMLAssertion__()
            self.__decideThePrincipalArn__()
            session_credentials = RefreshableCredentials.create_from_metadata(
                                    metadata      = self.__refreshCred__(),
                                    refresh_using = self.__refreshCred__,
                                    method        = 'sts-assume-role-with-saml'
                                )
        else:
            session_credentials = RefreshableCredentials.create_from_metadata(
                                    metadata      = self.__refreshCred__(),
                                    refresh_using = self.__refreshCred__,
                                    method        = 'sts-assume-role'
                                )
        s = get_session()
        s._credentials = session_credentials
        if self.__region is None:
            self.__region = s.get_config_variable('region')
        s.set_config_variable('region', self.__region)
        return botoSession(botocore_session=s)

'''
def main():
#sts = AwsSTS(roleArn ="arn:aws:iam::426625017959:role/Prod-BigDataETLProdSupp", useSAML=True)
    botoSession = STS(roleArn ="arn:aws:iam::426625017959:role/Prod-BigDataETLProdSupp", useSAML=True).assume_role()
    client = botoSession.client('glue',region_name='us-east-1')
    responseGetDatabases = client.get_databases()
    databaseList = responseGetDatabases['DatabaseList']
    for databaseDict in databaseList:
        databaseName = databaseDict['Name']
        responseGetTables = client.get_tables( DatabaseName = databaseName )
        tableList = responseGetTables['TableList']
        for tableDict in tableList:
            tableName = tableDict['Name']
            print(databaseName + ',' + tableName)

if __name__ == "__main__":
    main()
'''
