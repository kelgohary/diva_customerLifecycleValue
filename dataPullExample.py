import pandas as pd
from google2pandas import *
from simple_salesforce import Salesforce

from sqlalchemy import create_engine

pd.set_option('display.max_rows', 10000)

#Set up Google Analytics connection and query using google2pandas library
conn = GoogleAnalyticsQuery(secrets='./ga-creds/client_secrets.json', token_file_name='./ga-creds/analytics.dat')

query = { \
    'ids': '110012641',
    'metrics': ['users','pageviews'],
    #'dimensions': ['dimension1', 'dimension2', 'userType', 'date', 'pagePath'],
    'dimensions': ['dimension1', 'userType', 'date'],
    'start_date': '2005-01-01',
    'max_results': 1000
}

ga_df, metadata = conn.execute_query(all_results=True, **query)

#Rename the custom dimensions (dimension1 : c_id, dimension2 : account_id)

#convert c_id from string to int
ga_df = ga_df.rename(columns={'dimension1':'c_id', 'dimension2': 'account_id'})

#Convert c_id from string to integer
ga_df['c_id'] = ga_df['c_id'].astype(int)

#Sort by c_id and date (should I convert the date type at some point??)
ga_df = ga_df.sort_values(['c_id','date'])

print('\n')

#Pull in SalesForce data
with open('./sf-creds/salesforce_login.rtf') as f:
    username, password, token = [x.strip("\n") for x in f.readlines()]
sf = Salesforce(username=username, password=password, security_token=token)

#Create a Leads Dataframe
leads = sf.query_all("""SELECT Id,
                 Status,
                 Lead_Created_Date__c,
                 Vertical__c,
                 Owner.Name
                 FROM Lead""")


lead_Id = [x['Id'] for x in leads["records"]]
statuses = [x['Status'] for x in leads["records"]]


leads_df = pd.DataFrame({'Lead_Id':lead_Id, 'Status': statuses})

leads_df = leads_df.sort_values('Lead_Id')


#Create an Accounts Dataframe
accounts = sf.query_all("""SELECT Id,
                    Name,
                    Account_Size__c,
                    status__c,
                    Vertical__c
                    FROM Account""")

account_Id = [x['Id'] for x in accounts["records"]]
account_Name = [x['Name'] for x in accounts["records"]]
#account_Number = [x['AccountNumber'] for x in accounts["records"]]
vertical = [x['Vertical__c'] for x in accounts["records"]]
account_size = [x['Account_Size__c'] for x in accounts["records"]]

accounts_df = pd.DataFrame({'Account_Id':account_Id, 'Account_Name':account_Name, 'Vertical':vertical,
                            'Account_Size':account_size})

accounts_df = accounts_df.sort_values('Account_Name')

#Create a Contacts Dataframe
contacts = sf.query_all("""SELECT Name, Account.Name, Account_ID__c, Sandbox_Key__c, c_ID__c
                    FROM Contact
                    WHERE Account.Name != null""")

contact_Name = [x['Name'] for x in contacts["records"]]
contact_Account_Name = [x['Account']['Name'] for x in contacts["records"]]
contact_Account_Id = [x['Account_ID__c'] for x in contacts["records"]]
c_id = [x['c_ID__c'] for x in contacts["records"]]

contacts_df = pd.DataFrame({'Contact_Name':contact_Name, 'Contact_Account_Name':contact_Account_Name,
                            'Account_Id':contact_Account_Id, 'C_Id':c_id})




#Convert c_id from string to integer
contacts_df['C_Id'] = pd.to_numeric(contacts_df['C_Id'])
    #.astype(str).astype(int)

contacts_df = contacts_df.sort_values('C_Id')


#Create an Opportunities Dataframe
opportunities = sf.query_all("""SELECT Name,
                    Id,
                    Account.Name,
                    Opp_Name__c
                    FROM Opportunity""")

opp_Id = [x['Id'] for x in opportunities["records"]]
opp_Name = [x['Name'] for x in opportunities["records"]]
opp_Account_Name = [x['Account']['Name'] for x in opportunities["records"]]
opp_NameCustom = [x['Opp_Name__c'] for x in opportunities["records"]]

opps_df = pd.DataFrame({'Opp_Id':opp_Id, 'Opp_Name':opp_Name, 'Opp_Account_Name':opp_Account_Name,
                        'Opp_NameCustom':opp_NameCustom})

opps_df = opps_df.sort_values('Opp_Account_Name')


#Create a TTV Dataframe
ttv = sf.query_all("""SELECT Opportunity__c,
               Age_Since_Contract_Signed__c,
               Age_Since_Launch__c,
               Age_Since_Lead_Created__c,
               Lead_Created_Date__c,
               Lead_To_Commit__c,
               Lead_to_Stage_2__c,
               Stage_2_to_Stage_3__c,
               Stage_3_to_Stage_4A__c,
               Stage_4A_to_Stage_4B__c,
               Stage_4B_to_Stage_4C__c,
               Stage_4C_to_Stage_5__c,
               Stage_4_to_Stage_5__c,
               TTV__c,
               Tick_Tock_Completed__c
               FROM TTV_Data__C""")

opportunity = [x['Opportunity__c'] for x in ttv["records"]]
duration_since_lead = [x['Age_Since_Lead_Created__c'] for x in ttv["records"]]
duration_ttv = [x['TTV__c'] for x in ttv["records"]]
duration_since_launch = [x['Age_Since_Launch__c'] for x in ttv["records"]]

ttv_df = pd.DataFrame({'Opportunity':opportunity, 'Duration_Since_Lead':duration_since_lead,
                       'Duration_TTV': duration_ttv, 'Duration_Since_Launch':duration_since_launch})

ttv_df = ttv_df.sort_values('Opportunity')

print 'Google Analytics', 'length: ', len(ga_df), '\n\n', ga_df.head(),'\n'
#print 'Leads', 'length: ', len(leads_df), '\n\n', leads_df.head(),'\n'
print 'Accounts', 'length: ', len(accounts_df), '\n\n', accounts_df.head(),'\n'
print 'Contacts', 'length: ', len(contacts_df), '\n\n', contacts_df.head(),'\n'
print 'Opportunities', 'length: ', len(opps_df), '\n\n', opps_df.head(),'\n'
print 'TTV data', 'length: ', len(ttv_df), '\n\n', ttv_df.head(),'\n'

#Inner join opportunity and time to value data frames (we only want data entries where we have ttv)
opps_ttv_df = pd.merge(opps_df, ttv_df, left_on = 'Opp_Id', right_on = 'Opportunity', how ='inner')
print 'Opps-TTV data', 'length: ', len(opps_ttv_df), '\n\n', opps_ttv_df.head(),'\n'

#Inner join account and updated time to value data frame
accounts_opps_ttv_df = pd.merge(accounts_df, opps_ttv_df, left_on = 'Account_Name', right_on = 'Opp_Account_Name',
                                how ='inner')
print 'Accounts-Opps-TTV data', 'length: ', len(accounts_opps_ttv_df), '\n\n', accounts_opps_ttv_df.head(),'\n'

#Inner join contact and updated time to value data frame, call it salesforce dataframe
sf_df = pd.merge(contacts_df, accounts_opps_ttv_df, left_on = 'Contact_Account_Name', right_on = 'Account_Name',
                 how ='inner')
print 'Salesforce data', 'length: ', len(sf_df), '\n\n', sf_df.head(),'\n'

#THERE IS AN ISSUE IN SF_DF (Account ID's x and y dont match, even with single occurence (ie Clearbanc)

#Merge GA and SF dataframes and call it preprogram df
preprogram_df = pd.merge(ga_df, sf_df, left_on = 'c_id', right_on = 'C_Id', how ='inner')
#preprogram_df = ga_df.join(sf_df, how='inner', lsuffix = 'c_id', rsuffix = 'C_Id')
print 'PreProgram data', 'length: ', len(preprogram_df), '\n\n', preprogram_df.head(),'\n'


# sf_df = sf_df.sort_values('Account_Name')
# print 'Unique Account-to-Opportunities in SF:', ' length: ', len(sf_df.Account_Name.unique()),\
#     '\n\n', sf_df.Account_Name.unique()