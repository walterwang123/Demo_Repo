# The Following codes are written to test the AxiomaRisk Service output
# --------------------------------------------- STEP 1 Infomration Extractions ----------------------------------------------#
host='qa.axioma.com'
user = 'wwang2'
passwd = 'P@ssword2'
client_id = '3f150edc787242ec8ced2f73d351b722'
portfolio_name='Walter_Multi_Asset_Class_Model'
portfolio_date=datetime.date(2020,2,10)
riskmodel_name_to_run = "WW Risk Model Analysis"
riskmodel_name_to_run = ''
where_to_save_foliodir= "C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/AxiomaRiskModel_Service/Walter_Multi_Asset_Class_Model/"
view_name="Extended Instrument Analytics Instance WW"
riskmodel_name_to_save = "test_model_"
analysis_id_to_run = 3707171
where_to_save_attribute_folder = "C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/AxiomaRiskModel_Service/Walter_Multi_Asset_Class_Model/"
run_risk_model=False
run_aggregation_attributes=False
run_workspace_creation=False
attribute_file_url="%s/%s" % ("C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/AxiomaRiskModel_Service/", "attribute.att")
json_fifm_url = "%s/%s" % ("C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/AxiomaRiskModel_Service/", "test_model_")
date_for_the_workspace = "2020-02-10"

print('Initiating Axioma Risk - Risk Model Service')
blue = bluepy.AxRiskConnector(host='%s/rest' % (host),
                              user=user,
                              passwd=passwd,
                              client_id=client_id,
                              protocol="https",
                              debug=True)
all_analysis_items = blue.get_analysis_definition_names()['items']
all_analysis_ids = [x['id'] for x in all_analysis_items]
all_analysis_names = [x['name'] for x in all_analysis_items]
combined_all_analysis = all_analysis_ids + all_analysis_names

print ('Running Risk Model')
"""
blue.post_entity_from_template('Historical Estimation Settings',
    {
    "Name": "Lookback 200W, HL 50W, Overlap 20D WW",
    "HalfLifeValue": 50,
    "HalfLifeUnit": "W",
    "LookbackValue": 200,
    "LookbackUnit": "Week",
    "SamplingDays": 20,  #
    "SamplingOverlap": 19  # this is rolling 20 days, overalp 19 days
    })
"""
# Fixed Income Model
# rmName = "DV Risk Model Analysis WW"
rmName = riskmodel_name_to_run
"""
blue.post_entity_from_template('FI Risk Model',
                               {"Name": rmName,
                                   "Currency": "USD",
                                   #  "RiskResolution": "GranularForFIFMv3",
                                   #  "RiskResolution": "MAC Global-SR w FI Specific Risk",
                                   "RiskResolution": "DV FIFM Risk Resolution",
                                   # Risk resolution is used to determine what factor to include (essentially all factors returns are calcualted)
                                   # "RiskResolution": "MAC Global-SR 2.0",
                                   #  "RiskResolution": "4 Node Credit with SwapSpread Alpha 10",
                                   #  "RiskResolution": "MAC Global BondSR",
                                   # "RiskResolution": "Multi-Factor"
                                   "NeweyWestLag": 0,
                                   "CorrelationEstimation": "Lookback 200W, HL 50W, Overlap 20D WW",
                                   "VolatilityEstimation": "Lookback 200W, HL 50W, Overlap 20D WW",
                                   "RiskHorizonValue": 252,
                                   "RiskHorizonUnit": "BusinessDay"
                               })
"""
viewName = view_name
# rmName = "DV Risk Model Analysis WW"
# Check if Risk Model Already Exists
rmID = blue.get_entity_id(rmName, "RiskModelAnalysisSettings")
# status, headers = utils.request_model(blue, rmID, viewName, "Walter_Multi_Asset_Class_Model", "2020-02-10", recompute=False)
status, headers = utils.request_model(blue, rmID, viewName,
                                      portfolio_name, str(portfolio_date),
                                      recompute=True, timelimit=3600)

# result = blue.get_risk_model_instance(headers['Location'])
result = blue.get_risk_model_instance(headers['Location'],zipOut=True)

# json
if not os.path.exists(where_to_save_foliodir):
    os.makedirs(where_to_save_foliodir)
json.dump(result, open(os.path.join(where_to_save_foliodir, riskmodel_name_to_save + '_' + str(portfolio_date).replace('-','') +
                                    '.json'), 'w'))

utils.extract_rms_flatfiles(blue.get_risk_model_instance(headers,True), "C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Folder_upload/")

# --------------------------------------------- STEP 2 ----------------------------------------------------------------------------#
# Compare - https://jira.axiomainc.com/browse/RISK-3376
import pandas as pd
import numpy as np
axioma_risk_df = pd.read_csv('C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/WW Risk Model Analysis.20200210.cov',
                 index_col=0, skiprows=2, sep='|')

equity_risk_model_name = 'WW21AxiomaMH'
equity_risk_model_date = '20200210'
# Value checking, cross-checking with equity_risk_model value
equity_risk_model_url = 'P:/current/riskmodels/2.1/FlatFiles/2020/02/AXWW21-MH.20200210.cov'
equity_df = pd.read_csv(equity_risk_model_url,index_col=0, skiprows=2, sep='|')

# Number matching
equity_df.index = equity_risk_model_name+'.'+equity_df.index
equity_df.columns = equity_risk_model_name+'.'+equity_df.columns

common_factors = equity_df.index.intersection(axioma_risk_df.index)
axioma_risk_df_temp = df.loc[common_factors]
axioma_risk_df_temp = axioma_risk_df_temp[common_factors]
equity_df_temp = equity_df.loc[common_factors]
equity_df_temp = equity_df_temp[common_factors]

# Difference
# More probably due to value settings and various treatments
number_difference = equity_df_temp - axioma_risk_df_temp



# JIRA 3380
# https://jira.axiomainc.com/browse/RISK-3380
# Issuer specific covariance
isc_df = pd.read_csv('C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/WW Risk Model Analysis.20200210.isc',
                 index_col=0, skiprows=2, sep='|')
# read in json format
import json
with open("C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/test_model__20200210.json", "r") as read_file:
    isc_json = json.load(read_file)

# Need to check json
# Value I don't belive is properly scaled.
# Values look way too small
# Would need to x 10,000
axioma_risk_isc = isc_json['specificCovariances']
len_diff = len(axioma_risk_isc)-len(isc_df)
if len_diff == 0.0:
    print ('Values are the Same, Fixed')
# Values however....
equity_isc = 'P:/current/riskmodels/2.1/FlatFiles/2020/02/AXWW21-MH.20200210.isc'
equity_isc_df = pd.read_csv(equity_isc,index_col=0, skiprows=2, sep='|')


# JIRA
# https://jira.axiomainc.com/browse/RISK-3324


# JIRA
# https://jira.axiomainc.com/browse/RISK-3267
# May or may not be an issue, but original file has an extra line ('|') that prevents pandas from reading it
# Will need to modify it
axioma_risk_exposure = pd.read_csv('C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/WW Risk Model Analysis.20200210_modified.exp',
                                   index_col=0, skiprows=2)

axioma_risk_exposure = pd.read_csv('C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/WW Risk Model Analysis.20200210.exp',
                                   index_col=0, skiprows=2)

# JIRA
# https://jira.axiomainc.com/browse/RISK-3325
# Tested, all passed

# JIRA
# https://jira.axiomainc.com/browse/RISK-3005




# Get all glob
# Check date, and shift by 1 day
import glob
import os
all_files = glob.glob("C:/Users/wawang.QONTIGO/Downloads/EgAlpha/EgAlpha/*.csv")
# All available dates
# df = pd.read_csv('C:/Users/wawang.QONTIGO/Downloads/EgAlpha/EgAlpha.csv', index_col=0)
all_available_dates = [x.split('\\')[-1].split('_')[-1].replace('.csv', '') for x in all_files]
# unique
all_available_dates = list(set(all_available_dates))
# sort
all_available_dates.sort()
# Then we will need to loop through all files
for i in range(len(all_available_dates)-1):
    print ('Working on %s' %(all_available_dates[i]))
    # find file name.
    temp_file_names = [x for x in all_files if all_available_dates[i] in x]
    for t in temp_file_names:
        os.rename(t, t.replace(all_available_dates[i], all_available_dates[i+1]).replace('EgAlpha', 'EgAlpha_Shifted'))
    #


isc_axioma_risk = pd.read_csv('C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/WW Risk Model Analysis.20200210.isc', skiprows=2, sep='|')
isc_equity = pd.read_csv('D:/AxiomaRiskModels-FlatFiles/2020/02/AXUS4-MH.20200210.isc', skiprows=5, sep='|')
rsk_axioma_risk = pd.read_csv('C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/WW Risk Model Analysis.20200210.rsk', skiprows=2, sep='|')
# Covariance matrix to back-out the volatility
cov_axioma_risk = pd.read_csv('C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/WW Risk Model Analysis.20200210_modified.exp', skiprows=2, sep=',')
# take the pair
id_a = '202053681'
id_b = '225163361'
isc_pair = isc_axioma_risk.iloc[0]['Covariance']
id_a_vol = cov_axioma_risk[cov_axioma_risk['#Columns: AxiomaID'] == id_a][id_a]


# Exposure file
cov_axioma_risk = pd.read_csv('C:/Users/wawang.QONTIGO/Desktop/Cases/Brian/Axioma_Risk_Testing_Case/WW Risk Model Analysis.20200210 - Copy.exp',
                              skiprows=2, sep='|')


