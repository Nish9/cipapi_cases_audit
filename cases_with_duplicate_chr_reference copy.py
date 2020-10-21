from pycipapi.cipapi_client import CipApiClient
import typing
import pandas as pd
import itertools
import requests
import yaml

#need module openpyxl installed

cred_dict = yaml.load(open('/Users/nishitathota/Documents/GEL_CREDENTIALS.yaml'), Loader=yaml.FullLoader)
cipapi_credentials = {entry['name']: entry for entry in cred_dict}
username = cipapi_credentials['cip_api_prod']['username']
password = cipapi_credentials['cip_api_prod']['password']
c = CipApiClient(url_base=cipapi_credentials['cip_api_prod']['host'], user=username, password=password)
AUTH_ENDPOINT = f"{cipapi_credentials['cip_api_prod']['host']}/api/2/get-token/" #just an endpoint used within the function to ensure token immortality


def get_list_of_cases(sample_type:str, assembly:str):
    """ To get a list of all b38 cases of a certain sample type from cipapi;
    sample_type and assembly ex: raredisease/cancer, GRCh37/GRCh38 to be passed """
    cases= c.get_cases(sample_type=sample_type, assembly=assembly)
    # cases = itertools.islice(cases, 215, 235, 1) #(if you'd like to use only a subset at a time)
    return cases


def clin_report_data(caselist: object, AUTH_ENDPOINT: str, username: str, password: str):

    """Get gms cases and variant in clinical report where:
     the chromosome number has a chr in front of it ex: "chr1" instead of "1".
    This is because cellbase/cipapi adds an extra chr and hence it gets duplicated to "chrchr1".
    Input caselist is a generator object of all the cases to be evaluated.
    This is retrieved from cipapi using the .get_cases() function, see: get_list_of_cases() above"""
    try:
        c._verify_response(response=requests.post(url=AUTH_ENDPOINT, json=dict(username=username, password=password))) #restclient function to evaluate the response and renew token
    except Exception as e:
        print(e)
    df1 = {'case': [], 'clin_report_version': [], 'variant': [], 'total_reported_variants': []}
    for index, case in enumerate(caselist):
        print(index, case.case_id)
        ir, version = case.case_id.split('-')[1], case.case_id.split('-')[2]
        case = c.get_case(case_id=ir, case_version=version, reports_v6='true')
        if case.has_clinical_reports:
            CR = case.clinical_report
            for _report in CR:
                report = _report.clinical_report_data
                variant = list(variant["variantCoordinates"] for variant in report["variants"] if "chr" in variant["variantCoordinates"]["chromosome"])
                if variant:
                    df1["variant"].append(variant)
                    df1["case"].append(case.case_id)
                    df1['clin_report_version'].append(_report.clinical_report_version)
                    df1["total_reported_variants"].append(len(report["variants"]))

    df1 = pd.DataFrame.from_dict(data=df1)
    df1.to_excel("cases_with_duplicate_chr_in_CR.xlsx")
    print(df1, "\n", len(set(df1["case"])))
    return df1


sample_type= "raredisease"
assembly = "GRCh38"
caselist = get_list_of_cases(sample_type, assembly)
clin_report_data(caselist, AUTH_ENDPOINT, username, password)
