import pandas as pd
from pdfReader import companyReport

banks = pd.read_csv('./BankPDFs.csv')
search_terms = ['total assets', "total liabilities"]
banks.columns = ['company', 'pdf_url', 'country']
comp_info = banks.drop('country', axis=1).to_dict('records')

dataset = []
for comp in comp_info[:4]:
    comapny = comp['company']
    pdf_url = comp['pdf_url']
    rep = companyReport(search_terms, comapny, pdf_url)
    print(rep)
    dataset.append(rep)
dataset = pd.concat(dataset)
dataset