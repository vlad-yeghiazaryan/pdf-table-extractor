import pandas as pd
from pdfReader import PdfTableReader

banks = pd.read_csv('./BankPDFs.csv')
banks.columns = ['company', 'pdf_url', 'country']
comp_info = banks.drop('country', axis=1).to_dict('records')
search_terms = ['total assets', "total liabilities"]

dataset = []
for comp in comp_info[:4]:
    comapny = comp['company']
    pdf_url = comp['pdf_url']
    extractor = PdfTableReader(search_terms, comapny, pdf_url)
    rep = extractor.pdf_search(attempts=1)
    print(rep)
    dataset.append(rep)
dataset = pd.concat(dataset)
print(dataset)
