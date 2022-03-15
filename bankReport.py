import pandas as pd
from pdfReader import PdfTableReader 

banksUrl = "https://docs.google.com/spreadsheets/d/1gas2JMxp5RtlUGSIxXUy5eI6lpb3w4eChTepVZIJu2I/export?gid=0&format=csv"
banks = pd.read_csv(banksUrl)
banks.columns = ['company', 'pdf_url', 'country']
comp_info = banks.drop('country', axis=1).to_dict('records')
header_regex = '^((dec)|(december)|(12|31))?[\s.-]?((dec)|(december)|(12|31))?[.\s-]?((2021|2020|2019)|(applicable\samount)|(IV-Q20)|(amount))'
query = {
    'total assets': '[\n\s]?.{1,4}?\s?(total)\s(asset)s?[\s]?((as per published financial statements)|(in accordance with financial statements disclosed))?',
    'LCR': '(liquidity\scoverage\sratio)|(lcr)'
}

dataset = []
for comp in comp_info[:1]:
    comapny = comp['company']
    pdf_url = comp['pdf_url']
    extractor = PdfTableReader(query, comapny, pdf_url, header_regex, unknown_header_date='2020-12-31')
    rep = extractor.pdf_search(attempts=2, edge_tol=200, row_tol=5)
    dataset.append(rep)
dataset = pd.concat(dataset)
print(dataset[~dataset.index.isna()])