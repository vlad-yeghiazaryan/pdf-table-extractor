import pandas as pd
from pdfScraper import PdfScraper 

banksUrl = "bankQueryData.csv"
ta_row = '(total\sassets?)'
col_regex = '((dec)|(december)|(12|31))?[\s.-/]?((dec)|(december)|(12|31))?[.\s\-\/]?((2021|2020|2019)|(IV-Q20)|Q20)'
rel_terms = [['(financial (position|statement)s?)', 'summary (of\sthe|on) reconciliation', 'balance sheet']]
many_scale_units = '((?:in\smillions)|(?:A-Z){3}\s?m|(?:\p{Sc}\s?mn?))'
table_query = {'total assets': {'row': 'total assets', 'column': col_regex, 
                                'select':'[\d,]+', 'related_terms': rel_terms}, 
               'total liabilities': {'row': 'total liabilities', 'column': col_regex, 
                                     'select':'[\d,]+', 'related_terms': rel_terms}}
frequency_page_matches = {'currency': '(\p{Sc}|(?:A-Z){3})', 'units':many_scale_units}

extractor = PdfScraper(banksUrl)
extractor.scrape(table_query, frequency_page_matches, start_index=12)
