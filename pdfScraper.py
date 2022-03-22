# main 
import os
import datetime
import pandas as pd

# charts
from tqdm.notebook import tqdm_notebook

# pdf processing
from glob import glob
from pathlib import Path
from urllib.error import HTTPError

# custom
from pdfTableReader import PdfTableReader

class PdfScraper():
    def __init__(self, input_file, output_file='data.pkl'):
        self.input_file = input_file
        self.output_file = output_file

    def scrape(self, search_query, frequency_query=[], start_index=0):
        Path("data").mkdir(parents=True, exist_ok=True)
        comp = pd.read_csv(self.input_file)
        comp_info = comp[['company', 'pdf_url']].to_dict('records')
        
        for info in tqdm_notebook(comp_info[start_index:]):
            name = info['company']
            url =  info['pdf_url']
            self.reader = PdfTableReader(name, url)
            unstructured_tables = self.reader.search(search_query, frequency_query)
            unstructured_tables.to_pickle(f'data/{name}.pkl')
        df_files = glob('data/*.pkl')
        dataset = pd.concat([pd.read_pickle(fp) for fp in df_files], ignore_index=True)
        dataset = pd.merge(comp, dataset, how='outer', on='company')
        dataset.to_pickle(self.output_file)
