# main 
import os
import datetime
import numpy as np
import pandas as pd
from functools import reduce
from collections import Counter
from decimal import InvalidOperation

# text processing
import re
import regex
import datefinder

# pdf processing
import requests
import camelot
import pikepdf
from tika import parser

class PdfTableReader():
    def __init__(self, company_name, pdf_url, attempts=1, flavor='stream', 
                 edge_tol=50, column_tol=0, document='document.pdf'):
        
        # setup
        self.company_name = company_name
        self.pdf_url = pdf_url
        self.attempts = attempts
        self.document = document
        self.flavor = flavor
        self.edge_tol = edge_tol
        self.column_tol = column_tol

        # preparing data
        pdf_path = self.retrieve_pdf(pdf_url, document)
        if type(pdf_path) != type(None):
            self.pages = np.array(self.parse_pages(pdf_path))
        else:
            self.pages = []

    # getting document pdf
    @staticmethod
    def retrieve_pdf(pdf_url, document_path):
        try:
            r = requests.get(pdf_url, timeout=10)
            with open(document_path, 'wb') as outfile:
                outfile.write(r.content)
        except:
            return None
        try:
            if r.ok:
                pdf = pikepdf.open(document_path, allow_overwriting_input=True)
                pdf.save(document_path)
                pdf.close()
            else:
                return None
        except pikepdf.PdfError:
            return None
        return document_path

    # fast pdf text parser
    @staticmethod
    def parse_pages(file):
        raw_xml = parser.from_file(file, xmlContent=True)
        body = raw_xml['content'].split('<body>')[1].split('</body>')[0]
        body_without_tag = body.replace("<p>", "").replace("</p>", "").replace("<div>", "").replace("</div>","").replace("<p />","")
        text_pages = body_without_tag.split("""<div class="page">""")[1:]
        num_pages = len(text_pages)
        if num_pages==int(raw_xml['metadata']['xmpTPg:NPages']): 
            #check if it worked correctly
            return text_pages
        
    # Identify all pages from each search term
    def createPageMapping(self, table_query):
        page_matches = {}
        for search, query in table_query.items():
            if len(query)!=0 and len(self.pages)!=0:
                row_matches = np.vectorize(lambda x: bool(re.search(query['row'], x, re.IGNORECASE)))(self.pages)
                column_matches = np.vectorize(lambda x: bool(re.search(query['column'], x, re.IGNORECASE)))(self.pages)
                related_matches = []
                for related_terms in query['related_terms']:
                    related_match = []
                    for related_term in related_terms:
                        rm = np.vectorize(lambda x: bool(re.search(related_term, x, re.IGNORECASE)) if related_term!='' else True)
                        rm = rm(self.pages)
                        related_match.append(rm)
                    related_match = np.logical_or.reduce(related_match)
                    related_matches.append(related_match)
                
                related_matches = np.logical_and.reduce(related_matches)         
                mapping = row_matches & column_matches & related_matches

                # adding 1 to convert indices to page numbers
                page_matches[search] = (np.where(mapping)[0]+1).tolist()
            else:
                page_matches[search] = []
        
        matches = []
        for search, pages in page_matches.items():
            for page in pages:
                match = {'search':search, 'row':table_query[search]['row'], 'column':table_query[search]['column'], 
                         'selector':table_query[search]['select'], 'page':page}
                matches.append(match)
        return pd.DataFrame(matches)
    
    # extracts best frequency based match from each page
    def pageFreqMatch(self, match_name, query):
        page_matches = []
        matches = []
        for index, page_text in enumerate(self.pages):
            text_matches = regex.findall(query, page_text, re.IGNORECASE)
            match = {'page':index+1,'freq_dict':Counter(text_matches)}
            page_matches.append(match)
        freq_table = sum(pd.DataFrame(page_matches)['freq_dict'], Counter())
        freq_max = max(freq_table, key=freq_table.get, default='No matches found')
        for page_match in page_matches:
            intersection = match['freq_dict'] | freq_table
            matches.append({match_name: max(intersection, key=intersection.get, default=freq_max), 
                            'page':page_match['page']})
        return pd.DataFrame(matches)
    
    def extractPageTables(self, page, flavor, edge_tol, column_tol):
        # extract all tables from page
        try:
            tables = camelot.read_pdf(self.document, pages=f'{page}', flavor=flavor, edge_tol=edge_tol, 
                                      column_tol=column_tol, suppress_stdout=True, flag_size=True)
        except (RecursionError, InvalidOperation, ZeroDivisionError):
            tables = []
        return tables

    # finds all tables from a single page
    def extractPageAttempts(self, page, attempts, 
                            flavor, edge_tol, column_tol):
        page_tables = []
        for attempt in range(attempts):
            edge_tol_increment = 50
            column_tol_increment = 20
            et = edge_tol+(edge_tol_increment*attempt)
            ct = column_tol+(column_tol_increment*attempt)
            tables = self.extractPageTables(page, flavor, et, ct)
            for index, table in enumerate(tables):
                page_tables.append({'attempt':attempt+1, 'table_index_in_page':index, 'table':table})
        return pd.DataFrame(page_tables)

    # loops through the pages and extracts required tables
    def getTables(self, page_matches, attempts, flavor, edge_tol, column_tol):
        tables = []
        for page in page_matches['page'].unique():
            page_tables = self.extractPageAttempts(page, attempts, flavor, 
                                                   edge_tol, column_tol)
            page_tables['page'] = page
            tables.append(page_tables)
        tables = pd.concat(tables)
        return tables

    def makeFrequencyExpansion(self, page_matches, frequency_query):
        frequency_matches = []
        for query_name, query_selector in frequency_query.items():
            frequency_match = self.pageFreqMatch(query_name, query_selector)
            frequency_matches.append(frequency_match)

        # combine all freq matches by page
        frequency_matches = reduce(lambda  left, right: pd.merge(left, right, 'inner', 'page'), frequency_matches)

        # add freq matches to page extraction info
        page_matches = pd.merge(page_matches, frequency_matches, 'left', 'page')
        return page_matches

    def search(self, table_query, frequency_query={}):
        # get candidate pages
        self.page_matches = self.createPageMapping(table_query)
        
        # extract high frequency info from each page
        if not self.page_matches.empty:
            self.page_matches = self.makeFrequencyExpansion(self.page_matches, frequency_query)
            
            # adding company
            self.page_matches['company'] = self.company_name

            # extract tables from each of the page matches
            unstructured_tables = self.getTables(self.page_matches, self.attempts, self.flavor, self.edge_tol, self.column_tol)
            unstructured_tables = pd.merge(self.page_matches, unstructured_tables)
            return unstructured_tables
        return pd.DataFrame()
