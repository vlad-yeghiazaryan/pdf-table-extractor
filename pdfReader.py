# main 
import datetime
import numpy as np
import pandas as pd
from functools import reduce

# charts
import matplotlib.pyplot as plt
from PIL import Image

# text processing
import re
import datefinder

# pdf processing
from urllib.request import urlretrieve
from urllib.error import HTTPError
import camelot
import pikepdf
from tika import parser

class PdfTableReader():
    def __init__(self, query, company_name, pdf_url, header_regex_match='default', unknown_header_date='2000-12-31'):
        self.company_name = company_name
        self.extracted_tables = {}
        self.current_search_terms = []
        self.pages = []
        self.unknown_header_date = unknown_header_date
        self.query = query
        self.search_terms = list(query.values())
        self.page_matches = self.createPageMapping(self.search_terms, pdf_url)
        aDate = '((dec)|(december)|(\d\d)|(\d\d\d\d))'
        date_query = f'^{aDate}[\s.-]{aDate}?[.\s-]?{aDate}$'
        self.header_match = date_query if header_regex_match =='default' else header_regex_match

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
    def createPageMapping(self, search_terms, pdf_url):
        # getting document pdf
        try:
            urlretrieve(pdf_url,'document.pdf')
        except HTTPError: 
            return {}
        try:
            pdf = pikepdf.open('document.pdf', allow_overwriting_input=True)
            pdf.save('document.pdf')
            pdf.close()
        except pikepdf.PdfError:
            return {}

        # get pdf pages and extract mapping
        self.pages = np.array(self.parse_pages('document.pdf'))
        matches = {}
        for search_term in search_terms:
            term_match = np.vectorize(lambda x: bool(re.search(search_term, x, re.IGNORECASE)))
            # adding 1 to convert indices to page numbers
            matches[search_term] = (np.where(term_match(self.pages))[0]+1).tolist()
        return matches
    
    def convertDate(self, date):
        dates = list(datefinder.find_dates(date))
        if len(dates) == 0:
            return self.unknown_header_date
        return dates[0].strftime("%Y-%m-%d")

    # custom table output format
    def  customTable(self, search_term, df, row_matches, columns_matches):
        e = df.iloc[row_matches, columns_matches]
        new_header = e.iloc[0].values 
        e = e[1:].reset_index(drop=True)
        e.columns = new_header
        e = e.T
        e.columns =[search_term]
        e = e.replace('', np.NaN).dropna().applymap(lambda x: x if bool(re.search('[\d,]+', x, re.IGNORECASE)) else np.nan)
        e.index = pd.Series(e.index).apply(self.convertDate)
        return e
    
    # searches and extracts most relevant info from the table
    def search_page(self, search_term, page, flavor='stream', edge_tol=100, row_tol=5):
        # extract all tables from page
        tables = camelot.read_pdf('document.pdf', pages=f'{page}', flavor=flavor, edge_tol=edge_tol, row_tol=row_tol, 
                                  suppress_stdout=True, flag_size=True)

        # Return None if no tables found
        if len(tables)==0:
            return (None, None)
        
        # find the table that contains the search term and return the table extract
        for table in tables:
            df =  table.df
            row_matches = df[df.apply(lambda l: any([re.search(search_term, x.lower().strip()) != None for x in l]), axis=1)].index.to_list()
            date_rows = df[df.apply(lambda l: any([re.search(self.header_match, x.lower().strip()) != None for x in l]), axis=1)]
            if (len(row_matches) != 0) and (len(date_rows)!=0):
                row_matches = [match for match in row_matches if match>date_rows.index[0]]
            if (len(row_matches) != 0) and (len(date_rows)!=0):
                row_matches.insert(0, date_rows.index[0])
                columns_matches = date_rows.iloc[0][date_rows.iloc[0].apply(lambda x: re.search(self.header_match, x.lower().strip()) != None)].index.to_list()
                return (self.customTable(search_term, df, row_matches[:2], columns_matches), table)
        return (None, None)
    
    # finds the best table in the page
    def findBestTable(self, search_term, page, attempts = 5, flavor='stream', edge_tol=100, row_tol=5):
        tables = []
        for attempt in range(attempts):
            df, table = self.search_page(search_term, page, flavor=flavor, edge_tol=edge_tol+(50*attempt), row_tol=row_tol+(10*attempt))
            if type(df) == type(None):
                continue
            if table.accuracy==0:
                good_quality = 0
            else:
                good_quality = 0.7*np.log(np.abs(table.accuracy))
            if table.whitespace==0:
                bad_quality = 0
            else:
                bad_quality = 0.3*(np.log(table.whitespace)*2)
            quality = good_quality - bad_quality
            tables.append({'table': table, 'df': df, 'quality':quality})
        if len(tables) == 0:
            return (None, None)
        tableReport = pd.DataFrame(tables)
        bestTable = tableReport.loc[tableReport['quality'].idxmax()]
        return (bestTable['df'], bestTable['table'])
    
    # compares tables from different pages and selects the most relevant info
    @staticmethod
    def selectBestMatch(tableReport):
        row = tableReport['#_of_rows']
        na = tableReport['NAs']
        bt_index = (((na+1)/((row+1)*row))*((na/row > 0.50)+1)).argmin()
        bestTable = tableReport.iloc[bt_index]
        return bestTable
    
    # performs a query search across pages and returns a table
    def search_pages(self, search_term, attempts = 5, flavor='stream', edge_tol=100, row_tol=5):
        df = pd.DataFrame({search_term:[np.nan]}, index=[np.nan])
        if len(self.page_matches) == 0:
            return (df, None)
        pages = self.page_matches[search_term]
        if len(pages) == 0:
            return (df, None)
        tables = []
        for page in pages:
            df, table = self.findBestTable(search_term, page, attempts, flavor, edge_tol=edge_tol, row_tol=row_tol)
            if type(df) == type(None):
                continue
            tables.append({'page':page, '#_of_rows': df.count().sum(), 'NAs':df.isna().sum().sum(), 'accuracy':np.abs(table.accuracy), 
                           'whitespace':table.whitespace, 'df':df, 'table':table})
        if len(tables) == 0:
            df = pd.DataFrame({search_term:[np.nan]}, index=[np.nan])
            return (df, None)
        tableReport = pd.DataFrame(tables)
        bestTable = self.selectBestMatch(tableReport)
        return (bestTable['df'], bestTable['table'])
    
    # finds related info for all of the search terms in the pdf
    def pdf_search(self, attempts = 2, flavor='stream', edge_tol=200, row_tol=20):
        tables = []
        self.extracted_tables = {}
        self.current_search_terms = self.search_terms
        for search_term in self.search_terms:
            df, table = self.search_pages(search_term, attempts, flavor, edge_tol, row_tol)
            tables.append(df)
            self.extracted_tables.update({search_term:table})
        report_table = reduce(lambda left,right: pd.merge(left,right,left_index=True, right_index=True, how='outer'), tables)
        report_table['company'] = self.company_name
        report_table.index.name = 'year'
        report_table.rename(columns={y:x for x,y in self.query.items()}, inplace=True)
        return report_table
    
    def plot_extracts(self):
        eTables = self.extracted_tables
        fig, axis = plt.subplots(2,2, figsize=(15, 10))
        for index, search_term in enumerate(self.current_search_terms):
            if type(eTables[search_term]) != type(None):
                cImage1 = camelot.plot(eTables[search_term], kind='contour')
                cImage2 = camelot.plot(eTables[search_term], kind='textedge')
                cImage1.canvas.draw()
                plt.close()
                cImage2.canvas.draw()
                plt.close()
                image1 = Image.frombytes('RGB', cImage1.canvas.get_width_height(),cImage1.canvas.tostring_rgb())
                image2 = Image.frombytes('RGB', cImage2.canvas.get_width_height(),cImage2.canvas.tostring_rgb())
                axis[0, index].title.set_text(search_term)
                axis[0, index].imshow(image1)
                axis[1, index].imshow(image2)
            else:
                axis[0, index].title.set_text(search_term)
        return fig
