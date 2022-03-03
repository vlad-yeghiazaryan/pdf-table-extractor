from urllib.request import urlretrieve
import numpy as np
import pandas as pd
import re
import tabula
from pdfminer.layout import LAParams, LTTextBox
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator

def createDocMapping(fileUrl):
    urlretrieve(fileUrl,'document.pdf')
    fp = open('document.pdf', 'rb')
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    pages = PDFPage.get_pages(fp)
    document = []
    index = 0

    for page in pages:
        index+= 1
        interpreter.process_page(page)
        layout = device.get_result()
        for lobj in layout:
            if isinstance(lobj, LTTextBox):
                x, y, text = lobj.bbox[0], lobj.bbox[3], lobj.get_text()
                element = {'text':text.lower(), 'page': index, 'pos_x':x, 'pos_y':y, 'page_size': page.mediabox}
                document.append(element)
    fp.close()
    device.close()
    return pd.DataFrame(document)

def search_var(search_term, document, pages='all', y_interval=1, y_adj=0, index=0):
    if pages=='all':
        document_mapping = document
    else:
        document_mapping = document[document['page'].isin(pages)]
    search_results = document_mapping[document_mapping['text'].str.contains(search_term)]
    if len(search_results)==0:
        return None
    search_result = search_results.iloc[index,:]
    filter_page = search_result['page']
    filter_pos_y = search_result['pos_y'] + y_adj
    page_filtered_results = document_mapping[document_mapping['page'] == filter_page]
    row_filtered_results = page_filtered_results[(page_filtered_results['pos_y'] - filter_pos_y).abs() < y_interval]
    results = row_filtered_results.sort_values('pos_x')
    results['#_of_search_results'] = search_results.shape[0]
    return results

def datesOfTable(document, page):
    matches = search_var('\d\d\d\d', document, [page], 1, 0, index=0)['#_of_search_results'].iloc[0]
    for i in range(matches):
        dates = search_var('\d\d\d\d', document, [page], 1, 0, index=i)
        if dates.shape[0]==1:
            continue
        match = re.search(".*(?<![\s(31)(december)(\d\d\d\d)])",  dates['text'].iloc[-1])
        if match.group(0) != '':
            continue
        return dates

def search_row(search_term, document, x_adj=30, top_adj=5, bottom_adj=5, index=0):
    # get first match for the search term
    search_results = search_var(search_term, document, 'all', 1, 0, index=index)

    # return None if no results are returned
    if type(search_results) == type(None):
        if "\n" not in search_term:
            return search_row(search_term+"\n", document, x_adj, top_adj, bottom_adj)
        else:
            return None

     # If result is not from a table move to the next one:
    if (search_results.shape[0] == 1):
        remaining = search_results['#_of_search_results'].iloc[0]-1
        if index < remaining:
            return search_row(search_term, document, x_adj, top_adj, bottom_adj, index+1)
        elif "\n" not in search_term:
            return search_row(search_term+"\n", document, x_adj, top_adj, bottom_adj)
        else:
            return None

    # extract page data
    search_results_page = search_results['page'].iloc[0]
    search_results_page_size_y = search_results['page_size'].iloc[0][-1]

    # extract table area data
    table_dates = datesOfTable(document=document, page=search_results_page)
    
    if type(table_dates) == type(None):
        remaining = search_results['#_of_search_results'].iloc[0]-1
        if index < remaining:
            return search_row(search_term, document, x_adj, top_adj, bottom_adj, index+1)
        elif "\n" not in search_term:
            return search_row(search_term+"\n", document, x_adj, top_adj, bottom_adj)
        else:
            return None
    top = search_results_page_size_y - table_dates['pos_y'].min()
    bottom = search_results_page_size_y - search_results['pos_y'].max()
    left = search_results['pos_x'].min()
    right = search_results['pos_x'].max()
    area = (top-top_adj, left-x_adj, bottom+bottom_adj, right+x_adj)

    # crop table with tabula
    table = tabula.read_pdf('document.pdf', pages=[search_results_page], stream=True, area=area)[0]

    if table.iloc[-1, 0].lower() not in search_term:
        return search_row(search_term, document, x_adj, top_adj, bottom_adj+5)
    return table


def pdf_search(search_term, document):
    sr = search_row(search_term, document)
    if type(sr) == type(None):
        sr = pd.DataFrame({'year': [np.nan], search_term:[np.nan]})
        return sr.set_index('year')
    sr = sr.loc[:,sr.columns.str.contains('\d\d\d\d')].iloc[-1]
    sr = pd.DataFrame({'year': sr.index, search_term:sr.values})
    sr['year'] = sr['year'].apply(lambda date: re.search(r"(\d{4})", date).group(1))
    return sr.set_index('year').sort_index()

def companyReport(search_terms, company_name, pdf_url):
    document = createDocMapping(pdf_url)
    search_results = []
    for search_term in search_terms:
        search_result = pdf_search(search_term, document)
        search_results.append(search_result)
    report_table = pd.concat(search_results, join='outer', axis=1)
    report_table['company'] = company_name
    return report_table
