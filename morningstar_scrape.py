__author__ = 'mshadish'
"""
This script is used to scrape MorningStar
for NAV information about specific closed-end funds

The API request can be structured as follows:
http://cef.morningstar.com/cef-header?&t=[exchange]:[symbol]&returnType=html&_=[epoch time]

For example...
http://cef.morningstar.com/cef-header?&t=XNYS:BME&returnType=html&_=1427318398203
"""

# imports
from bs4 import BeautifulSoup
import sys
import urllib2
import re
import time
import random
import csv
import numpy as np
from datetime import datetime

sys.path.insert(0, '/users/mshadish/cef_model/')


# specify the path to fund files
fund_file_path = '/users/mshadish/cef_model/fund_data/'


def generateMorningStarAPICall(exchange, symbol):
    """
    Generates the HTTP request URL based on the given exchange (XNYS, XNAS, etc.)
    as well as symbol
    
    :param exchange = string, represent the exchange the security is traded on
    :param symbol = string, symbol of the security
    
    Return:
        HTTP request URL
    """
    base_url = 'http://cef.morningstar.com/cef-header?&t='
    url_suffix = '&returnType=html&_='
    
    # combine the exchange and symbol
    exchange_and_symb = ':'.join([exchange, symbol])
    # and create the current epoch time
    now = str(int(time.time() * 1000))
    
    # combine everything into a single url
    return_url = ''.join([base_url, exchange_and_symb, url_suffix, now])
    return return_url



def attemptUrl(url, in_attempts = 5, interval = 3):
    """
    This function will attempt to open a URL a specific number of times
    before exiting
    
    Returns the instance returned from urllib2.urlopen()
    """
    # Initialize variables
    attempts = in_attempts
    request = urllib2.Request(url)
    page = None

    # try to open the URL
    # until we run out of "attempts,"
    # at which point we will move on
    while attempts > 0:
        
        try:
            page = urllib2.urlopen(request, timeout=30)
            return page
        except:
            print 'Trying ' + str(attempts - 1) + ' more times'
            wait = round(random.expovariate(1), 2)
            print 'Waiting %.2f seconds' % wait
            time.sleep(wait)
        """
        except urllib2.URLError, e:
            # catch the error, we may try again
            if hasattr(e, 'reason'):
                print e.reason
            
                # if the error is 'service unavailable', keep trying the url
                if str(e.reason).strip() == 'Connection reset by peer':
                    print 'Trying ' + str(attempts-1) + ' more times'
                    # pause
                    wait = round(random.expovariate(2), 2)
                    print 'Waiting %.2f seconds' % wait
                    time.sleep(wait)
                
            elif hasattr(e, 'code'):
                print 'Error: ' + str(e.code)
                break
        """
            
        attempts -= 1
    # end while loop
        
    # if we've made it out of the loop, we failed to open the page
    print 'Failed to open the url'
    return None
    
    
    
def soupify(page):
    """
    Takes in a page object
    Reads the page, returns a Beautiful Soup object
    with newlines subbed out
    """
    content = page.read()
    content = re.sub('\n', '', content)
    content = re.sub('\t', '', content)
    soup = BeautifulSoup(content, 'html5lib')
    
    return soup
    
    
    
def extractQuote(soup_obj):
    """
    Pulls the price, NAV, and corresponding date
    from a given beautiful soup object
    
    :param soup_obj = beautiful soup object, corresponding to a page
    from MorningStar returned from the API call used to fill its dynamic tables
    
    Returns:
        1) price (float)
        2) NAV (float)
        3) corresponding date (what format?)
    in the following form...
        (price, nav, date)
    """
    # first, let's find the price
    try:
        price_anchor = soup_obj.find('div', {'id': 'lastPrice'})
        price_str = price_anchor.get_text()
        # and convert it into our float format
        price = float(price_str.strip())
    except:
        #print 'could not extract price'
        price = None
    
    # next, find the NAV
    try:
        nav_anchor = soup_obj.find('td', {'id': 'last-act-nav'})
        # pull out the NAV
        nav_str = nav_anchor.get_text()
        # and format it into a float
        nav = float(nav_str.strip())
    except:
        #print 'could not extract NAV and date'
        nav = None
        date = None
        return price, nav, date
    
    # using our NAV anchor, step to the next sibling,
    # which is assumed to be the date
    date_anchor = nav_anchor.findNextSibling()
    # pull out the date
    date_str = date_anchor.get_text()
    # for now, we'll keep it in unicode
    date = date_str.strip()
    
    return price, nav, date
    
    
    
# open the csv containing the fund names and their respective exchanges
# and add the records to a list of (fund, exchange) tuples
key_list = []
with open('/users/mshadish/cef_model/almost_complete_mapping.csv', 'rU') as infile:
    reader = csv.reader(infile)
    for row in reader:
        key_list.append(tuple(row))
        
        
# initialize our reporting string
reporting_str = 'Could not extract the following fields:\n'

today = time.strftime('%m/%d/%Y', time.localtime(time.time()))
# now loop through our key list
for key in key_list:
    # for readibility, pull out the symbol and exchange
    fund = key[0]
    exchange = key[1]
    
    # do a quick wait
    wait = round(random.expovariate(1), 2)
    time.sleep(wait)
    
    # create our url
    request_url = generateMorningStarAPICall(exchange, fund)
    # request the url
    page = attemptUrl(request_url)
    # if the page is None, then we were unable to open the page
    # and so we will report and continue to the next symbol
    if page is None:
        print 'Could not open URL for ' + fund
        continue
    # turn the page into soup
    try:
        soup = soupify(page)
    except:
        continue
    # and pull out the contents
    quote = extractQuote(soup)
    
    # once we have the quote extracted, let's pull out the relevant fields
    price = quote[0]
    nav = quote[1]
    last_nav_date = quote[2]
    # and compute the derived values
    try:
        premium_discount = price - nav
        pd_pct = premium_discount / nav
    except TypeError:
        premium_discount = None
        pd_pct = None
    except ZeroDivisionError:
        pd_pct = None
    
    # finally, let's append this record to its file
    filename = fund_file_path + '.'.join([fund, 'csv'])
    
    # grab the current time
    now = datetime.now()
    time_to_write = (now.hour * 100) + now.minute
    
    with open(filename, 'ab') as outfile:
        # initialize the writer
        writer = csv.writer(outfile, lineterminator = '\n')
        # and write to the file
        try:
            writer.writerow([fund, today, last_nav_date, price, nav,
                             premium_discount, pd_pct, np.nan, np.nan,
                             time_to_write])
        except:
            print 'could not write for fund {0}'.format(fund)
            #pass
            raise
                         
    # report on any fields we could not extract
    if None in [price, nav, last_nav_date, premium_discount, pd_pct]:
        reporting_str += fund + ': '
        # will compile our missing fields in a list, then combine at the end
        missing_field_list = []
        if price is None:
            missing_field_list.append('price')
        if nav is None:
            missing_field_list.append('NAV')
        if last_nav_date is None:
            missing_field_list.append('last NAV date')
        if premium_discount is None:
            missing_field_list.append('premium/discount')
        # now combine
        reporting_str += ','.join(missing_field_list) + '\n'
# repeat for every fund
        
# print out the report of issues
print reporting_str
