# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 10:15:15 2015

@author: mshadish
"""

# imports
import os
import copy
import datetime
import pandas as pd
import multiprocessing as mp
from hidden_funcs import compute_signal
from hidden_funcs import pull_latest_signal


pd.options.mode.chained_assignment = None


# specify the path to the fund data
fund_file_path = '/users/mshadish/cef_model/fund_data/'



def normalize_single_date(single_record, trading_day_set):
    """
    Normalize a single datetime object by continuously subtracing a day
    until the day is in the set
    """
    # make a copy of the record
    out_record = copy.copy(single_record)
    # prevent infinite loops
    counter = 0

    # pull out the datetime
    dt_obj = out_record['quote_date_dt']
    time = out_record['time']

    while dt_obj not in trading_day_set:
        # subtract a day
        dt_obj = dt_obj - datetime.timedelta(1)
        # and add 24 hrs to the time
        time += 2400
        # infinite loop protection
        counter += 1
        if counter > 1000:
            print 'Infinite loop detected in date subtraction'
            raise

    # update the dataframe record
    out_record['quote_date_dt'] = dt_obj
    out_record['time'] = time

    return out_record



def normalize_dates_in_df(in_df, trading_day_set):
    """
    Takes in a dataframe with a quote date datetime column
    as well as a set of trading days (also datetime types)
    and for every day in our dataframe not in the trading day set,
    we'll continue subtracting a day from that record until the day
    is in a set (i.e., if we collected data on a Saturday for Friday)
    """
    out_df = in_df.apply(lambda x: normalize_single_date(x, trading_day_set),
                         axis=1)
    # overwrite the quote date
    out_df['quote_date'] = out_df['quote_date_dt'].apply(lambda x: x.strftime('%m/%d/%Y'))
    return out_df





def dedup_on_date(input_df):
    """
    Deduplicates the dataframe on date,
    taking the record with the most recent time
    """
    # copy the dataframe
    out_df = copy.copy(input_df)

    # group by the date
    grouped = out_df.groupby('quote_date')

    # take the record associated with the most recent time
    out_df = out_df.ix[grouped['time'].idxmax()]

    return out_df



def clean_NAs(input_df):
    """
    Forward-fills NA values and/or removes records
    where both price and nav are NA
    """
    input_df = input_df[pd.notnull(input_df['price']) | pd.notnull(input_df['nav'])]

    # fill NA nav's forward
    input_df['nav'] = input_df['nav'].fillna(method='ffill', axis=0)

    # fill price forward
    input_df['price'] = input_df['price'].fillna(method='ffill', axis=0)

    return input_df



def fund_cleaning_wrapper(filename, trading_day_set):
    """
    Wrapper to perform all of the operations
    i.e., reading in, transforming
    
    Returns the transformed dataframe
    """
    # read in the dataframe
    fund_df = pd.read_csv(filename)

    # add the quote date datetime column
    fund_df['quote_date_dt'] = fund_df['quote_date'].apply(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y'))

    # fill any NA nav's with the previous day
    fund_df = clean_NAs(fund_df)

    # normalize the data we have collected such that
    # all records correspond to trading days
    fund_df = normalize_dates_in_df(fund_df, trading_day_set)
    # ...such that we can dedupe on date
    fund_df = dedup_on_date(fund_df)

    return fund_df



def fund_cleaning_parallel_wrap(args):
    return fund_cleaning_wrapper(*args)



def write_out_df(dataframe):
    # grab the symbol
    symb = list(dataframe['symbol'])[0]

    dataframe.to_csv('/users/mshadish/cef_model/computed/{0}_computed.csv'.format(symb), index=False)
    return





if __name__ == '__main__':
    # initialize the pool for multiprocessing
    pool = mp.Pool()

    # take a list of the dataframes
    fund_file_list = os.listdir('/users/mshadish/cef_model/fund_data')
    fund_file_list = map(lambda x: '/users/mshadish/cef_model/fund_data/' + x,
                         fund_file_list)

    # pull the set of trading days
    trading_day_df = pd.read_table('/users/mshadish/cef_model/trading_days.txt', header=None)
    # convert to a list
    trading_day_list = list(trading_day_df[0])
    # convert the dates to datetimes
    trading_day_dt = map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y'),
                         trading_day_list)
    # convert to a set
    trading_day_set = set(trading_day_dt)


    clean_dfs = pool.map(fund_cleaning_parallel_wrap,
                         [(i, trading_day_set) for i in fund_file_list])

    computed_dfs = pool.map(compute_signal, clean_dfs)

    signals = map(pull_latest_signal, computed_dfs)
    # filter out na signals
    signals = filter(lambda x: pd.notnull(x[2]), signals)
    # and filter out any signals not lower than -1.8
    signals = filter(lambda x: x[2] <= -1.5, signals)
    # also filter out anything where the nav date isn't today
    signals = filter(lambda x: datetime.datetime.strptime(x[1], '%m/%d/%Y').toordinal() >= datetime.datetime.now().toordinal(), signals)
    # sort
    signals = sorted(signals, key=lambda x: x[2])
    # finally, format the scores to 2 decimal places
    signals = map(lambda x: '{0},{1},{2}'.format(x[0], datetime.datetime.strftime(datetime.datetime.strptime(x[1], '%m/%d/%Y'), '%m/%d/%y'), format(x[2], '.2f')), signals)

    #for i in sorted(signals, key=lambda x: x[2], reverse=True):
    for i in signals:
        print i
    
    # write out the dataframes to a 'derived' subdirectory
    pool.map(write_out_df, computed_dfs)
