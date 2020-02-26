#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loop through a University of Helsinki / Geography Travel Time Matrix
Join data with MSSSUF (fi: YKR) commuting data and count aggregated
information.

Created on Tue Feb 18 13:13:02 2020

@author: Pinja-Liina Jalkanen (pinjaliina@iki.fi)
"""

import sys
#import os
#import tempfile
#import datetime
import psycopg2
import re
#import configparser
import argparse

# get_relations(postgresql)

def ttm_years(): return (2013, 2015, 2018)

def journey_data_years(all_y = False):
    if not (all_y):
        return (2012, 2014, 2015, 2016)
    else:
        return (2007, 2009, 2010, 2012, 2014, 2015, 2016)

def ttm_fields_tuple(chosen_year):
    chosen_year = str(chosen_year)
    year = {
        '2013': (
            'walk_t',
            'walk_d',
            'pt_m_tt',
            'pt_m_t',
            'pt_m_d',
            'car_m_t',
            'car_m_d',
            ),
        '2015': (
            'walk_t',
            'walk_d',
            'pt_r_tt',
            'pt_r_t',
            'pt_r_d',
            'pt_m_tt',
            'pt_m_t',
            'pt_m_d',
            'car_r_t',
            'car_r_d',
            'car_m_t',
            'car_m_d',
            ),
        '2018': (
            'walk_t',   # Walking time.
            'walk_d',   # Walking distance.
            'bike_s_t', # Standard riding time on a bicycle.
            'bike_f_t', # Fast riding time on a bicycle.
            'bike_d',   # Riding distance on a bicycle.
            'pt_r_tt',  # Rush-hour PT travel time incl. initial waiting.
            'pt_r_t',   # Rush-hour PT travel time without initial waiting.
            'pt_r_d',   # Rush-hour Public transport (PT) travel distance.
            'pt_m_tt',  # Midday PT travel time incl. initial waiting.
            'pt_m_t',   # Midday PT travel time without initial waiting.
            'pt_m_d',   # Midday Public transport (PT) travel distance.
            'car_r_t',  # Rush-hour car travel time.
            'car_r_d',  # Rush-hour car travel distance.
            'car_m_t',  # Midday car travel time.
            'car_m_d',  # Midday car travel distance.
            'car_sl_t', # Car travel time based only on speed limits.
            )
        }
    try:
        fields = year.get(chosen_year)
        return (chosen_year, fields)
    except KeyError('No such year in the TTM: ' + chosen_year):
        exit(1)

def journey_fields_tuple():
    fields = (
        'akunta',
        'tkunta',
        'vuosi',
        'matka',
        'yht',
        'a_alkut',
        'b_kaivos',
        'c_teoll',
        'd_infra1',
        'e_infra2',
        'f_rakent',
        'g_kauppa',
        'h_kulj',
        'i_majrav',
        'j_info',
        'k_raha',
        'l_kiint',
        'm_tekn',
        'n_halpa',
        'o_julk',
        'p_koul',
        'q_terv',
        'r_taide',
        's_muupa',
        't_koti',
        'u_kvjarj',
        'x_tuntem',
        'txyind',
        'axyind',
        )
    return fields

def get_result_fields(option = 0):
    base = ['measure', 'ttm_year', 'journey_year']
    if option == 0:
        return tuple(base + ['total'])
    if option == 1 or option == 3:
        ic = list()
        for field in journey_fields_tuple():
            if re.match('[a-z]_[a-z0-9]{4,}', field):
                ic.append(field)
        if option == 1:
            return tuple(base + ['total'] + ic)
        if option == 3:
            return tuple(['yht'] + ic)
    if option == 2:
        return tuple(['yht'])
        
# Define DB connection params.
def get_db_conn():
    conn_params = {
         'dbname': 'tt'
        }
    
    try:
        dbconn = psycopg2.connect(**conn_params)
        dbconn.autocommit = True
        return dbconn.cursor()
    except psycopg2.OperationalError:
        print('Failed to establish a DB connection!\n')
        raise

# Run a DB query. Note: this func DOES NOT validate the query string!
def run_query(pg, query, *params):
    # "SELECT tablename FROM pg_tables WHERE schemaname='public'"
    results = False
    # print(pg.mogrify(query, params)) #DEBUG!
    pg.execute(query, params)
    if re.match('^DROP TABLE.*', query) and \
        re.match('^DROP TABLE$', pg.statusmessage):
        results = True
    if pg.rowcount > 0:
        try:
            results = pg.fetchall()
        except psycopg2.ProgrammingError:
            results = True
            pass
    return results

# Check if table exists and create it if needed. Note that this
# function alone is not SQL injection safe if tablename is user input!
def check_table(pg, tablename, *fields, drop = False):
    try:
        if drop:
            query = 'DROP TABLE IF EXISTS {}'.format(tablename)
            results = run_query(pg, query)
            if results:
                print('Dropping existing table "' + tablename + \
                      '" as requested...')
    except psycopg2.ProgrammingError:
        print('Failed to delete table ' + tablename + '!')
        raise
    try:
        fields = '(' + fields[0] + ' text not null, ' + \
            fields[1] + ' integer not null, ' + \
            fields[2] + ' integer not null, ' + ' integer not null, '\
            .join(fields[3:]) + ' bigint not null)'
        query = 'CREATE TABLE ' + tablename + ' ' + fields
        run_query(pg, query)
        return tablename
    except psycopg2.ProgrammingError:
        print('Failed to create the table ' + tablename + '!')
        raise

def get_query_string(ttm_y, msssuf_y, ic_data_only = False):
    ttm_tuple = ttm_fields_tuple(ttm_y)
    ttmtbl = 'hcr_journeys_t_d_' + ttm_tuple[0]
    ttmtblpfx = ttmtbl + '.'
    # ttmtbl_tuple_sep = ', ' + ttmtblpfx
    # ttm_data_fields = ttmtblpfx + ttmtbl_tuple_sep.join(ttm_tuple[1])
    # ttm_key_fields = ttmtblpfx + 'from_id, ' + ttmtblpfx + 'to_id, '
    # ttm_fields = ttm_key_fields + ttm_data_fields
    # journey_fields = 'j.' + ', j.'.join(journey_fields_tuple())
    sum_fields = ''
    agg_fields = get_result_fields(option = int(ic_data_only) + 2)
    print(agg_fields)
    for ttm_field in ttm_tuple[1]:
        for agg_field in agg_fields:
            sum_fields = sum_fields + 'SUM(' + ttmtblpfx + ttm_field + \
                ' * ' + agg_field + ') AS ' + ttm_field + ', '
    sum_fields = sum_fields[:-2]
#     query = '\
# SELECT ' +  ttm_fields  + ', ' + journey_fields + ' \
# FROM hcr_msssuf_grid g \
# INNER JOIN ' + ttmtbl + ' ON g.gid = ' + ttmtbl + '.from_id \
# AND ' + ttmtbl + '.from_id=5925835 \
# INNER JOIN hcr_msssuf_journeys j ON g.xyind = j.axyind \
# WHERE j.vuosi = %s AND j.sp=0'# \
# # GROUP BY ' + ttm_key_fields + ', ' + ttm_fields + ', ' + journey_fields
    #print(query)
    sum_query = '\
SELECT ' + sum_fields + ' FROM ' + ttmtbl + ' INNER JOIN \
hcr_msssuf_journeys j ON ' + ttmtbl + '.id = j.id AND \
' + ttmtbl + '.vuosi = j.vuosi WHERE \
' + ttmtbl + '.vuosi = %s AND ' + ttmtbl + '.sp = %s'
    if(ic_data_only):
        sum_query = sum_query + ' AND j.yht >= 10'
    print(sum_query)
    sys.exit(1)
    return sum_query

def build_tables(tablename, ic_data_only = False, drop = False):
    pg = get_db_conn()
    fields = get_result_fields(option = int(ic_data_only))
    tablename = check_table(pg, tablename, *fields, drop = drop)
    # for ttm in ttm_years():
    for ttm in [2013]:
        rowhead = ttm_fields_tuple(2013)[1]
        for year in journey_data_years():
            query = get_query_string(ttm, year, ic_data_only = ic_data_only)
            params = (str(year), '0')
            print('Processing the ' + str(ttm) + ' TTM with ' + str(year) + \
                  ' journey data...')
            query_res = run_query(pg, query, *params)
            for key, res_item in enumerate(query_res[0]):
                row = ([rowhead[key], ttm, year, res_item])
                columns = ', '.join(fields)
                params = ', '.join(['%s' for i in row])
                insert = 'INSERT INTO {} ({}) VALUES ({})'.format(
                    tablename, columns, params)
                run_query(pg, insert, *row)

# Define a custom argparse action for checking that the tablename is sane.
class tablename_check(argparse.Action):
    def __call__(self, parser, namespace, tablename, option_string=None):
        # Define an exception handler to enable suppressing traceback:
        def exceptionHandler(exception_type, exception, traceback):
            print('{}: {}'.format(exception_type.__name__, exception))
        
        if re.match('^[a-z0-9_]{3,}$', tablename):
            setattr(namespace, self.dest, tablename)
        else:
            parser.print_usage()
            sys.excepthook = exceptionHandler # Suppress traceback.
            raise argparse.ArgumentTypeError(
                'Table name contains illegal characters!')

def main():
    parser = argparse.ArgumentParser(
        description='Create Time Travel Matrix aggregate tables.')
    parser.add_argument('tablename', action=tablename_check, \
                        help='Output table name. Allowed characters ' + \
                        "are lowercase a-z, 0-9 and underscore ('_').")
    parser.add_argument('-d', '--drop', dest='drop', \
                        default=False, action='store_true', \
                            help='Drop existing table of the same name.')
    parser.add_argument('-c', '--classified', dest='ic', \
                        default=False, action='store_true', help=\
                            'Count only journeys classified by industry.')
    args = parser.parse_args()
    build_tables(args.tablename, ic_data_only = args.ic, drop = args.drop)
    
if __name__ == '__main__':
    main()