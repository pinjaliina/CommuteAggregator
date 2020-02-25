#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loop through a University of Helsinki / Geography Travel Time Matrix
Join data with MSSSUF (fi: YKR) commuting data and count aggregated
information.

Created on Tue Feb 18 13:13:02 2020

@author: Pinja-Liina Jalkanen (pinjaliina@iki.fi)
"""

#import sys
#import os
#import tempfile
#import datetime
import psycopg2
import csv
#import re
#import configparser
import argparse
from random import randint

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
            'from_id',
            'to_id',
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
            'from_id',  # From MSSSUF grid ID.
            'to_id',    # To MSSSUF grid ID.
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

# Define DB connection params. No passwd here; using a client cert. 
def get_db_conn():
    conn_params = {
        'host': 'localhost',
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
    pg.execute(query, params)
    if pg.rowcount > 0:
        results = pg.fetchall()
    return results

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
    for field in ttm_tuple[1]:
        sum_fields = sum_fields + 'SUM(' + ttmtblpfx + field + ' * yht) AS '\
            + field + ', '
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
    return sum_query

def build_tables(outfile, ic_data_only = False):
    pg = get_db_conn()
    csvfile = False
    try:
        csvfile = open(outfile, 'w', newline = '')
    except IOError:
        print('Cannot open the output CSV file ' + outfile + '!')
        raise
    writer = csv.writer(csvfile, delimiter='\t',
                        quotechar='', quoting=csv.QUOTE_NONE)
    for ttm in ttm_years():
    # for ttm in [2013]:
        writer.writerow(['TTM ' + str(ttm)] + list(ttm_fields_tuple(ttm)[1]))
        for year in journey_data_years():
            query = get_query_string(ttm, year)
            params = (str(year), '0')
            print('Processing the ' + str(ttm) + ' TTM with ' + str(year) + \
                  ' journey data...')
            query_res = run_query(pg, query, *params)
            writer.writerow([str(year) + ' journeys'] + list(query_res[0]))
    csvfile.close()

def main():
    parser = argparse.ArgumentParser(description='Create Time Travel Matrix aggregate tables.')
    parser.add_argument('outfile', help='Output file name.')
    args = parser.parse_args()
    build_tables(args.outfile)
    
if __name__ == '__main__':
    main()