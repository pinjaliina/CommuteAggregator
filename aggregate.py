#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loop through a University of Helsinki / Geography Travel Time Matrix
Join data with MSSSUF (fi: YKR) commuting data and count aggregated
information. Write the output back to the DB.

Created on Tue Feb 18 13:13:02 2020

@author: Pinja-Liina Jalkanen (pinjaliina@iki.fi)
"""

import sys
import psycopg2
import re
import argparse

def ttm_years():
    """Return a tuple of all available Time Travel Matrices."""
    return (2013, 2015, 2018)

def journey_data_years(all_y = False):
    """Return available journey data years
    
    Either return all the years, if all_y = True, or just
    those that are relevant from the TTM viewpoint.
    """
    
    if not (all_y):
        return (2012, 2014, 2015, 2016)
    else:
        return (2007, 2009, 2010, 2012, 2014, 2015, 2016)

def ttm_fields_tuple(chosen_year):
    """Return the data fields of a particular TTM.
    
    Return all the DB fields (except from_id and to_id)
    of a particular Time Travel Matrix. Takes the TTM
    year as an argument.
    """
    
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
    """Returns the data fields of the journeys table (no id/xy/geom fields.)"""

    fields = (
        'akunta',
        'tkunta',
        'vuosi',
        'matka',
        'sp',
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
        )
    return fields

def get_result_fields(option = 0, regs = False):
    """Returns a field list for the SQL queries.
    
    Depending on the value of the "option" argument it might return either
    a complete list of fields for the target table or just those fields
    of the source table whose values are meant to be aggregated, as follows:
        
    0: the complete target field list, without industry classification fields.
    1: the complete target field list, with industry classification fields.
    2: source table fields to be aggregated, without IC fields.
    3: source table fields to be aggregated, with IC fields.
    
    If "reg_fields" is true, region fields are appended to complete lists.
    """
    
    base = ['measure', 'ttm_year', 'journey_year']
    reg_fields = ['mun', 'area', 'dist', 'reg_id']
    if option == 0:
        if not regs:
            return tuple(base + ['total'])
        else:
            return tuple(base + reg_fields + ['total'])
    if option == 1 or option == 3:
        ic = list()
        for field in journey_fields_tuple():
            if re.match('[a-z]_[a-z0-9]{4,}', field):
                ic.append(field)
        if option == 1:
            if not regs:
                return tuple(base + ['total'] + ic)
            else:
                return tuple(base + reg_fields + ['total'] + ic)                
        if option == 3:
            return tuple(['yht'] + ic)
    if option == 2:
        return tuple(['yht'])
        
# Define DB connection params.
def get_db_conn():
    """Define DB connection params.
    
    If successful, return a psycopg2 DB cursor.
    
    On localhost just dbname will usually suffice, but over remote
    connections e.g. the following can be defined as well:
        * host
        * user
        * sslmode (please use "verify-full"!)
        * password (if using password authentication instead of a client cert).
    """
    
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

def run_query(pg, query, *params):
    """Run a DB query. Note: this func DOES NOT validate the query string!"""
    
    # Show tables: "SELECT tablename FROM pg_tables WHERE schemaname='public'"
    results = False
    # print(pg.mogrify(query, params)) #DEBUG: print the query str as executed.
    pg.execute(query, params)
    if re.match('^DROP TABLE.*', query) and \
        re.match('^DROP TABLE$', pg.statusmessage):
        results = True
    if pg.rowcount > 0:
        try:
            results = pg.fetchall()
        except psycopg2.ProgrammingError: # INSERT queries end up here.
            results = True
            pass
    return results

def check_table(pg, tablename, *fields, drop = False, r = False):
    """Check if a requested table exists and create it if needed.
    
    Note that this function alone is NOT SQL injection safe if the tablename
    is input by the user! This is because psycopg2 doesn't allow using table
    names as query params.
    
    An existing table is dropped only if explicitly requested; otherwise
    an error is raised instead.
    
    If r = True, adds region classification fields to the table.
    
    The field list for creating a new table is expected to be created
    by get_result_fields().
    """
    
    try:
        if drop:
            query = 'DROP TABLE IF EXISTS {}'.format(tablename)
            results = run_query(pg, query)
            if results:
                print('Attempted to drop the existing table "' + tablename + \
                      '" as requested...')
    except psycopg2.ProgrammingError:
        print('Failed to delete table ' + tablename + '!')
        raise
    try:
        fieldstr = '(' + fields[0] + ' text not null, ' + \
            fields[1] + ' integer not null, ' + \
            fields[2] + ' integer not null, '
        if r:
            fieldstr += ' integer not null, '.join(fields[3:6]) + \
                ' integer not null, '
            fieldstr += fields[6] + ' bigint not null, '
        fieldstr += ' integer not null, '.join(fields[3+int(r)*4:]) + \
            ' bigint not null)'
        query = 'CREATE TABLE ' + tablename + ' ' + fieldstr
        run_query(pg, query)
        return tablename
    except psycopg2.ProgrammingError:
        print('Failed to create the table ' + tablename + '!')
        raise

def get_region_ids(pg):
    """Fetch a list of subregion identifiers from the DB."""
    
    query = 'SELECT kokotun FROM hcr_subregions'
    res = run_query(pg, query)
    return res

def get_query_string(ttm_y,
                     msssuf_y,
                     aggregate,
                     ic_data_only = False,
                     region = None):
    """Build a query string for searching the data that should be aggregated.
    
    Takes the following arguments:
        ttm_y        = Year of the Travel Time Matrix (TTM).
        msssuf_y     = Year of the journey data to be joined with the TTM.
        aggregate    = The journey data field that is to be aggregated.
                       Note that this should be set together with the
                       ic_data_only option; otherwise, the results
                       won't make sense.
        ic_data_only = Whether the query criteria will be limited to only
                       cover the data that is classified by industry.
                       Default: False.
        'region'     = Whether the query criteria will be limited to only
                       cover the data of a certain sub-region.
                       Default: None (returns all data).
    """
    
    ttm_tuple = ttm_fields_tuple(ttm_y)
    ttmtbl = 'hcr_journeys_t_d_' + ttm_tuple[0]
    ttmtblpfx = ttmtbl + '.'
    sum_fields = ''
    for field in ttm_tuple[1]:
        sum_fields = sum_fields + 'SUM(' + ttmtblpfx + field + ' * ' + \
            aggregate + ') AS ' + field + ', '
    sum_fields = sum_fields[:-2]
    query_select_base = 'SELECT ' + sum_fields
    query_regions_fields = ''
    if region:
        query_regions_fields += ', s.kunta::int AS mun'
        query_regions_fields += ', s.suur::int AS area'
        query_regions_fields += ', s.tila::int AS dist'
        query_regions_fields += ', s.kokotun::bigint AS reg_id'
    query_join_base = ' FROM ' + ttmtbl + \
        ' INNER JOIN hcr_msssuf_journeys j ON ' + ttmtbl + \
            '.id = j.id AND ' + ttmtbl + '.vuosi = j.vuosi'
    query_join_reg = ''
    if region:
        query_join_reg = " AND j.a_region = '" + region + "'" + \
            ' INNER JOIN hcr_subregions s ON j.a_region = s.kokotun'
    query_where_base = ' WHERE ' + ttmtbl + '.vuosi = %s AND ' + \
        ttmtbl + '.sp = %s'
    query_where_ic = ''
    if ic_data_only:
        query_where_ic = ' AND j.yht >= 10'    
    query_regions_groupby = ''
    if region:
        query_regions_groupby = ' GROUP BY s.kokotun, s.tila, s.suur, s.kunta'
    sum_query = query_select_base + query_regions_fields + query_join_base + \
        query_join_reg + query_where_base + query_where_ic + \
        query_regions_groupby
    return sum_query

def build_tables(tablename,
                 ic_data_only = False,
                 regions = False,
                 drop = False):
    """Build the query result tables and write them to the DB.
    
       This function builds on almost all of the others; it reads
       data from the DB, aggregates it, creates a new DB table for
       the results and writes the results to the DB.
       
       Arguments:
           tablename    = Name of the result table to be created.
           ic_data_only = Whether to count only those results that are
                          classified by industry. Should not be used together
                          with regions.
           regions      = Count results by region. Should not be used
                          together with ic_data_only.
           drop         = If the tablename already exists, whether to drop
                          the existing table.
    """
    
    pg = get_db_conn()
    fields = get_result_fields(option = int(ic_data_only), regs = regions)
    reg_ids = list()
    if not (regions):
        reg_ids.append(None)
    else:
        regs = get_region_ids(pg)
        for reg in regs:
            reg_ids.append(reg[0])
    tablename = check_table(pg, tablename, *fields, drop = drop, r = regions)
    for reg in reg_ids:
        # for ttm in ttm_years():
        for ttm in [2013]: #DEBUG
            rowhead = ttm_fields_tuple(ttm)[1]
            # for year in journey_data_years():
            for year in [2012]: #DEBUG
                query_results = list()
                agg_fields = get_result_fields(option = int(ic_data_only) + 2)
                for aggregate in agg_fields:
                    query = get_query_string(ttm,
                                             year,
                                             aggregate,
                                             ic_data_only,
                                             reg)
                    params = (str(year), '0')
                    status = 'Aggregating field "' + aggregate + \
                        '" of the ' + str(year) + ' journey data with the ' + \
                            str(ttm) + ' TTM data...'
                    if regions:
                        status += ' region #' + reg
                    print(status)
                    query_results.append(run_query(pg, query, *params))
                rows = list()
                reg_fields = list()
                res = 0
                limit = 0
                if (query_results and query_results[0]):
                    limit = len(query_results[0][0])
                while res < limit:
                    row = list()
                    for agg_key, aggregate in enumerate(agg_fields):
                        row.append(query_results[agg_key][0][res])
                    rows.append(row)
                    res += 1
                if(regions):
                    for i in range(4):
                        if rows:
                            reg_fields.insert(0, int(rows.pop()[0]))
                if rows:
                    for key, res_item in enumerate(rows):
                        row = [rowhead[key], ttm, year]
                        if regions:
                            row += reg_fields
                        row += res_item
                        columns = ', '.join(fields)
                        params = ', '.join(['%s' for i in row])
                        insert = 'INSERT INTO {} ({}) VALUES ({})'.format(
                            tablename, columns, params)
                        run_query(pg, insert, *row)

class tablename_check(argparse.Action):
    """
    Define an argparse action for checking that the input tablename is sane.
    """
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
    """Main program for direct calls."""
    parser = argparse.ArgumentParser(
        description='Create Time Travel Matrix aggregate tables.')
    parser.add_argument('tablename', action=tablename_check, \
                        help='Output table name. Allowed characters ' + \
                        "are lowercase a-z, 0-9 and underscore ('_').")
    parser.add_argument('-d', '--drop', dest='drop', \
                        default=False, action='store_true', \
                            help='Drop existing table of the same name')
    parser.add_argument('-c', '--classified', dest='ic', \
                        default=False, action='store_true', help=\
                            'Only industry-classified journeys. ' + \
                            "Incompatible with '-r'")
    parser.add_argument('-r', '--regions', dest='regions', \
                        default=False, action='store_true', help=\
                            'Journey totals by subregion.\n' + \
                            "Incompatible with '-c'")
    try:
        args = parser.parse_args()
        if args.ic and args.regions:
            raise argparse.ArgumentTypeError(
                "Options '-c' and '-r' are mutually incompatible.")
    except argparse.ArgumentTypeError:
        raise
    build_tables(args.tablename,
                 ic_data_only = args.ic,
                 regions = args.regions,
                 drop = args.drop)
    
if __name__ == '__main__':
    main()