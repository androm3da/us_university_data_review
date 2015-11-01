#!/usr/bin/env python

import json
from csv import DictReader
from glob import glob
import os.path


def get_val(text):
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            if text == 'NULL':
                return None
            return text


def get_data_elements_types(fileobj):
    TYPES_INDEX = {
        'integer': int,
        'string': str,
        'float': float,
    }

    # These are fields where the data types appear to be inaccurate
    # according to the data dictionary
    TYPES_CORRECTIONS = {
        'ZIP': str,
        'DEP_INC_AVG': float,
        'IND_INC_AVG': float,
        'CUML_DEBT_P10': float,
        'CUML_DEBT_P25': float,
        'CUML_DEBT_P75': float,
        'CUML_DEBT_P90': float,
        'md_faminc': float,
        'lnfaminc': float,
        'lnfaminc_ind': float,
        'faminc': float,
        'median_hh_inc': float,
        'ln_median_hh_inc': float,
        'faminc_ind': float,
        'age_entry_sq': float,
        'age_entry': float,
        'fsend_count': float,
        'OPEID': str,
        # 'AccredAgency': lambda text: set([field.strip() for field in text.split('|')]),
        'AccredAgency': lambda text: [field.strip() for field in text.split('|')],
    }
    for data_elt in DictReader(fileobj):
        varname = data_elt['VARIABLE NAME']
        typename = data_elt['API data type']
        type_ = TYPES_INDEX.get(typename, None)
        type_ = TYPES_CORRECTIONS.get(varname, type_)
        yield varname, type_


def get_entries(entries_fileobj, dictionary_fileobj):
    datatypes = dict(get_data_elements_types(dictionary_fileobj))

    for entry in DictReader(entries_fileobj):
        for k, v in entry.items():
            if k not in datatypes:
                continue
            try:
                entry[k] = datatypes[k](v) if v not in ('NULL', 'PrivacySuppressed', ) else None
            except ValueError:
                print('err', k)
                raise
        yield entry


entries = {}
# TODO: safe to reuse dictionary among dumps?
with open('CollegeScorecard_Raw_Data/CollegeScorecardDataDictionary-09-12-2015.csv',  'rt') as dict_f:
    for merged in ('CollegeScorecard_Raw_Data/MERGED2001_PP.csv', 'CollegeScorecard_Raw_Data/MERGED2012_PP.csv', ):
        with open(merged, 'rt') as data_f:
            entries[os.path.basename(merged)] = list(get_entries(data_f, dict_f))

with open('college_scorecard.json', 'wt') as f:
    json.dump(entries, f, indent=4)
