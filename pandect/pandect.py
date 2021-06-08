#!/usr/bin/env python3

import logging
import os
import re
import sys

import pandas
import pyreadstat

########################################################################

def expand_path(x):
    """Helper function to expand ~ and environment variables in paths"""
    x = os.path.expandvars(os.path.expanduser(x))
    logging.debug(f"expanded: {x}")
    return x

def load(source,
    sep=',',
    expand=True,
    flags=re.IGNORECASE,
    table=None,
):
    """Load dataset into pandas.DataFrame object

    Uses file extension as heuristic to determine input format.

    Supports: csv, tsv, xlsx, sav, dta (unreliable), sqlite3

    Parameters
    ----------
    sep : str
        Separator used by csv
    expand : true
        Expand ~ and environment variables in path strings
    flags : re.RegexFlag
        Regular expression flags for matching file name extensions
    table : str
        Name of table to load (needed for some database input sources)

    Returns
    -------
    data : pandas.DataFrame
        DataFrame object
    meta : pyreadstat.metadata_container
        Metadata (empty if not provided by data source)

    Raises
    ------
    FileNotFoundError
    IOError

    Notes
    -----
    Loading dta files is unreliable (bug in pyreadstat, might segfault)

    Metadata Objects
    ----------------

    Incomplete list of metadata:

    - column_names : list with the names of the columns
    - column_labels : list with the column labels, if any
    - column_names_to_labels : dict{column_names: column_labels}
    - variable_value_labels : dict{variable_names: dict}
    - variable_to_label : dict{variable_names: label_name}
    - value_labels : dict{label_name: dict}
    - variable_measure : nominal, ordinal, scale or unknown

    See the pyreadstat web docs for complete spec.
    """

    meta = pyreadstat.metadata_container()

    if type(source) is str:
        logging.info(f"data source: {source}")

        if expand:
            source = expand_path(source)
        if not os.path.exists(source):
            logging.error(f"file not found: {source}")
            raise FileNotFoundError(source)

        if re.search('\.csv$', source, flags):
            data = pandas.read_csv(source, sep=sep)
        elif re.search('\.tsv$', source, flags):
            data = pandas.read_csv(source, sep='\t')
        elif re.search('\.xlsx$', source, flags):
            data = pandas.read_excel(source)
        elif re.search('\.sav$', source, flags):
            data, meta = pyreadstat.read_sav(source)
        elif re.search('\.dta$', source, flags):
            logging.warning("loading dta files known to cause segfaults")
            data, meta = pyreadstat.read_dta(source)
        elif re.search('\.sqlite3$', source, flags):
            if table is None:
                message = "missing table specification for sqlite"
                logging.error(message)
                raise IOError(message)
            connection = sqlite3.connect(source)
            query = "SELECT * FROM %s" % (table)
            data = pandas.read_sql_query(query, connection)
        else:
            message = f"unrecognized file type {source}"
            logging.error(message)
            raise IOError(message)
    else:
        message = f"unrecognized data source {source}"
        logging.error(message)
        raise IOError(message)

    vars = list(data)
    logging.info('loaded data')
    logging.info(f"number of variables: {len(vars)}")
    logging.info(f"observations: {len(data)}")
    return(data, meta)
