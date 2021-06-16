#!/usr/bin/env python3

import logging
import os
import re
import sys
import typing

myself = pathlib.Path(__file__).stem

# configure library-specific logger
logger = logging.getLogger(myself)
logging.getLogger(myself).addHandler(logging.NullHandler())

import attrs
import pandas
import pyreadstat

import optini

########################################################################

def expand_path(x):
    """Expand ~ and environment variables in paths"""
    x = os.path.expandvars(os.path.expanduser(x))
    logger.debug(f"expanded: {x}")
    return x

########################################################################

@attr.s(auto_attribs=True)
class Pandect:
    """
    """
    source: str
    sep: str = ','
    expand: bool = True,
    flags: enum = re.IGNORECASE
    table: str = None

    def __attrs_post_init__(self):
        """Constructor"""
        self._data, self._meta = load(source,
            sep=',',
            expand=True,
            flags=re.IGNORECASE,
            table=None,
        )
        logger.debug(f"loaded data")

    def save(self, output):
        save(self._data, output, self._meta)

def save(data, output, meta=None, flags=re.IGNORECASE):
    """
    """
    #column_names_to_labels
    if re.search('\.csv$', output, flags):
        data.to_csv(output, sep=',')
    elif re.search('\.tsv$', output, flags):
        data.to_csv(output, sep='\t')
    elif re.search('\.xlsx$', output, flags):
        data.to_excel(output)
    elif re.search('\.sav$', output, flags):
        #pyreadstat.write_sav(data, output, column_labels=names)
        pyreadstat.write_sav(data, output)
    elif re.search('\.dta$', output, flags):
        pyreadstat.write_dta(data, output)
    else:
        logger.error(f"unknown output format: {output}")
        return
    logger.info(f"wrote {output}")

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

########################################################################

def sav2dta():
    """Entry point for sav2dta command line script"""
    optini.Config(appname='sav2dta', io=True)
    data, meta = load(optini.opt.input)
    save(data, optini.opt.output)
