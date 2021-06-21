#!/usr/bin/env python3

"""
See top level package docstring for documentation
"""

import logging
import os
import pathlib
import re
import sqlite3
import sys

import pandas
import pyreadstat

import optini

myself = pathlib.Path(__file__).stem

logger = logging.getLogger(myself)
logging.getLogger(myself).addHandler(logging.NullHandler())

########################################################################

# exceptions


class Error(Exception):
    pass


class UnknownInputFormat(Error):
    def __init__(self, x):
        self.message = f"unknown input format: {x}"


class UnknownOutputFormat(Error):
    def __init__(self, x):
        self.message = f"unknown output format: {x}"


########################################################################

# helper functions


def expand_path(x):
    """Expand ~ and environment variables in paths"""
    x = os.path.expandvars(os.path.expanduser(x))
    logger.debug(f"expanded: {x}")
    return x


########################################################################


def save(data, output, meta=None, flags=re.IGNORECASE, version=None):
    """
    Procedure to save data frame with optional metadata to file

    Parameters
    ----------
    data : pandas.DataFrame
        Data
    output : str
        Output file name
    meta : pyreadstat.metadata_container, default=None
        Metadata
    flags : re.RegexFlag, default=re.IGNORECASE
        Regex match flags (relevant to determining file types)
    version : int, default=None
        Output format version (applies only to dta output currently)

    Raises
    ------
    UnknownOutputFormat
    """
    names = None
    if meta is not None:
        names = meta.column_names_to_labels

    if re.search(r'\.csv$', output, flags):
        data.to_csv(output, sep=',')
    elif re.search(r'\.tsv$', output, flags):
        data.to_csv(output, sep='\t')
    elif re.search(r'\.xlsx$', output, flags):
        data.to_excel(output, index=False)
    elif re.search(r'\.sav$', output, flags):
        pyreadstat.write_sav(data, output, column_labels=names)
    elif re.search(r'\.dta$', output, flags):
        version = version if version else 14
        pyreadstat.write_dta(
            data,
            output,
            version=version,
            column_labels=names,
        )
    else:
        logger.error(f"unknown output format: {output}")
        raise UnknownOutputFormat(output)
    logger.info(f"wrote {output}")


def load(source, sep=',', expand=True, flags=re.IGNORECASE, table=None):
    """
    Function to load dataset into pandas.DataFrame object

    - Uses file extension as heuristic to determine input format
    - Supports: csv, tsv, xlsx, sav, dta, sqlite3
    - Preserve metadata to the degree possible

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
    """

    meta = pyreadstat.metadata_container()

    if type(source) is str:
        logging.info(f"data source: {source}")

        if expand:
            source = expand_path(source)
        if not os.path.exists(source):
            logging.error(f"file not found: {source}")
            raise FileNotFoundError(source)

        if re.search(r'\.csv$', source, flags):
            data = pandas.read_csv(source, sep=sep)
        elif re.search(r'\.tsv$', source, flags):
            data = pandas.read_csv(source, sep='\t')
        elif re.search(r'\.xlsx$', source, flags):
            data = pandas.read_excel(source)
        elif re.search(r'\.sav$', source, flags):
            data, meta = pyreadstat.read_sav(source)
        elif re.search(r'\.dta$', source, flags):
            # logging.warning("loading dta files is known to cause segfaults")
            data, meta = pyreadstat.read_dta(source)
        elif re.search(r'\.sqlite3$', source, flags):
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
            raise UnknownInputFormat(message)
    else:
        message = f"unrecognized data source {source}"
        logging.error(message)
        raise UnknownInputFormat(message)

    vars = list(data)
    logging.info('loaded data')
    logging.info(f"number of variables: {len(vars)}")
    logging.info(f"observations: {len(data)}")
    return(data, meta)


########################################################################

# helper functions for command line utilities


def _arg2input():
    """Return input file name from -i or first unparsed argument"""

    if optini.opt.input is not None:
        return optini.opt.input
    elif len(optini.opt._unparsed) > 0:
        logger.debug('-i not specified, using first unparsed argument')
        return optini.opt._unparsed[0]
    else:
        logger.error('no input found; try -i <input>')
        logger.error('aborting')
        sys.exit(1)


def _arg2output():
    """Return output file name from -o or secend unparsed argument"""

    if optini.opt.output is not None:
        return optini.opt.output
    elif len(optini.opt._unparsed) > 1:
        logger.debug('-o not specified, using second unparsed argument')
        return optini.opt._unparsed[1]
    else:
        logger.error('no output found; try -o <output>')
        logger.error('aborting')
        sys.exit(1)


########################################################################

# command line utilities
# flit packages these functions as separate scripts


def sav2dta():
    """Entry point for sav2dta command line script"""
    desc = 'Convert sav data file to dta data file'
    optini.spec.input.help = 'input sav file'
    optini.spec.input.type = str
    optini.Config(appname='sav2dta', desc=desc, logging=True)
    logger.debug(f"unparsed = {optini.opt._unparsed}")
    input = _arg2input()
    output = f"{pathlib.Path(input).stem}.dta"
    logger.debug(f"input = {input}")
    logger.debug(f"output = {output}")
    try:
        data, meta = load(input)
        save(data=data, output=output, meta=meta)
    except (FileNotFoundError, Error):
        sys.exit(1)


def pandect():
    """Entry point for pandect command line script"""
    desc = 'Convert data file between formats (csv, tsv, xlsx, sav, dta)'
    optini.spec.input.help = 'input file'
    optini.spec.input.type = str
    optini.spec.output.help = 'output file'
    optini.spec.output.type = str
    optini.Config(appname='pandect', desc=desc, logging=True)
    input = _arg2input()
    output = _arg2output()
    logger.debug(f"input = {input}")
    logger.debug(f"output = {output}")
    try:
        data, meta = load(input)
        save(data=data, output=output, meta=meta)
    except (FileNotFoundError, Error):
        sys.exit(1)


########################################################################

if __name__ == '__main__':
    # default to the general converter
    pandect()
