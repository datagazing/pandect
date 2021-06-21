"""
Simple wrappers to load and convert common data file types

Features
--------

* Uses file extension as heuristic to determine input format
* Provides metadata using pyreadstat objects when appropriate
* Supports: csv, tsv, xlsx, sav, dta (unreliable), sqlite3
* Loads data into pandas.DataFrame
* Provides command line utilities: sav2dta, pandect

Examples
--------

Load a data file into a pandas.DataFrame object:

.. code-block:: python

  >>> import pandect
  >>> data, meta = pandect.load(input_file_name)

Save a pandas.DataFrame object as a data file:

.. code-block:: python

  >>> import pandas
  >>> import pandect
  >>> data = pandas.DataFrame([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])
  >>> pandect.save(data=data, output='out.sav')
  >>>

Save a pandas.DataFrame object as a data file with metadata:

.. code-block:: python

  >>> import pandas
  >>> import pandect
  >>> import pyreadstat
  >>> data = pandas.DataFrame([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])
  >>> meta = pyreadstat.metadata_container()
  >>> meta.column_names_to_labels = {'a': 'A Label', 'b': 'B Label'}
  >>> pandect.save(data=data, output='out.sav', meta=meta)
  >>>

Convert a data file at the command line:

.. code-block:: console

  $ pandect input.csv output.dta

Convert sav data file to dta data file at the command line:

.. code-block:: console

  $ sav2dta some_file.sav

* Derives output file name from input file name (here: `some_file.dta`)
* This is a convenience utility for a common task
* It is basically a special case of the `pandect` utility

Limitations
-----------

* Loading dta files is unreliable (bug in pyreadstat, might segfault)

License
-------

* Free software: MIT license

Documentation
-------------

* https://pandect.readthedocs.io/
"""

__author__ = """Brendan Strejcek"""
__email__ = 'brendan@datagazing.com'
__version__ = '0.3.2'

from .pandect import load, save, pandect, sav2dta # noqa F401
from .pandect import Error # noqa F401
from .pandect import UnknownInputFormat # noqa F401
from .pandect import UnknownOutputFormat # noqa F401
