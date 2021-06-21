=======
pandect
=======


.. image:: https://img.shields.io/pypi/v/pandect.svg
        :target: https://pypi.python.org/pypi/pandect

.. image:: https://img.shields.io/travis/datagazing/pandect.svg
        :target: https://travis-ci.com/datagazing/pandect

.. image:: https://readthedocs.org/projects/pandect/badge/?version=latest
        :target: https://pandect.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status



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

Convert sav data file to dta data file:

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


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
