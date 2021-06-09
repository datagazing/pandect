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




Simple wrapper to load dataset files into pandas.DataFrame objects


* Free software: MIT license
* Documentation: https://pandect.readthedocs.io.


Features
--------

* Uses file extension as heuristic to determine input format
* Provides metadata using pyreadstat objects when appropriate
* Supports: csv, tsv, xlsx, sav, dta (unreliable), sqlite3 

Examples
--------

.. code-block:: python

  import pandect
  data, meta = pandect.load(input_file_name)

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
