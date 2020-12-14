SciDB-Bridge: Python Library to access externally stored SciDB data
===================================================================

.. image:: https://img.shields.io/badge/SciDB-19.11-blue.svg
    :target: https://forum.paradigm4.com/t/scidb-release-19-11/2411

.. image:: https://img.shields.io/badge/arrow-0.16.0-blue.svg
    :target: https://arrow.apache.org/release/0.16.0.html


Requirements
------------

- Python ``2.7.x``, ``3.5.x``, ``3.6.x``, ``3.7.x``, ``3.8.x``, or newer
- SciDB ``19.11`` or newer
- SciDB-Py ``19.11.2`` or newer
- Apache PyArrow ``0.16.0``
- Boto3 ``1.14.16`` for Amazon Simple Storage Service (S3) support


Installation
------------

Install latest release::

  pip install scidb-bridge

Install development version from GitHub::

  pip install git+http://github.com/paradigm4/bridge.git#subdirectory=py_pkg
