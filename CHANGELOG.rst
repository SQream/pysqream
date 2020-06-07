Ver. 3.0.3
------------
*07/06/2020*

* Fixed bad error message on timeout
* A few adaptations to avoid memory buildup in large ETLs


Ver. 3.0.2
------------
*10/04/2020*

* Fixed bug in which a faulty statement would silently fail with `execute` and `executemany`

Ver. 3.0.1
-------------
*09/01/2020*

* Returned support for Python 3.6
* Fixed minor packaging issue with README opening as UTF-8

Ver. 3.0.0
----------
*05/01/2020*

* Connector rewritten to be DB-API complaint
* Minimal supported Python version 3.7
* Tests: updated to run on Windows, and accept sqreamd from command line


Ver. 2.1.4
----------
*17/07/2019*

* SQream DB Protocol 7 support
* Varchar encoding support for Thai (windows-874)

Ver. 2.1.3
----------
*28/06/2019*

* Fetch all as dictionary
* Bring repo up to PyPi standards


Ver. 2.1.1
----------
*30/01/2019*

* Python 3 fixes - tested on Python 3.6
* Bad message and clustered connection fixes

Ver. 2.1.1
----------
*07/08/2018*

* Backward protocol support - Connector now works with all SQream versions with protocols 4-6


Ver. 2.1.0
----------
*09/07/2018*

* Support for Sqream protocol v6


Ver. 2.0.2
----------
*13/05/2018*

* Connector refactored to parse metadata without using external queries. A few internal workflows simplified


Ver. 2.0.1
----------
*25/04/2018*

* 2 function renames to match the official API

Ver. 2.0
----------
*18/02/2018*

* Support for the official SQream API
* Support for SQream protocol version 5 (Back compatible with 4)
* Netwrok insert (Part of the API)
* Preparation for SSL support
* Other improvements and fixes coming from 1.5

