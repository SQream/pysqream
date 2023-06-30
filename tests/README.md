# Tests

## Testing

### Must know

New database is created for each pytest run (for all tests) with default name
`test_pysqream_testrun_worker_master`, thus providing clean environment and 
does not affect existing databases.

> other names for running pytest in parallel see [Testing in Parallel](#testing-in-parallel)

If DB has already existed, then it would produce error before tests runs
To recreate testing DB - use `--recreate` flag:
`pytest -svvx --recreate`

### Prepare

Installation of the package (in editable mode for developing) while at root folder of the repository
`pip3 install --editable .`

pytest installation
`pip install pytest`

### Configuration

Pytest has a lot of configuration that helps to avoid coding too much
for that it uses pytest.ini file in root folder, that has already include
some values, please revise it

### Running the tests

From the root folder of the repository
`pytest tests/`

Or with test specified:
`pytest tests/dbapi_test.py`
> python3 -m pytest also could be used

#### Logging

For showing pretty log use pytest parameter:
`pytest tests/ --log-cli-level=<desired_level>`

desired level could be one of the python standard's:

* CRITICAL
* ERROR
* WARNING
* INFO
* DEBUG
* NOTSET

##### To file

To specify file for logging use `--log-file` parameter:
`pytest tests/ --log-file=test_log`
To set the logging level in file there is `--log-file-level`
So all logging information would be passed to file
`--log-cli-level`, `--log-file-level` and `--log-file` could be used together and then logging info
would be written in both a command line and a file

##### Format

To format logging output there are parameters:

* `--log-cli-format` for formatting logging in command line
* `--log-file-format` for formatting logging in file

##### Example

`pytest tests/dbapi_test.py::TestPositive::test_nulls  --log-cli-level=WARNING --log-file=log.txt --log-file-format="%(asctime)s - %(name)s - %(levelname)s - %(message)s" --log-file-level=NOSET`
This will run with providing warning (and more severe) logging output to command lime with default formatting and all levels to the file log.txt
with formatting "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

Some logging implementation was in `tests/pytest_logger.py` and they included this params:
`--log-file=log.txt --log-file-format="%(asctime)s - %(name)s - %(levelname)s - %(message)s" --log-file-level=NOSET`
They had been added to pytest.ini, so they are default now, but could be changed by direct passing.

##### Few words about testing packages

Pytest uses `prepend` import mode by default, but it does not allow to
import module as is, so mode changed in pytest.ini to recommended `importlib`,
which allows to use native python imports and does not affect PATH

## Testing in Parallel

There are a lot of integration tests without mocking, for increasing their testing speed,
there is some configuration that allows to use `pytest-xdist` for parallel testing out of box.

To do that just install:
`pip3 install pytest-xdist`
And add flag to use it:
`pytest -svvx --recreate -n auto`, where auto could be replaced with a number of cores
While working with xdist:
* logs are saved in separate files for each worker with the same path and basename
* Separated databases are created for each worker `test_pysqream_testrun_worker_gw{n}`, where n - from 0 to core_number-1

> Without xdist flags such as `-n`, it would be run as normal pytest. 

### Issues with internal server error

There might be error while testing with pytest-xdist from local machine outside the infrastructure, while DB is inside
In that case turning of Ping helped

> To turn-off make `_start_ping_loop` do nothing 

## Test Coverage

Using `coverage` package:

### Prepare coverage

`pip3 install coverage`

### Running coverage

To run the test from the test folder, saving the html report in a folder `test_report`:

`coverage run pysqream_tests.py  && coverage html --include='../pysqream/dbapi.py' -d test_report`

To see the report with your web browser:

`firefox test_report/index.html`

### Checking for uncovered code in new commit / branch

Using `diff-cover` package:

`pip3 install diff_cover`

Run the coverage test on your code and generate an xml report

`coverage run pysqream_tests.py  && coverage xml`

Then generate html of new uncovered code:

`diff-cover coverage.xml --html-report commit_coverage.html && firefox commit_coverage.html`

To know if any code is uncovered in the new commit after running the above:

`python3 is_commit_covered.py`


# Refactoring design

To make testing much efficient it is advisable to split unit tests and
integration test by folder (optionally by pytest.mark also)

## Unit testing

Make unit tests works only with small parts of code such a function or method,
while mocking other responses by monkeypatch. For example, mock socket.socket.recv_into(bytes)
so it would return data from generator providing bytest those server would provide.

Set up Github Actions for checking unit-tests on PR, thus avoid broken code to be merged.

## Integration testing

Split them into few parts:
1. Where checks integration of different parts of package with mocked server response. (Inner integration tests)
2. Where checks everything work with real server by providing IP & ports. (Outer integration tests)

Check coverage for unit testing, inner integration test and outer integration tests separately
to fully cover everything.

### Inner integration tests

Check that different parts works together.

### Outer integration tests

For example, as it is now, when all the data fetch and insertion is checked by cursor calls.
