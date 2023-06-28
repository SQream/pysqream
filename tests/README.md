# Tests

## Testing

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