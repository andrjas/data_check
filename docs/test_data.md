# Test data

`data_check fake fake_config.yml` is used to generate test data for a table from a configuration file.
The data type for each column is deferred from the table and can be changed in the configuration.

See [Usage](usage.md#fake) for command line options.

Test data generation is done using [Faker](https://github.com/joke2k/faker).


## Example

The minimal configuration only names the table for test data generation:

```yaml
table: main.simple_table
```

When running `data_check fake fake_config.yml` the column definition from the table is read from the database and a CSV file _main.simple\_table.csv_ is generated with some data for the table (100 rows by default). The CSV file can be used to [load](usage.md#load) the data back into the table.

A more complete configuration looks like this and is described in the following:

```yaml
table: main.simple_table
business_key: # the key that should not change between iterations; must not be null
  - bkey
rows: 200  # how many rows to generate

iterations:  # generate data with same business_key with some variation
  count: 5  # how many iterations to generate

columns:
  bkey:
    faker: iban

  date_col:
    add_values:  # also use these value
      - 1900-01-01
      - 9999-12-31
  col2:
    faker: name # faker provider method, if not correctly inferred
  col3:
    from_query: select colx from main.other_table  # use values from the query
    next: inc  # "algorithm" for next iteration
  col4:
    next: random
    values:  # use these values randomly
      - 1
      - 2
      - null
```


## Test data configuration

The configuration is a YAML file for a single table. The top level elements are:

### table

_table_ tells data_check for which table to generate the data.

Example:
```yaml
table: main.simple_table
```

### business_key

_business\_key_ is a list of columns that are unique and do not change between iterations.

Example:
```yaml
business_key:
  - column_1
  - column_2
```

### iterations

_iterations_ configures how many iterations will be generated. Each iteration is a single CSV file with the same business keys. Each column can define a [next](#next) algorithm how the data should change between iterations.

Example:
```yaml
iterations:
  count: 5
```


### columns

_columns_ defines a [configuration](#column-configuration) for each column. If a column is not listed here, a default configuration will be used based on the data type.

Example:

```yaml
columns:
  column_1:
    faker: iban
  column_2:
    from_query: select colx from main.other_table
```

### column configuration

Each column can have multiple configurations:

#### faker

_faker_ defines the [provider](https://faker.readthedocs.io/en/master/providers.html) used to generate the data. If not given, a default provider is used based on the data type of the column.

```yaml
faker: name
```

| data type | default provider  |
| --------- | ----------------- |
| decimal   | [pydecimal](https://faker.readthedocs.io/en/master/providers/faker.providers.python.html#faker.providers.python.Provider.pydecimal) |
| varchar   | [pystr](https://faker.readthedocs.io/en/master/providers/faker.providers.python.html#faker.providers.python.Provider.pystr)     |
| date      | [date_between](https://faker.readthedocs.io/en/master/providers/faker.providers.date_time.html#faker.providers.date_time.Provider.date_between)      |
| datetime  | [date_time_between](https://faker.readthedocs.io/en/master/providers/faker.providers.date_time.html#faker.providers.date_time.Provider.date_time_between) |

#### faker_args:

_faker\_args_ defines a map or arguments that are passed to the [provider](https://faker.readthedocs.io/en/master/providers.html). Each provider can define different arguments.

Example:
```yaml
faker: date_between
faker_args:
  start_date: 1900-01-01
  end_date: 2030-12-31
```

#### from_query

_from\_query_ defines a SQL query. The values of the query are used randomly to generate the data.
If _from\_query_ is given, _faker_ is ignored.

```yaml
from_query: select column from other_table
```

#### values

_value_ defines a list of values that are used randomly to generate the data.
If _values_ is given, _from\_query_ and _faker_ are ignored.

```yaml
values:
  - 1
  - 2
  - null
```

#### add_values

_add\_values_ is used to add some specific values that are additionally used to generate the data. _add\_values_ can be used with _faker_, _from\_query_ and _values_. Each value in _add\_values_ has the same probability to occur as all the other generated values combined.


```yaml
add_values:
  - 9999-12-31
  - 1900-01-01
```

#### next

_next_ defines an algorithm for the iterator. If _next_ is not given then the column is not changed between iterations.

```yaml
next: inc
```

Possible values for _next_:

* _inc_: increment by 1 (day for date)
* _dec_: decrement by 1 (day for date)
* _random_: generate a random value from the configured faker/values

