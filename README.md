
# About Pramen
[![Build](https://github.com/AbsaOSS/pramen/workflows/Build/badge.svg)](https://github.com/AbsaOSS/pramen/actions)

Pramen is a framework for defining data pipelines based on Spark and a configuration driven tool to run and
coordinate those pipelines. The project focuses around Hadoop and Spark, but can run arbitrary jobs.

The idea behind Pramen pipelines is simple. A pipeline consists of
* _Sources_ are the data systems that are not managed by the pipeline. An example could be an operational relational database.
  - Ingestion jobs are used to get data from external systems into the metastore.
* _Metastore_ is the data storage managed by the pipeline. Data in the metastore is accessed by table names.
  The metastore hides the underlying storage and format, which is usually Parquet or Delta on HDFS or S3.
  - Transformation jobs are used to transform data from the metastore and save the results back to the metastore.
    Transformers can be written in Scala or in Python.
* _Sinks_ are targets to send data from the metastore. An example could be a Kafka cluster or a CSV file in a local folder.
  - Sink jobs are used to send data from the metastore to sinks

The architecture is customizable, so you can define your own sources, transformers and sinks and deploy it independently
from the framework.

![](resources/concepts.png)

With Pramen you can:
* Build a data lake for tabular data.
  - Define ingestion jobs to get data from external data sources to HDFS or S3.
  - Organize data by partitioning it according to event or snapshot date.
* Create ETL data pipelines
  - Define ingestion jobs to _extract_ data from external sources to the metastore.
  - Use transformers _transform_ data inside the metastore.
  - Use sinks to _load_ data from the metastore to the target system.
* Create ML pipelines
  - Define ingestion jobs to get raw data to the metastore.
  - Use transformers to clean, aggregate and extract features from the raw data  in the metastore.
  - Use sinks to train and deploy models or to send data from the metastore to target systems.

There are many other data pipeline management tools. Why you would want to use Pramen?

* Declarative pipeline definitions
  - You define dependencies for transformers and Pramen will resolve them for you making sure a transformation
    runs only when all dependencies are satisfied.
* Auto-healing as much as possible
  - Keeping pipeline state allows quicker recovery from a faulty source or transformation since the framework will
    automatically determine which jobs to run. Jobs that already succeeded won't run again by default.
  - Handling of late data and retrospective updates to data in data sources by re-running dependent jobs.
  - Handling of schema changes from data sources.
* Functional design
  - The discipline on restricting mutable operations allows re-playable deterministic pipelines.
  - Easier to test individual transformers.
* Language support
  - You can use Scala and Python transformers and combine them.
* Extendable
  - If your data source or sink is not supported by Pramen yet? You can implement your own very easy.
* Builtin support of various relational database sources
  - Pramen already supports getting data from the following RDMS: PostgreSQL, Oracle Data Warehouse, Microsoft SQL Server,
    Denodo Virtualized and other standard JDBC compliant data sources 
    
# Typical Use Case and Benefits

Many environments still have numerous heterogeneous data sources that aren't integrated into a central data lake environment.

Pramen provides the ability to ingest and manage data pipelines en-masse from sourcing to producing.

Pramen assists with simplifying the efforts of ingestion and orchestration to a "no/low-code" level:
 - Automatic data loading and recovery (including missed and late data sources)
 - Automatic data reloading (partial or incorrect data load)
 - Automatic orchestration and coordination of dependant jobs (re-run downstream Pramen jobs automatically when upstream jobs are re-executed)

In addition to basic error notification, typical operational warnings are generated through email notifications such as:
 - Changes to upstream schema (unexpected changes to source data schemas) 
 - Sourcing performance thresholds (unexpected slower than expected data throughput)

**With Pramen data engineers and data scientists may focus on development and worry less about monitoring and maintaining existing data and machine learning pipelines.**

# Quick start

1. Get Pramen pipeline runner:

   You can download Pramen from Maven by following link: [Pramen]()

   Or you can build it from source by running:
   ```sh
   git clone https://github.com/AbsaOSS/pramen
   mvn clean package
   ```
   (You need JDK 1.8 and Maven installed to run this)

2. Define an ingestion pipeline

   Paste the contents of [ingestion_pipeline.conf](examples/ingestion_pipeline/ingestion_pipeline.conf)
   to a local file.

3. Run the pipeline. Depending on the environment the command may vary. Here is an example for Yarn:
   ```sh
   spark-submit --master yarn \
     --deploy-mode client \
     --num-executors 1 \
     --driver-memory 2g \
     --executor-memory 2g \
     --class za.co.absa.pramen.runner.PipelineRunner \
     pipeline-runner-0.12.10.jar \
     --workflow ingestion_pipeline.conf \
     --rerun 2022-01-01
   ```

# Building a data pipeline

Let's take a look on components of a data pipeline in more detail.

## Pipeline components

A pipeline consists of _sources_, _the metastore_ and _sinks_.

Currently there are 3 types of jobs:
- _Ingestion_ jobs to get data from external sources to the metastore.
- _Transformation jobs_ to transform data inside the metastore.
- _Sink_ jobs to send data from the metastore to external systems.

### Metastore
A metastore helps to abstract away the tabular data and the underlying storage. The idea is simple: a data pipeline 
designer can choose different ways of storing data, but implementers of transformations don't need to worry about it
since they can access data by table names.

#### How does this work? 

Pramen can store data in folders of file systems supported by Hadoop (HDFS, S3, AzureFS,
etc.) in Parquet or Delta format, or as tables in DeltaLake. But implementers of transformations do not need to worry
about the underlying storage. They can access it using `getTable()` method of a metastore object provided to them. The
framework will provide them with a Spark DataFrame. 

> The advantage of such approach is that transformations are storage agnostic and can be migrated from one storage
> system / data format to another seamlessly.

#### Defining a metastore
A metastore is simply a mapping from a _table name_ to a _path_ where the data is stored.

##### Storage types
Currently, the following underlying storage is supported. 
- Parquet files in Hdfs
- Delta files in Hdfs
- DeltaLake tables

Here is an example of a metastore configuration with a single table (Parquet format):
```config
pramen.metastore {
  tables = [
  {  
    name = "table_name"
    format = "parquet"
    path = "hdfs://cluster/path/to/parquet/folder"
    records.per.partition = 1000000
    information.date.column = "INFORMATION_DATE"
    information.date.format = "yyyy-MM-dd"
    information.date.start = "2022-01-01"
  }
]
```

Metastore table options:

| Name                         | Description                                                                       |
|------------------------------|-----------------------------------------------------------------------------------|
| `name`                       | Name of the metastore table                                                       |
| `format`                     | Storage format (`parquet` or `delta`)                                             |
| `path`                       | Path to the data in the metastore.                                                |
| `table`                      | DataLake table name (if DataLake tables are the underlying storage).              |
| `records.per.partition`      | Number of records per partition (in order to avoid small files problem).          |
| `information.date.column`    | Name of the column that contains the information date. *                          |
| `information.date.format`    | Format of the information date used for partitioning (in Java format notation). * |
| `information.date.start`     | The earliest date the table contains data for. *                                  |

`*` - It is recommended to standardize information date column used for partitioning folders in the metastore. You can
define default values for the information date column at the top of configuration and it will be used by default if not
overridden explicitly for a metastore table.  

Default information date settings can be set using the following configuration keys:

| Name                             | Default value     | Description                                        |
|----------------------------------|-------------------|----------------------------------------------------|
| `pramen.information.date.column` | pramen_info_date  | Default information date column name.              |
| `pramen.information.date.format` | yyyy-MM-dd        | Default information date format.                   |
| `pramen.information.date.start`  | 2020-01-01        | Default starting date for tables in the metastore. |

Storage type examples:

<details>
  <summary>Click to expand</summary>

  A config for a Parquet folder example:
  ```config
  {
    name = "table_name"
    format = "parquet"
    path = "hdfs://cluster/path/to/parquet/folder"
    records.per.partition = 1000000
  }
  ```
  
  A config for a Delta folder example:
  ```config
  {
    name = "table_name"
    format = "delta"
    path = "s3://cluster/path/to/delta/folder"
    records.per.partition = 1000000
  }
  ```
  
  A config for a DeltaLake table example:
  ```config
  {
    name = "table_name"
    format = "delta"
    table = "delta_lake_table_name"
    records.per.partition = 1000000
  }
  ```
</details>

### Sources

Sources define endpoints and paths go get data into the pipeline. Currently, Pramen supports the following
builtin sources:

- **JDBC source** - allows fetching data from a relational database. The following RDBMS dialects are supported at
  the moment:
   - PostgreSQL
   - Oracle (a JDBC driver should be provided in the classpath)
   - Microsoft SQL Server
   - Denodo (a JDBC driver should be provided in the classpath)
   - Hive 1/2
- **Parquet on Hadoop** - allows fetching data in Parquet format from any Hadoop-compatible store: HDFS, S3, etc.

You can define your own source by implementing the corresponding interface.

Sources are defined like this:
```config
pramen.sources = [
  {
    # The name of the source. It will be used to refer to the source in the pipeline.
    name = "source1_name"
    # The factory class of the source determines the source type.
    factory.class = "za.co.absa.pramen.core.source.JdbcSource"
    
    # Depending of the factory source parameters vary.
  },
  {
    name = "source2_name"
    factory.class = "za.co.absa.pramen.core.source.ParquetSource"
    # ...
  }
  ## etc
]
```

Builtin sources:

| Factory Class                                 | Description               |
|-----------------------------------------------|---------------------------|
| `za.co.absa.pramen.core.source.JdbcSource`    | JDBC Source               |
| `za.co.absa.pramen.core.source.ParquetSource` | Parquet on Hadoop source. |

Here is how each of these sources can be configured:

#### JDBC source
Here is how you can configure a JDBC source. The source defines an end point. Which exact table to load
is determined by the pipeline configuration.
```config
{
    name = "source1_name"
    factory.class = "za.co.absa.pramen.core.source.JdbcSource"

    jdbc = {
      # Driver fully qualified class
      driver = "org.postgresql.Driver"
      
      # The connection URL 
      url = "jdbc:postgresql://example1.com:5432/test_db"
      
      # Authentication credentials
      user = "my_login"
      password = "some_password"
      
      # Any option passed as 'option.' will be passed to the JDBC driver. Example:
      #option.database = "test_db"
    }

    # Any option passed as '.option' here will be passed to the Spark reader as options. For example,
    # the following options increase the number of records Spark is going to fetch per batch increasing
    # the throughut of the sourcing.
    option.fetchsize = 50000
    option.batchsize = 50000

    # Specifies if tables of the data source have an information date colunn
    has.information.date.column = true
    
    # If information column is present, specify its parameters:
    information.date {
      column = "info_date"
      # Column format. Can be one of: "date", "string", "number", "datetime"
      date.type = "date"
      
      # The format of the information date. If date.type = "date" the format is usually:
      date.app.format = "yyyy-MM-dd"
      
      # When date.type = "number" the format is usually:
      #date.app.format = "yyyyMMdd"
      
      # When date.type = "string" the format may vary significantly 
      # The format should be specified according to `java.time` spec:
      # https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
    }

    # Convert decimals with no scale to integers and longs, fix 'NUMBER' SQL to Spark mapping. 
    correct.decimals.in.schema = true
    
    # Fix the input precision interpretation (fixes errors like "Decimal precision 14 exceeds max precision 13")
    correct.decimals.fix.precision = true
    
    # Specifies the maximum number of records to fetch. Good for testing purposes.
    #limit.records = 100
  }
```

You can specify more than one JDBC url. Pramen will always try the primary URL first. If connection fails,
it will try fallback URLs in random order. If the primary URL is not specified, Pramen will try fallback URLs in
random order. You can also specify the number of retries. By default the number of retries is the same as the number
of URLs.

```conf
    jdbc = {
      # The primary connection URL 
      url = "jdbc:postgresql://example1.com:5432/test_db"
      fallback.url.1 = "jdbc:postgresql://example2.com:5432/test_db"
      fallback.url.2 = "jdbc:postgresql://example3.com:5432/test_db"
    }
    # The number of retries (optional)
    connection.retries = 5
```

#### Parquet source
Here is how you can configure the source. Parquet data source defines only if an information date column is
present. The exact path to the source is defined in the sourcing job configuration of the pipeline.

```config
{
    name = "parquet_source1"
    factory.class = "za.co.absa.pramen.core.source.ParquetSource"

    # Specifies if tables of the data source have an information date colunn
    has.information.date.column = true
    
    # If information column is present, specify its parameters:
    information.date {
      column = "info_date"
      
      # The format of the information date.
      # The format should be specified according to `java.time` spec:
      # https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
      date.app.format = "yyyy-MM-dd"
    }
}
```

### Sinks
Sinks define a way data needs to be sent to a target system. Built-in sinks include:
- Kafka sink.
- CSV in a local folder sink.
- Command Line sink.
- Dynamic Conformance Engine (Enceladus) sink.

You can define your own sink by implementing `Sink` trait and providing the corresponding class name in pipeline configuration.

#### Kafka sink
A Kafka sink allows sending data from a metastore table to a Kafka topic in Avro format.
You can define all endpoint and credential options in the sink definitions. The output topic
name should be defined in the definition of the pipeline operation.

Here is an example of a Kafka sink definition:

```config
{
  # Define a name to reference from the pipeline:
  name = "kafka_avro"
  factory.class = "za.co.absa.pramen.extras.sink.KafkaSink"
  
  writer.kafka {
    brokers = "mybroker1:9092,mybroker2:9092"
    schema.registry.url = "https://my.schema.regictry:8081"
    
    # Can be one of: topic.name, record.name, topic.record.name
    schema.registry.value.naming.strategy = "topic.name"
    
    # Arbitrary options for creating a Kafka Producer
    option {
      kafka.sasl.jaas.config = "..."
      kafka.sasl.mechanism = "..."
      kafka.security.protocol = "..."
      # ...
    }
    
    # Arbitrary options for Schema registry
    schema.registry.option {
      basic.auth.credentials.source = "..."
      basic.auth.user.info = "..."
      # ...
    }
  }
}
```

The corresponding pipeline operation could look like this:
<details>
  <summary>Click to expand</summary>

```config
{
  name = "Kafka sink"
  type = "sink"
  sink = "kafka_avro"
  schedule.type = "daily"
  # Optional dependencies
  dependencies = [
    {
      tables = [ dependent_table ]
      date.from = "@infoDate"
    }
  ]
  tables = [
    {
      metastore.table = metastore_table
      output.topic.name = "my.topic"
      
      # All following settings are OPTIONAL
      
      # Date range to read the source table for. By default the job information date is used.
      # But you can define an arbitrary expression based on the information date.
      # More: see the section of documentation regarding date expressions, an the list of functions allowed.
      date {
        from = "@infoDate"
        to = "@infoDate"
      }
      transformations = [
       { col = "col1", expr = "lower(some_string_column)" }
      ],
      filters = [
        "some_numeric_column > 100"
      ]
      columns = [ "col1", "col2", "col2", "some_numeric_column" ]
    }
  ]
}
```
</details>

#### CSV sink
The CSV sink allows generating CSV files in a local folder (on the edge node) from a table in the metastore. 

Here is an example of a CSV sink definition:
<details>
  <summary>Click to expand</summary>

```config
{
  name = "local_csv"
  factory.class = "za.co.absa.pramen.core.sink.LocalCsvSink"
  temp.hadoop.path = "/tmp/csv_sink"
  
  # This defines output file name pattern.
  # The below options will produce files like: FILE_20220118_122158.csv
  file.name.pattern = "FILE_@timestamp"
  file.name.timestamp.pattern = "yyyyMMdd_HHmmss"
  
  # This can be one of the following: no_change, make_upper, make_lower
  column.name.transform = "make_upper"
  
  # This defines the format of date and timestamp columns as they are exported CSV
  date.format = "yyyy-MM-dd"
  timestamp.format = "yyyy-MM-dd HH:mm:ss Z"
  
  # This defines arbitrary options passed to the CSV writer. The full list of options is available here:
  # https://spark.apache.org/docs/latest/sql-data-sources-csv.html
  option {
    sep = "|"
    quoteAll = "false"
    header = "true"
  }
}
```
</details>

The corresponding pipeline operation could look like this:
<details>
  <summary>Click to expand</summary>

```config
{
  name = "CSV sink"
  type = "sink"
  sink = "local_csv"
  schedule.type = "daily"
  dependencies = [
    {
      tables = [ dependent_table ]
      date.from = "@infoDate"
    }
  ]
  tables = [
    {
      metastore.table = metastore_table
      output.path = "/local/csv/path"
      # Date range to read the source table for. By default the job information date is used.
      # But you can define an arbitrary expression based on the information date.
      # More: see the section of documentation regarding date expressions, an the list of functions allowed.
      date {
        from = "@infoDate"
        to = "@infoDate"
      }
      transformations = [
       { col = "col1", expr = "lower(some_string_column)" }
      ],
      filters = [
        "some_numeric_column > 100"
      ]
      columns = [ "col1", "col2", "col2", "some_numeric_column" ]
    }
  ]
}
```

</details>

#### Command Line sink

Command Line sink allows outputting batch data to an application written in any language as long as it can be ran from a command line.
The way it works as follows:
1. Data for the sink will be prepared at a temporary path on Hadoop (HDFS, S3, etc.) in a format of user's choice.
2. Then, a custom command line will be invoked on the edge node passing the temporary path URI as a parameter.
3. Once the process has finished, the exit code will determine if the sink succeeded (exit code 0 means success, of course).
4. After the execution the data in the temporary folder will be cleaned up.

Here is an example of a command line sink definition that outputs to a CSV in a temporary folder and runs a command line:

<details>
  <summary>Click to expand</summary>

```config
{
  name = "cmd_line"
  factory.class = "za.co.absa.pramen.core.sink.CmdLineSink"
  
  # A temporary folder in Hadoop to put data to.
  temp.hadoop.path = "/tmp/cmd_line_sink"
  
  # Defines the output data format.
  format = "csv"
  
  # The number of command line log lines to include in email notification in case the job fails.
  include.log.lines = 1000
  
  # This defines arbitrary options passed to the CSV writer. The full list of options is available here:
  option {
    sep = "|"
    quoteAll = "false"
    header = "true"
  }
}
```
</details>

The pipeline operation for this sink could look like this:

<details>
  <summary>Click to expand</summary>

```config
{
  name = "Command Line sink"
  type = "sink"
  sink = "cmd_line"
  schedule.type = "daily"
  
  # Optional dependency definitions
  dependencies = [
    {
      tables = [ dependent_table ]
      date.from = "@infoDate"
    }
  ]
  
  tables = [
    {
      input.metastore.table = metastore_table
      output.cmd.line = "/my_apps/cmd_line_tool --path @dataPath --date @infoDate"
      
      ## All following settings are OPTIONAL
      
      # Date range to read the source table for. By default the job information date is used.
      # But you can define an arbitrary expression based on the information date.
      # More: see the section of documentation regarding date expressions, an the list of functions allowed.
      date {
        from = "@infoDate"
        to = "@infoDate"
      }
      
      transformations = [
       { col = "col1", expr = "lower(some_string_column)" }
      ],
      
      filters = [
        "some_numeric_column > 100"
      ]
      
      columns = [ "col1", "col2", "col2", "some_numeric_column" ]
    }
  ]
}
```
</details>

### Dynamic Conformance Engine (Enceladus) sink

This sink is used to send data to the landing area of the Enceladus Data Lake (also known as 'raw folder'). You can configure
output format, partition patterns and info file generation option for the sink.

Here is an example configuration of a sink:

<details>
  <summary>Click to expand</summary>

```config
{
  # Define a name to reference from the pipeline:
  name = "enceladus_raw"
  
  factory.class = "za.co.absa.pramen.extras.sink.EnceladusSink"
  
  # Output format. Can be: csv, parquet, json, delta, etc (anything supported by Spark). Default: parquet
  format = "csv"
  
  # Save mode. Can be overwrite, append, ignore, errorifexists. Default: errorifexists
  mode = "overwrite"
  
  # Information date column, default: enceladus_info_date
  info.date.column = "enceladus_info_date"
  
  # Partition pattern. Default: {year}/{month}/{day}/v{version}
  partition.pattern = "{year}/{month}/{day}/v{version}"
  
  # If true (default), the data will be saved even if it does not contain any records. If false, the saving will be skipped
  save.empty = true
  
  # Optionally repartition te dataframe according to the number of records per partition
  records.per.partition = 1000000
  
  # Output format options
  option {
    sep = "|"
    quoteAll = "false"
    header = "false"
  }
  
  # Info file options
  info.file {
    generate = true
    source.application = "Unspecified"
    country = "Africa"
    history.type = "Snapshot"
    timestamp.format = "dd-MM-yyyy HH:mm:ss Z"
    date.format = "yyyy-MM-dd"
  }
}
```
</details>

The pipeline operation for this sink could look like this:

<details>
  <summary>Click to expand</summary>

```config
{
  name = "Enceladus sink"
  type = "sink"
  sink = "enceladus_raw"
  
  schedule.type = "daily"
  
  tables = [
    {
      metastore.table = metastore_table
      output.path = "/datalake/base/path"
      
      # Optional info version (default = 1)
      info.version = 1
    }
  ]
}
```
</details>

Full Enceladus ingestion configuration examples: 
 - [examples/enceladus_sourcing](examples/enceladus_sourcing)
 - [examples/enceladus_single_config](examples/enceladus_single_config)

## Implementing transformers in Scala

Transformers define transformations on tables in the metastore and outputs it to the metastore in a functional manner.
This means if you define transformations in the deterministic way and if it does not contain side effects, it becomes
'replayable'. 

In order to implement a transformer all you need to do is define a class that implements `Transformer` trait and either
has the default constructor or a constructor with one parameter - a TypeSafe configuration object. Example:

```scala
package com.example

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.MetastoreReader
import za.co.absa.pramen.Transformer

import java.time.LocalDate

class ExampleTransformer(conf: Config) extends Transformer {
  override def validate(metastore: MetastoreReader,
                        infoDate: LocalDate,
                        options: Map[String, String]): Reason = {
    if (/* fatal failure */) {
      throw new IllegalArgumentException("Validation failed")
    }

    if (/* no data to run the transformer */) {
      Reason.NotReady(s"No data for the transformation at $infoDate")
    } else if (/* need to skip this information date and don't check again*/) {
      Reason.Skip(s"Empty data for the transformation at $infoDate. Nothing to process")
    } else {
      /* everything is in order */
      Reason.Ready
    }
  }

  override def run(metastore: MetastoreReader, 
                   infoDate: LocalDate,
                   options: Map[String, String]): DataFrame = {
    val df = metastore.getTable("some_table", Option(infoDate), Option(infoDate))

    /* Business logic of the transformation */
    df.withColumn("new_column", rand())
  }
}
```
(full example: [IdentityTransformer.scala](pramen/core/src/main/scala/za/co/absa/pramen/core/transformers/IdentityTransformer.scala))

You can refer to the transformer from the pipeline by its fully qualified class name (`com.example.ExampleTransformer` in this case).

In order to define a transformer you need to define 2 methods:
- `validate()` Validation allows pre-condition checks and failure to execute gracefully, before the transfomer is initialized.
  Alternativaly, throwing an exception inside this method is considered validation failure. 

  Possible validation return reasons:
  - `Reason.Ready` - the transformer is ready to run.
  - `Reason.NotReady` - required data is missing to run the transformer. The transformer can possibly run later for the
    information date when the data is available.
  - `Reason.Skip` - the requirements for the transformer won't be satisfied for the specified information date. The
    transformation is skipped (unless forced to run again). This could be used for cases when, say, nothing has arrived
    so nothing to process.

- `run()` Run the transformation and return a `DataFrame` containing transformation results. Input data can be fetched
  from the metastore. If an exception is thrown from this method, it is not considered as a failure. The pipeline will try
  running such transformations again when run again for the same information date.

Let's take a look at parameters passed to the transformer:
- `conf: Config` This is app's configuration. you can use it to fetch all parameters defined in the config, and you can
  override them when launching the pipeline. More on TypeSafe config [here](https://github.com/lightbend/config).

  While this is useful, we would recommend avoiding it for passing parameters to transformers. Prefer `options` (below)
  when possible. 
- `metastore: MetastoreReader` - this is the object you should use to access data. While you can still use `spark.read(...)`,
  the use of the metastore is strongly preferred in order to make transformers re-playable. 
  - `getTable()` - returns a `DataFrame` for the specified table and information date range. By default fetched data for
    the current information date.
  - `getLatest()` - returns a `DataFrame` for the specified table and latest information date for which the data is
    available. This latest data is no bugger that `infoDate` by default so you can re-run historical jobs that do not
    depend on the future data. But you can specify the maximum date in the `until` parameter.
  - `getLatestAvailableDate()` - returns the latest information date the data is available for a given table.
- `infoDate: LocalDate` - the [output] information date of the transformation.
- `options: Map[String, String]` - a map of key/value pairs of arbitrary options that you can define for the
  transformation in the pipeline. 

## Implementing transformers in Python

Here is an example transformer implemented in Python:
```python
@attrs.define(auto_attribs=True, slots=True)
class ExampleTransformation1(Transformation):
    async def run(
        self,
        metastore: MetastoreReader,
        info_date: datetime.date,
        options: Dict[str, str],
        **kwargs: T_EXTRA_OPTIONS,
    ) -> DataFrame:
        """Example transformation 1."""
        logger.info("Hi from ExampleTransformation1!")
        dep_table = metastore.get_table(
            "table1_sync",
            info_date_from=datetime.date(2022, 3, 23),
            info_date_to=datetime.date(2022, 3, 26),
        )
        return dep_table
```

Full example can be found here: [ToDo](ToDo)

## Setting up a pipeline
Once the metastore, sources, transformers and sinks are defined, they can be connected to form a data pipeline. A data
pipeline in Pramen defines a set of jobs that should run together or in a sequence. Your data engineering estate can
consist of several pipelines scheduled to run at different times. You can define dependencies between jobs in the pipeline
and jobs between pipeline as long as these pipelines share the metastore. 

Here is how a typical pipeline looks like:
![](resources/pipeline_example.png)

Every element is optional. You can have a pipeline without sources if sources are loaded by a different pipeline. You can
have a pipeline without transformers if data ingestion is all is needed.

Each pipeline has several mandatory options:

```config
pramen {
  # The environment name and pipeline name are defined to be included in email notifications.
  # You can reference system environment variables if you want your pipeline config to be deployable
  # to different envorinments without a change.
  environment.name = "MyEnv/UAT"
  pipeline.name = "My Data Pipeline"

  # The number of tasks to run in parallel. A task is a source, transformer, or sink running for a specified information date.
  # This feature is experimental, use more than 1 with caution. 
  parallel.tasks = 1

  
  # Pramen-Py settings
  py {
    # This is mandatory of you want to use Python transformations
    location = "/opt/Pramen-Py/bin"
    
    # Optionally you can specify Pramen-Py executable name
    executable = "pramen-py"
    
    # Optionally you can override the default command line pattern for Pramen-Py 
    cmd.line.template = "@location/@executable transformations run @pythonClass -c @metastoreConfig --info-date @infoDate"
    
    # Optionally you can override the default number of log lines to include in email notifications on a transformation failure.
    keep.log.lines = 2000
  }
}
```

A pipeline is defined as a set of operations. Each operation is either a source, transformation or a sink job. When a pipeline is
started, Pramen splits operations into jobs, jobs into tasks:

![](resources/ops_jobs_tasks.png)


A pipeline is defined as an array of operations. It becomes a DAG (directed acyclic graph) when each operation dependencies
are evaluated.

```config
pramen.operations = [
  {
    name = "Source operation"
    type = "ingestion"
    
    # Can be 'daily', 'weekly', 'monthly'
    schedule.type = "daily" 
    
    # schedule.type = weekly
    # 1 - Monday, ..., 7 - Sunday
    # schedule.days.of.week = [ 7 ]

    # schedule.type = monthly
    # schedule.days.of.month = [ 1 ]
    
    # (optional) Specifies an expression for date of initial sourcing for all tables in this operation.
    # Overrides 'default.daily.output.info.date.expr'   
    initial.sourcing.date.expr = "@runDate - 5"
    
    source = "my_jdbc_source"
    
    # Specifies an expression to calculate output information date based on the day at which the job has ran.
    # Optional, the default depends on the schedule.
    # For daily jobs the default is:   "@runDate"
    # For weekly jobs the default is:  "lastMonday(@runDate)"
    # For monthly jobs the default is: "beginOfMonth(@runDate)"
    info.date.expr = "@runDate"

    tables = [
      {
        input.db.table = table1
        output.metastore.table = table1
      },
      {
        input.sql = "SELECT * FROM table2 WHERE info_date = date'@infoDate'"
        output.metastore.table = table2
      }
    ]
  },
 {
    name = "A transformer"
    type = "transformer"
    class = "za.co.absa.pramen.core.transformers.IdentityTransformer"
    schedule.type = "daily"

    # Specifies a metastore table to save output data to
    output.table = "transformed_users"

    # Specifies an expression to calculate output information date based on the day at which the job has ran.
    info.date.expr = "@runDate"

    # Specifies which tables are inputs to the transformer and which date range input tables are expected to have input data.
    dependencies = [
      {
        tables = [ users1 ]
        date.from = "@infoDate"
        date.to = "@infoDate" // optional
      }
    ]
  },
  {
    name = "A Kafka sink"
    type = "sink"
    sink = "kafka_prod"

    schedule.type = "daily"

    tables = [
      {
        input.metastore.table = table1
        output.topic = kafka.topic.table1"
      }
    ]
  }
 ]
```

Each operation has the following properties:
- **Schedule** - (mandatory) defines which days it should run.
- **Information date expression** - defines an expression to calculate output information date from the date a job actually ran.
- **Dependencies** - specify data availability requirements that need to be satisfied for the operation to run.
- **Filters** - specify post-processing filters for each output table of the operation.
- **Schema transformations** - specify post-processing operations for the output table, usually related to schema evolution.
- **Columns selection** - specify post-processing projections (which columns to select) for the output table.

#### Schedule
A schedule specifies when an operation should run. 

Pramen does not have a built-in scheduler, so an external scheduler should be used to trigger runs of a pipeline.
It can be AirFlow, Dagster, RunDeck, DataBricks job scheduler, or even  local cron. Usually a pipeline runs daily,
but each operation can be configured to run only at specific days so some of them won't run each day. The schedule
setting specifies exactly that.

A schedule can be daily, weekly, or monthly.

Here are a couple of examples:

Daily:
```conf
    schedule.type = "daily" 
```

Weekly, on Sundays:
```conf
    schedule.type = weekly
    # 1 - Monday, ..., 7 - Sunday
    schedule.days.of.week = [ 7 ]
```

Twice a week, on Mondays and Fridays:
```conf
    schedule.type = weekly
    schedule.days.of.week = [ 1, 5 ] 
```

Monthly (on 1st day of the month):
```conf
    schedule.type = monthly
    schedule.days.of.month = [ 1 ]
```
Twice a month (on 1st and 15th day of each month):
```conf
    schedule.type = monthly
    schedule.days.of.month = [ 1, 15 ]
```

#### Output information date expression
Metastore tables are partitioned by information date. A chunk of data in a metastore table for specific information date is
considered an immutable atomic portion of data and a minimal batch. For event-like data information date may be considered 
the date of the event. For catalog-like data information date is considered the date of the snapshot.

Output information date expression allows specifying how the information date is calculated based on the date when the
pipeline is ran at.

- For daily jobs information date is usually calculated as the same day when the job has ran, or a day before.
- For weekly jobs information date is usually either beginning or end of week.
- For monthly jobs information date is usually either beginning or end of month.

Well-designed pipelines standardize information dates for weekly and monthly jobs across ingestion and transformation jobs
so that querying the data is easier.

You can specify default output information date expressions in the config (usually `common.conf`) liek this:
```conf
pramen {
  # Default infroamation date expression for daily jobs
  default.daily.output.info.date.expr = "@runDate"

  # Default infroamation date expression for weekly jobs (Monday of the current week)
  default.weekly.output.info.date.expr = "lastMonday(@runDate)"

  # Default infroamation date expression for monthly jobs (The first day of the month)
  default.monthly.output.info.date.expr = "beginOfMonth(@runDate)"
}
```

You can override defaults for specific operations by changing the definition of the operation as follows:
```
pramen.operations = [
  ...
  {
    ...
    info.date.expr = "@runDate"
  }
  ...
]
```

#### Initial sourcing dates

When you add a new table to the metastore and have a sourcing job for it, by default Pramen will load only recent data.
You can change the behavior by either providing default initial sourcing date expressions or specifying an initial 
sourcing date expression for an operation.

In the expression you specify an expression that given the current date (@runDate) returns the oldest date to load data for.  

Default values are configured like this:
```conf
pramen {
  # Default initial sourcing date expression for daily jobs
  initial.sourcing.date.daily.expr = "@runDate"

  # Default initial sourcing date expression for weekly jobs (pick up any information date last week)
  initial.sourcing.date.weekly.expr = "@runDate - 6"

  # Default initial sourcing date expression for monthly jobs (start from the beginning on the current month)
  initial.sourcing.date.monthly.expr = "beginOfMonth(@runDate)"
}
```

You can override defaults for specific operations by changing the definition of the operation as follows:
```
pramen.operations = [
  ...
  {
    ...
    initial.sourcing.date.expr = "@runDate"
  }
  ...
]
```

#### Dependencies
Dependencies for an operation allow specifying data availability requirements for a particular operation. For example,
'in order to run transformation T the input data in a table A should be not older than 2 days'. Dependencies determine
order of execution of operations.

You can use any expressions from [the date expression reference](#date-functions).

Here is a template for a dependency definition:
```config
{
  # The list of input tables for which the condition should be satisfied 
  tables = [ table1, table2 ]
  
  # Date range expression for which data should be available.
  # 'date.from' is mandatory, 'date.to' is optional. 
  date.from = "@infoDate"
  date.to = "@infoDate"
  
  # If true, retrospective changes to any of the tables in the list will cause the operation to rerun.
  trigger.updates = true
  
  # If true, dependency failure will cause a warning in the notification instead of error
  optional = true
}
```

Here is an example of dependencies definition:
```config
dependencies = [
  {
    # Tables table1 and table2 should current.
    # Any retrospective updates to these tables should trigger rerun of the operation. 
    tables = [ table1, table2 ]
    date.from = "@infoDate"
    trigger.updates = true
  },
  {
    # Table table3 should have data for the previous week from Mon to Sun  
    tables = [ table3 ]
    date.from = "lastMonday(@infoDate) - 7"
    date.to = lastMonday(@infoDate) - 1"
  },
  {
    # Table table4 should be available for the current month, older data will trigger a warning   
    tables = [ table3 ]
    date.from = "beginOfMonth(@infoDate)"
    optional = true
  }
]
```

#### Filters
Filters can be defined for any operation as well as any ingestion on sink table. Filters are applied before saving data
to the metastore table or before sending data to the sink.

The purpose of filters is to load or send only portion of the source table. You can use any Spark boolean expressions
in filters.

Example:
```config
filters = [
  "some_column1 > 100",
  "some_column2 < 300",
  "some_data_column == @infoDate"
]
```

#### Schema transformations
Schema transformations can be defined for any operation as well as any ingestion on sink table. Schema transformations
are applied before saving data to the metastore table or before sending data to the sink.

The purpose of schema transformations is to adapt to schema changes on data load or before sending data downstream.

You can create new columns, modify or delete existing columns. If the expression is empty, the column will be dropped. 

Example:
```config
transformations = [
  { col = "new_column", expr = "lower(existing_column)" },
  { col = "existing_column", expr = "upper(existing_column)" },
  { col = "column_to_delete", expr = "" }
],
```

#### Columns selection / projection
Columns selection or project can be defined for any operation as well as any ingestion on sink table. Columns selection
are applied before saving data to the metastore table or before sending data to the sink.

The purpose of columns selection is to define the set and the order of columns to load or send. Similar can be achieved
by schema transformations, but the only way to guarantee the order of columns (for example for CSV export) is to use
column selection.

Example:
```config
columns = [ "column1", "column2", "column3", "column4" ]
```

### Sourcing jobs

Sourcing jobs synchronize data at external sources with tables at the metastore. You specify an input source, and a mapping
between input tables/queries/paths to a table in the metastore. 

Here is an example configuration for a JDBC source:
```config
{
  # The name of the ingestion operation will be included in email notifications
  name = "JDBC data sourcing"
  
  # The operation type is 'ingestion'
  type = "ingestion"
  
  # THe schedule is mandatory
  schedule.type = "daily"
  
  # This specifies the source name from `sources.conf`
  source = "my_jdbc_source"
  
  # Optionally you can specify an expression for the information date.
  info.date.expr = "@runDate"
  
  # Data is 1 day late (T+1). When run at 2022-01-15, say, the expectation is that
  # the input table has data up until 2022-01-14 (inclusive).
  expected.delay.days = 1
  
  tables = [
    {
      input.db.table = "table1"
      output.metastore.table = "table1"
    },
    {
      input.db.table = "table2"
      output.metastore.table = "table2"
      
      # You can define range queries to the input table by providing date expressions like this:
      date.from = "@infoDate - 1"
      date.to = "@infoDate"
    },
    {
      input.db.table = "table3"
      output.metastore.table = "table3"
      
      # Optional filters, schema transformations and column selections
      filters = [ ]
      transformations = [ ]
      columns = [ ]
    },
    {
      input.sql = "SELECT * FROM table4 WHERE info_date = date'@infoDate'"
      output.metastore.table = "table4"
      
      # You can override any of source settings here 
      source {
        has.information.date.column = true
        information.date.column = "info_date"
      }
    }
  ]
}
```

Full example of JDBC ingestion pipelines: [examples/jdbc_sourcing](examples/jdbc_sourcing)

Here is an example configuration for a Parquet on Hadoop source. The biggest difference is that
this source uses `input.path` rather than `input.db.table` to refer to the source data. Filters,
schema transformations, column selection and source setting overrides can apply for this
source the same way as for JDBC sources:

```config
{
  name = "Parquet on Hadoop data sourcing"
  type = "ingestion"
  schedule.type = "daily"
  
  source = "my_parquet_source"
  
  tables = [
    {
      input.path = "s3a://my-bucket-data-lake/prefix/table1"
      output.metastore.table = "table1"
    }
  ]
}
```

### Transformation jobs (Scala)
In order to include a Scala transformer in the pipeline you just need to specify the fully qualified class name
of the transformer. 

Here is a example:
```config
{
  name = "My Scala Transformarion"
  type = "transformer"
  class = "com.example.MyTransformer"
  
  schedule.type = "daily"
  
  output.table = "my_output_table"
  
  dependencies = [
    {
      tables = [ table1 ]
      date.from = "@infoDate - 1"
      date.to = "@infoDate"
      trigger.updates = true
      optional = false
    },
    {
      tables = [table2, table3]
      date.from = "@infoDate"
      optional = true
    }
  ]
  
  # Arbitrary key/value pairs to be passed to the transformer.
  # Remember, you can refer enevironment variables here.
  option {
    key1 = "value1"
    key2 = "value2"
    key3 = ${MY_ENV_VARIABLE}
  }
  
  # Optional schema transformations 
  transformations = [
      {col = "A", expr = "cast(A as decimal(15,5))"}
  ]
  
  # Optional filters
  filters = [ "A > 0", "B < 2" ]
  
  # Optional column selection
  columns = [ "A", "B", "C" ]
}
```

Remember that although the dependency section is optional, you can use a table inside in the transformer only if it is 
included in dependencies. Even an optional dependency allows using a table inside the transformer.


### Transformation jobs (Python)
Python transformer definition is very similar to Scala transformer definitions. Use 'python_transformer' operation type
and 'python.class' to refer to the transformer.

```config
{
  name = "My Python Transformarion"
  type = "python_transformer"
  python.class = "MyTransformer"
  
  schedule.type = "daily"
  
  output.table = "my_output_table"
  
  dependencies = [
    {
      tables = [ table1 ]
      date.from = "@infoDate - 1"
      date.to = "@infoDate"
      trigger.updates = true
      optional = false
    },
    {
      tables = [table2, table3]
      date.from = "@infoDate"
      optional = true
    }
  ]
  
  # Arbitrary key/value pairs to be passed to the transformer.
  # Remember, you can refer enevironment variables here.
  option {
    key1 = "value1"
    key2 = "value2"
    key3 = ${MY_ENV_VARIABLE}
  }
  
  # Optional schema transformations 
  transformations = [
      {col = "A", expr = "cast(A as decimal(15,5))"}
  ]
  
  # Optional filters
  filters = [ "A > 0", "B < 2" ]
  
  # Optional column selection
  columns = [ "A", "B", "C" ]
}
```

### Sink jobs

Sink jobs allow sending data from the metastore downstream. The following examples may serve as a template for
sink operation definition.

#### Kafka sink example
```config
{
  name = "Kafka sink"
  type = "sink"
  sink = "kafka_prod_sink"

  schedule.type = "daily"

  tables = [
    {
      input.metastore.table = table1
      output.topic = "kafka.topic1"
      
      columns = [ "A", "B", "C", "D" ]
      
      date = {
        from = "@infoDate"
        to = "@infoDate"
      }
    }
  ]
}
```

#### Local CSV sink example
```config
{
  name = "CSV sink"
  type = "sink"
  sink = "local_sftp_sink"

  schedule.type = "weekly"
  schedule.days.of.week = [ 2 ] // Tuesday

  tables = [
    {
      input.metastore.table = table1
      output.path = "/output/local/path"
      
      columns = [ "A", "B", "C", "D" ]
      
      date = {
        from = "lastMonday(@infoDate) - 7"
        to = "lastSunday(@infoDate)"
      }
    }
  ]
}
```

### Enceladus ingestion pipelines for the Data Lake
Pramen can help with ingesting data for data lake pipelines of [Enceladus](https://github.com/AbsaOSS/enceladus).
A special sink (`EnceladusSink`) is used to save data to Enceladus' raw folder.

Here is a template for such a pipeline:
<details>
  <summary>Click to expand</summary>

```config
pramen.metastore {
  tables = [
    {
      name = "table"
      description = "Some table 1"
      format = "parquet"
      path = /datalake/table1/landing
      information.date.start = "2022-01-01"
    }
  ]
}

pramen.sources = [
  {
    name = "postgre"
    factory.class = "za.co.absa.pramen.core.source.JdbcSource"

    jdbc = {
      driver = "org.postgresql.Driver"
      connection.primary.url = "jdbc:postgresql://connection.host/test_db"
      user = "user"
      password = "mypassword"
    }

    option.fetchsize = 50000
    option.batchsize = 50000

    has.information.date.column = true
    information.date.column = "info_date"
    information.date.type = "date"
    information.date.app.format = "yyyy-MM-dd"
    information.date.sql.format = "YYYY-MM-DD"
  }
]

pramen.sinks = [
  {
    name = "enceladus_sink"
    factory.class = "za.co.absa.pramen.extras.sink.EnceladusSink"

    format = "json"

    mode = "overwrite"

    records.per.partition = 1000000
    
    partition.pattern = "{year}/{month}/{day}/v{version}"

    info.file {
      generate = true

      source.application = "MyApp"
      country = "Africa"
      history.type = "Snapshot"
      timestamp.format = "dd-MM-yyyy HH:mm:ss Z"
      date.format = "yyyy-MM-dd"
    }
  }
]

pramen.operations = [
{
    name = "Table1 sourcing"
    type = "ingestion"
    schedule.type = "daily"

    source = "postgre"

    expected.delay.days = 1

    tables = [
      {
        input.db.table = table1
        output.metastore.table = table1

        date.from = "@infoDate"
        date.to = "@infoDate"

        transformations = [
          {col = "last_name_u", expr = "upper(last_name)"}
        ],
        filters = [
          "age > 50"
        ]
      }
    ]
  },
  {
    name = "Enceladus sink"
    type = "sink"
    sink = "enceladus_sink"

    schedule.type = "daily"

    tables = [
      {
        input.metastore.table = table1
        output.path = "/datalake/table1/raw"
        output.info.version = 1

        date {
          from = "@infoDate"
          to = "@infoDate"
        }
      }
    ]
  }
]
```
</details>

More can be found at the implementation of the sink itself: [EnceladusSink](pramen/extras/src/main/scala/za/co/absa/pramen/extras/sink/EnceladusSink.scala)

## Schema evolutions patterns

# Deploying the pipeline

# Running pipelines

Pramen is a Spark application so a pipeline can be started using a job submission tool applicable for particular cluster.
Usually a pipeline is ran daily. But each operation can define its own run schedule. If a pipeline is ran on a day
not applicable for a particular operation, the operation will be automatically skipped. So at different days different set
of operations is executed when you run a pipeline.

Pramen framework and some of built-in jobs are packaged in `pramen-runner-x.y.z.jar`. In order to minimize chance of
binary incompatibility with custom sources, sinks and transformers jobs that require additional libraries are packaged
in `pramen-extras-x.y.z.jar`. If you are running a pipeline with builtin jobs make sure `pramen-extras-x.y.z.jar` is included
in the class path.

## Running on a single local computer

## Running on an EC2 instance in local mode

## Running on Hadoop/Yarn clusters (like AWS EMR)

Use `spark-submit` to run `pramen` jobs.

```sh
spark-submit --class za.co.absa.pramen.core.runner.PipelineRunner \
  pramen-runner_2.11-x.y.z.jar --workflow my_workflow.conf
```

### Running built-in jobs

```sh
spark-submit --jars pramen_extras_2.11-x.y.z.jar \
  --class za.co.absa.pramen.runner.PipelineRunner \
  pramen-runner_2.11-x.y.z.jar --workflow my_workflow.conf
```

When configuration files are located at HDFS or S3 use `--files` option to fetch the configuration:
```sh
spark-submit --jars pramen-extras_2.11-x.y.z.jar \
  --class za.co.absa.pramen.runner.PipelineRunner \
  pramen-runner_2.11-x.y.z.jar --workflow ingestion_job.conf \
  --files s3://mybucket/ingestion_job.conf,s3://mybucket/common.conf,s3://mybucket/sources.conf,s3://mybucket/metastore.conf
```

### Running custom jobs

To run builtin jobs just specify your custom jar with `--jars` when submitting the runner jar.

```sh
spark-submit --jars custom-x.y.z.jar \
  --class za.co.absa.pramen.runner.PipelineRunner \
  pramen-runner_2.11-x.y.z.jar --workflow my_workflow.conf
```

## Running on Databricks


## Default pipeline run

When started without additional command line arguments Pramen will run a normal daily pipeline checks and will execute
jobs scheduled for the day.

Here is how it works. Suppose you run a pipeline at `2020-07-19` and `expected.delay.days = 1`. This means that
the pipeline should process data for the `information date = 2020-07-18`, as usual for T+1 jobs. During a normal
execution Pramen will do the following:

![](resources/run_diagram.png)

- Check for retrospective updates of the source data according to `track.days` of corresponding tables in the metastore.
  For this check Pramen will query sources for record counts for each of previous days.
  - If a mismatch is found (as at `2020-07-16` on this diagram), the data is reloaded and dependent transformers are
    recomputed (if `trigger.updates = true`)
- Check for late data by querying sources for records for previous days if none were loaded. If such data is found, it
  is loaded and related transformations will be ran.
- Run the pipeline for the day - check new data and run dependent transformers and sink jobs.

### Command line arguments

Specify which jobs to run:

| Argument             | Example                                                             | Description                                                                               |
|----------------------|---------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| --workflow <config>  | `--workflow pipeline.conf`                                          | Specify the pipeline to run                                                               |
| --files <files>      | `--files s3a://bucket/common.conf,hdfs://server/path/pipeline.conf` | Load files from a durable store into the local directory before processing the pipeline.  |
| --ops <operations>   | `--ops table1,table2`                                               | Run only specified operations. Operations are referenced by output metastore table names. |

Specify which dates to run:

| Argument                                         | Example                                       | Description                                                                                                                                                                                                  |
|--------------------------------------------------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --date <yyyy-MM-dd>                              | `--date 2020-07-18`                           | Runs the pipeline as if the current date was the one specified.                                                                                                                                              |
| --rerun <yyyy-MM-dd>                             | `--rerun 2020-07-18`                          | Re-runs the pipeline for the specific date.                                                                                                                                                                  |
| --date-from <yyyy-MM-dd> --date-to <yyyy-MM-dd>  | `--date-from 2020-07-01 --date-to 2020-07-18` | Runs the pipeline for a historical date range. Usually only subset of operations are selected for a historical run.                                                                                          |
| --run-mode { fill_gaps / check_updates / force } | `--run-mode fill_gaps`                        | Specifies the more of processing historical data range. `fill_gaps` runs only jobs that haven't ran before, `check_updates` runs jobs only if there are updates, `force` always runs each date in the range. |
| --inverse-order <true/false>                     | `--inverse-order true`                        | By default jobs are executed from the oldest to newest, this option allows reversing the order. For long historical jobs this can help make newest data available as soon as possible.                       |

Execution options:

| Argument                   | Example             | Description                                                                                                                                                                                                         |
|----------------------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --dry-run <true/false>]    | `--dry-run true`    | If `true` the pipeline won't be executed. Pramen will print which job it would have run.                                                                                                                       |
| --check-late-only          | `--check-late-only` | If specified, Pramen will do only late data checks and checks for retrospective updates. It won't run jobs that are not yet late. Useful for catch-up job schedules.                                           |
| --check-new-only           | `--check-new-only`  | If specified, Pramen will not check for late and updated data and will run only jobs scheduled for the current date.                                                                                           |
| --undercover <true/false>  | `--undercover true` | If true, Pramen will not update bookkeeper so any changes caused by the pipeline won't be recorded. Useful for re-running historical transformations without triggering execution of the rest of the pipeline. |

### Command line examples

The below examples assume the spark application submit command is embedded in `run.sh` script. 

Load the table or run the transformation for an explicit date, regardless of if the data is already loaded.
```sh
 ./run.sh --ops mytable --rerun 2021-01-01
```

Load the table or run the transformation for all info dates between `2020-01-01` and `2021-04-01` skipping dates for
which data is already loaded:
```sh
./run.sh --ops mytable --date-from 2020-01-01 --date-to 2021-04-01 --run-mode fill_gaps
```

Load the table or run the transformation for all info dates between `2020-01-01` and `2021-04-01` rerunning info
dates for which data is already loaded:
```sh
./run.sh --ops mytable --date-from 2020-01-01 --date-to 2021-04-01 --run-mode check_updates
```

Load the table or run the transformation for all info dates between `2020-01-01` and `2021-04-01` rerunning info
dates for which data is already loaded. Do not update bookkeeping information so that dependent jobs won't be
triggerred:
```sh
./run.sh --ops mytable --date-from 2020-01-01 --date-to 2021-04-01 --run-mode force --undercover true
```

## Expression syntax

When an expression is expected as a configuration option `@infoDate` variable is set to the information date of the job.

You can use days arithmetic, e.g. `'2020-12-27' + 1` will produce `'2020-12-28'`.

Any time a date expression is expected you can apply functions from the table below, and also day arithmetic.

For example, `plusDays(@infoDate, 2)` is the same as `@infoDate + 2`.

You can use nested function calls, for example `lastSunday(beginOfMonth(@infoDate - 1))`.

You can use the following functions to do date arithmetic in order to specify input data dependency window:

### Date Functions

For below examples, assume @infoDate = '2020-12-27'

|           Function          |                    Example                    |      Description      |
| --------------------------- | --------------------------------------------- | --------------------- |
| monthOf(date)               | `monthOf(@infoDate) = 12`                     | Returns month of the given date, 1 = January. |
| yearOf(date)                | `yearOf(@infoDate) = 2020`                    | Returns year by the given date. |
| yearMonthOf(date)           | `yearMonthOf(@infoDate) = '2020-12'`          | Returns a string by combining the year and the month in 'yyyy-MM' format. |
| dayOfMonth(date)            | `dayOfMonth(@infoDate) = 27`                  | Returns day of months for the given date.  |
| dayOfWeek(date)             | `dayOfWeek(@infoDate) = 7`                    | Returns day of week for the given day, 1 - Monday, 7 - Sunday. |
| plusDays(date, days)        | `plusDays(@infoDate, 2) = '2020-12-29'`       | Adds the specified number of days to the input date. |  
| minusDays(date, days)       | `minusDays(@infoDate, 2) = '2020-12-25'`      | Subtracts the specified number of days from the specified date. |
| plusWeeks(date, weeks)      | `plusWeeks(@infoDate, 3) = '2021-01-17'`      | Adds the specified number of weeks to the input date. |
| minusWeeks(date, weeks)     | `minusWeeks(@infoDate, 3) = '2020-12-06'`     | Subtracts the specified number of weeks from the specified date. |
| plusMonths(date, months)    | `plusMonths(@infoDate, 1) = '2021-01-27'`     | Adds the specified number of weeks to the input date. |
| minusMonths(date, months)   | `minusMonths(@infoDate, 1) = '2021-11-27'`    | Subtracts the specified number of weeks from the specified date. |
| beginOfMonth(date)          | `beginOfMonth(@infoDate) = '2020-12-21'`      | Returns the beginning of the month by the specified date. |
| endOfMonth(date)            | `endOfMonth(@infoDate) = '2020-12-31'`        | Returns the end of the month by the specified date. |
| lastDayOfMonth(date, month) | `lastDayOfMonth(@infoDate, 3) = '2020-12-03'` | Returns the last specific day of of month. |
| lastMonday(date)            | `lastMonday(@infoDate) = '2020-12-21'`        | Returns last Monday. |
| lastTuesday(date)           | `lastTuesday(@infoDate) = '2020-12-22'`       | Returns last Tuesday. |
| lastWednesday(date)         | `lastWednesday(@infoDate) = '2020-12-23'`     | Returns last Wednesday. |
| lastThursday(date)          | `lastThursday(@infoDate) = '2020-12-24'`      | Returns last Thursday. |
| lastFriday(date)            | `lastFriday(@infoDate) = '2020-12-25'`        | Returns last Friday. |
| lastSaturday(date)          | `lastSaturday(@infoDate) = '2020-12-26'`      | Returns last Saturday. |
| lastSunday(date)            | `lastSunday(@infoDate) = '2020-12-27'`        | Returns last Sunday. |


