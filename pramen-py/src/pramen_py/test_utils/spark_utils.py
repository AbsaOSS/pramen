#  Copyright 2022 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json
import re

from typing import Dict, Optional, Pattern, Tuple, TypeVar

import pyspark.sql.types as T

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


DataType = TypeVar("DataType", bound=T.DataType)

_RE_IS_TABLE_SEP_LINE = re.compile(r"^\s*\+(?:-+\+)+$")
_RE_SCHEMA_STRUCT = re.compile(
    r"^\s+\|--\s(.+):\s(\w+)(?:\(.+\))?\s\(nullable = (true|false)\)$"
)

STRUCT_TYPES = {
    "string": T.StringType(),
    "integer": T.IntegerType(),
    "date": T.DateType(),
    "long": T.LongType(),
    # TODO #39 implement array and nested types support
    "array": T.StringType(),
    "decimal": T.DecimalType(),
    "timestamp": T.TimestampType(),
    "boolean": T.BooleanType(),
}


def is_table_separator_line(
    line: str,
    pattern: Pattern[str] = _RE_IS_TABLE_SEP_LINE,
) -> bool:
    return bool(pattern.match(line))


def is_struct_field_row(
    row: str,
    pattern: Pattern[str] = _RE_SCHEMA_STRUCT,
) -> bool:
    return bool(pattern.match(row))


def extract_row_vals(line: str) -> Tuple[str, ...]:
    return tuple(map(str.strip, line.strip().split("|")[1:-1]))


def compose_struct_field(
    row: str,
    pattern: Pattern[str] = _RE_SCHEMA_STRUCT,
    types: Dict[str, T.AtomicType] = STRUCT_TYPES,
) -> T.StructField:
    try:
        name, type_str, nullable_str = pattern.findall(row)[0]
        if type_str == "array":
            logger.warning(
                f"casting parameter {name} as string type in row {row}."
                f"currently arrays are not supported."
            )
        return T.StructField(
            name,
            types[type_str],
            nullable=(nullable_str == "true"),
        )
    except ValueError as err:
        raise ValueError(
            f"Unknown row received {row}. Expected rows should match the "
            f"regex {pattern}, i.e. `|-- CLCAA521: string (nullable = true)`"
        ) from err
    except KeyError as err:
        raise KeyError(
            f"Unknown data type received: {type_str}. The list of known types"
            f" is {', '.join(types)}."
        ) from err


def df_show_repr_to_json(show_repr: str) -> str:
    show_repr = show_repr.strip()

    lines = filter(
        lambda line: not is_table_separator_line(line),
        show_repr.split("\n"),
    )
    header = [s.strip() for s in next(lines).split("|")][1:-1]

    return json.dumps(
        [dict(zip(header, row)) for row in map(extract_row_vals, lines)]
    )


def spark_schema_repr_to_schema(schema_repr: str) -> T.StructType:
    schema_repr = schema_repr.strip()
    return T.StructType(
        [
            compose_struct_field(row)
            for row in filter(is_struct_field_row, schema_repr.split("\n"))
        ]
    )


def apply_schema_to_df(
    raw_df: DataFrame,
    schema: T.StructType,
) -> DataFrame:
    schema_map = {f.name: f.dataType for f in schema.fields}
    logger.debug(f"Mapping dtypes to the dataframe {str(raw_df)}")

    return raw_df.select(
        *(
            col(column_name).cast(schema_map[column_name])
            for column_name in raw_df.columns
        )
    )


def generate_df_from_show_repr(
    spark: SparkSession,
    show_repr: str,
    schema_repr: Optional[str] = None,
) -> DataFrame:
    """Generate spark df from df.show and df.printSchema strings."""
    data = df_show_repr_to_json(show_repr)
    schema = spark_schema_repr_to_schema(schema_repr) if schema_repr else None

    # TODO #40 understand why df becomes empty if added schema=schema option
    raw_df = spark.read.json(
        spark.sparkContext.parallelize([data]),
    )
    return (
        apply_schema_to_df(
            raw_df,
            schema=schema,
        )
        if schema
        else raw_df
    )
