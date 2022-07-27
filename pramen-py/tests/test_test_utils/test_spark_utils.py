import datetime

from decimal import Decimal

import pyspark.sql.types as T
import pytest

from chispa import assert_df_equality
from pyspark.sql import SparkSession

from pramen_py.test_utils.spark_utils import generate_df_from_show_repr


@pytest.mark.asyncio
async def test_spark_utils_generate_df_from_show_repr(
    spark: SparkSession,
    capsys,
):
    """Test if df produced with generate_df_from_show_repr is identical.

    We create a df, then running show and printSchema on it and getting its
    values by reading stdout (with help of capsys fixture). Then we generate
    a new df based on these strings and comparing to the original one.
    """
    df_expected = spark.createDataFrame(
        (
            (1, 2, datetime.date(2022, 3, 23), "string", Decimal(15), True),
            (3, 4, datetime.date(2022, 3, 23), "string", Decimal(15), False),
        ),
        T.StructType(
            [
                T.StructField("A", T.IntegerType()),
                T.StructField("B", T.LongType()),
                T.StructField("info_date", T.DateType()),
                T.StructField("string col", T.StringType()),
                T.StructField("decimal col", T.DecimalType()),
                T.StructField("bool col", T.BooleanType()),
            ],
        ),
    )

    df_expected.show(truncate=False)
    df_expected.printSchema()

    out, _ = capsys.readouterr()
    show, schema = out.split("\n\n")[:-1]

    df_actual = generate_df_from_show_repr(
        spark,
        show_repr=show,
        schema_repr=schema,
    )
    assert_df_equality(
        df_actual,
        df_expected,
        ignore_column_order=True,
    )
