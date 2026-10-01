from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TAX_RATE = 1.20


def clean_data(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("name").isNotNull())
        .filter(F.col("amount") > 0)
        .withColumn("amount_with_tax", F.col("amount") * F.lit(TAX_RATE))
    )
