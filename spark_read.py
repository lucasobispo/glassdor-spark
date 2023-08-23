import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

spark = (
    SparkSession.builder.appName(f"processing table")
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    .config("spark.hadoop.fs.s3n.multiobjectdelete.enable", "false")
    .config("spark.hadoop.fs.s3.multiobjectdelete.enable", "false")
    .getOrCreate()
)


parser = argparse.ArgumentParser(
    description="process to generate tables."
)

parser.add_argument(
    "--bucket_name",
    metavar="bucket_name",
    type=str,
    nargs="?",
    help="Name of bucket.",
)

parser.add_argument(
    "--separator",
    metavar="separator",
    type=str,
    nargs="?",
    help="Separator.",
)

parser.add_argument(
    "--encoding",
    metavar="encoding",
    type=str,
    nargs="?",
    help="encoding",
)

parser.add_argument(
    "--file_format",
    metavar="file_format",
    type=str,
    nargs="?",
    help="file_format",
)

parser.add_argument(
    "--column_name",
    metavar="column_name",
    type=str,
    nargs="?",
    help="Name of column.",
)

parser.add_argument(
    "--s3_path",
    metavar="s3_path",
    type=str,
    nargs="?",
    help="Path of s3.",
)

args = parser.parse_args()


accented_chars = "áàâãäéèêëíìîïóòôõöúùûüç"
normalized_chars = "aaaaaeeeeiiiiOOOOOuuuuc"

def remove_accents(dataframe):
    
    for col_name in [col for col in dataframe.columns if dataframe.schema[col].dataType == StringType()]:
        for i in range(len(accented_chars)):
            dataframe = dataframe.withColumn(col_name, translate(lower(col(col_name)), accented_chars[i], normalized_chars[i]))
    return dataframe

def rename_columns(dataframe):
    for col in dataframe.columns:
        cleaned_name = col.lower()

        for i in range(len(accented_chars)):
            
            cleaned_name = cleaned_name.replace(accented_chars[i], normalized_chars[i])
        
        cleaned_name = cleaned_name.replace(" ", "_").replace("-", "_").replace("-", "_").replace("(%)", "_").replace("", "")
        #print(col + "  " +  cleaned_name )
        dataframe = dataframe.withColumnRenamed(col,cleaned_name)

    return dataframe

def remove_word(dataframe, column_name):
    dataframe = dataframe.withColumn(column_name, regexp_replace(col(column_name), "^\s+|\s+$", ''))
    dataframe = dataframe.withColumn(column_name, regexp_replace(col(column_name), "\(conglomerado\).*", ''))
    dataframe = dataframe.withColumn(column_name, regexp_replace(col(column_name), "- prudencial.*", ''))
    dataframe = dataframe.withColumn(column_name, trim(col(column_name)))
    return dataframe


def read_csv(folder_path: str, separator: str, encoding: str, file_format: str):
    return spark.read.csv(folder_path, header=True, inferSchema=True, sep=separator, encoding = encoding)
  


df_source = read_csv(args.bucket_name, separator = args.separator, encoding = args.encoding, file_format = args.file_format)


rename_columns_df = rename_columns(df_source) 
df_clean   = remove_word(dataframe = remove_accents(rename_columns_df), column_name= args.column_name)

df_clean.write.mode("overwrite").parquet(args.s3_path)


