import argparse


from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import format_number

spark = (
    SparkSession.builder.appName(f"processing table")
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    .config("spark.hadoop.fs.s3n.multiobjectdelete.enable", "false")
    .config("spark.hadoop.fs.s3.multiobjectdelete.enable", "false")
    .getOrCreate()
)

bancos_clean = spark.read.parquet("s3://638059466675-target/Banco/")
glassdor_clean = spark.read.parquet("s3://638059466675-target/Empregados/")
reclamacoes_clean = spark.read.parquet("s3://638059466675-target/Reclamacoes/")

parser = argparse.ArgumentParser(
    description="process to generate tables."
)


parser.add_argument(
    "--s3_path",
    metavar="s3_path",
    type=str,
    nargs="?",
    help="Path of s3.",
)

args = parser.parse_args()

def convertCol(cols_to_fix, df):
    for col_name in cols_to_fix:
        # Substitui células vazias e com espaço por null
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    return df

def aggregation(df,  cols_to_fix, partition_by, aggregation_columns):
    df = convertCol(cols_to_fix, df)
    
    aggregation_exprs = []
    for column, aggregation_type in aggregation_columns.items():
        if aggregation_type == "avg":
            aggregation_exprs.append(format_number(avg(column), 2).alias(f"avg_{column}"))
        elif aggregation_type == "sum":
            aggregation_exprs.append(format_number(sum(column), 2).alias(f"sum_{column}"))
        
        
    df = df.groupBy(partition_by).agg(*aggregation_exprs)

    return df

glassdor_agg = aggregation(
        df=glassdor_clean,
        cols_to_fix = ['geral', 'remuneracao_e_beneficios'],
        partition_by= 'nome', 
        aggregation_columns = {
            "geral": "avg",  
            "remuneracao_e_beneficios": "avg"  
        }
        
)

reclamacoes_agg = aggregation(
        df=reclamacoes_clean,
        cols_to_fix = ['quantidade_total_de_reclamacoes', 'quantidade_total_de_clientes__ccs_e_scr'],
        partition_by= 'instituicao_financeira', 
        aggregation_columns = {
        "quantidade_total_de_reclamacoes": "sum",
        "quantidade_total_de_clientes__ccs_e_scr": "sum"
        }
        
)

dataframe_final = reclamacoes_agg.join(
    glassdor_agg,reclamacoes_agg.instituicao_financeira == glassdor_agg.nome, 'inner'
).join(
    bancos_clean,reclamacoes_agg.instituicao_financeira == bancos_clean.nome, 'inner'
).drop('nome')

dataframe_final.write.mode("overwrite").parquet(args.s3_path)

