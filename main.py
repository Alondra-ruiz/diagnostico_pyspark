from pyspark.sql import SparkSession
from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.engine.Transformer import Transformer
from minsait.ttaa.datio.utils.Writer import Writer

if __name__ == '__main__':
        #spark: SparkSession = SparkSession \
        #.builder \
        #.master(SPARK_MODE) \
        #.getOrCreate()
        #transformer = Transformer(spark)

    #Se crea la sesion de spark con el nombre sacado de las variables de entorno
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    #Leemos el archivo csv obteniendo la ruta desde las variables de entorno
    df = spark.read.csv(INPUT_PATH, header=HEADER)

    #Obtenemos solo las columnas solicitadas
    df_select_colums = Transformer.select_colums(df)

    #Calcula la division entre potential vs overall
    df_potential_vs_overall = Transformer.calculate_potential_overall(df_select_colums)

    #Crea la columna player_cat y les asigna una letra segun su rank
    df_rank = Transformer.window_function(df_fill=df_potential_vs_overall)

    #Se Filtra el Dataframe segun sus categorias y puntajes en potential vs overall
    df_filter = Transformer.filter_columns(df_rank)

    #Enviamos el df al writer para guardarlo en parquet
    Writer.write(Writer,df_filter)



