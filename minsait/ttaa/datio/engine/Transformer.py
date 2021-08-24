import pyspark.sql.functions as f
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.utils.Writer import Writer

from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.read_columns import *


class Transformer(Writer):
    def __init__(self, spark: SparkSession):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        df.printSchema()
        df = self.clean_data(df)
        df = self.example_window_function(df)
        df = self.column_selection(df)

        # for show 100 records after your transformations and show the DataFrame schema
        df.show(n=100, truncate=False)
        df.printSchema()

        # Uncomment when you want write your final output
        self.write(df)

    def read_input(self) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(INPUT_PATH)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        df = df.filter(
            (short_name.column().isNotNull()) &
            (short_name.column().isNotNull()) &
            (overall.column().isNotNull()) &
            (team_position.column().isNotNull())
        )
        return df

    def column_selection(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 5 columns...
        """
        df = df.select(
            short_name.column(),
            overall.column(),
            height_cm.column(),
            team_position.column(),
            catHeightByPosition.column()
        )
        return df

    def example_window_function(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        w: WindowSpec = Window \
            .partitionBy(team_position.column()) \
            .orderBy(height_cm.column().desc())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank < 10, "A") \
            .when(rank < 50, "B") \
            .otherwise("C")

        df = df.withColumn(catHeightByPosition.name, rule)
        return df

    # Metodos para el test diagnostico
    def select_colums(dataframe):
        df_select_colums = dataframe.select(short_name.column(),
                                            long_name.column(),
                                            age.column(),
                                            height_cm.column(),
                                            weight_kg.column(),
                                            nationality.column(),
                                            club_name.column(),
                                            overall.column(),
                                            potential.column(),
                                            team_position.column()
                                            )

        return df_select_colums

    def calculate_potential_overall(df):
        df_potential_vs_overall = df.withColumn("potential_vs_overall",
                                                f.round(df.potential / df.overall, 4))
        return df_potential_vs_overall

    def window_function(df_fill):
        windowSpec = Window.partitionBy(nationality.column(),
                                        team_position.column()
                                        ) \
            .orderBy(overall.column())
        df_partition = df_fill.withColumn("player_cat", f.when(f.rank().over(windowSpec).between(1, 3), "A") \
                                          .when(f.rank().over(windowSpec).between(3, 5), "B") \
                                          .when(f.rank().over(windowSpec).between(6, 10), "C") \
                                          .otherwise("D"))
        return df_partition

    def filter_columns(df_rank):
        df_filter = df_rank.filter(((df_rank.player_cat == "A") | (df_rank.player_cat == "B"))
                            | ((df_rank.player_cat == "C") & (df_rank.potential_vs_overall > 1.15))
                            | ((df_rank.player_cat == "D") & (df_rank.potential_vs_overall > 1.25))
                            )
        return df_filter