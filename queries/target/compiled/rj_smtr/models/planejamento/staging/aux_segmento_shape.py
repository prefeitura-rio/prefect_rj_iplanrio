# -*- coding: utf-8 -*-
import numpy as np
import pyproj
from pyspark.sql.functions import col, explode, lit, udf
from pyspark.sql.types import ArrayType, StringType
from shapely import wkt

# from shapely.geometry import LineString, Point
from shapely.ops import substring, transform


def model(dbt, session):
    dbt.config(
        materialized="table",
    )
    df = dbt.ref("aux_shapes_geom_filtrada")
    bq_projection = pyproj.CRS(dbt.config.get("projecao_wgs_84"))
    shapely_projection = pyproj.CRS(dbt.config.get("projecao_sirgas_2000"))

    def transform_projection(shape, from_shapely=False):
        if from_shapely:
            project = pyproj.Transformer.from_crs(
                shapely_projection, bq_projection, always_xy=True
            ).transform
        else:
            project = pyproj.Transformer.from_crs(
                bq_projection, shapely_projection, always_xy=True
            ).transform

        return transform(project, shape)

    def cut(line, distance, buffer_size):
        line_len = line.length
        dist_mod = line_len % distance
        dist_range = list(np.arange(0, line_len, distance))
        middle_index = (len(dist_range) // 2) + 1

        last_final_dist = 0
        lines = []

        for i, _ in enumerate(dist_range, start=1):
            if i == middle_index:
                cut_distance = dist_mod
            else:
                cut_distance = distance
            final_dist = last_final_dist + cut_distance
            segment = substring(line, last_final_dist, final_dist)
            lines.append(
                [
                    str(i),
                    transform_projection(segment, True).wkt,
                    segment.length,
                    transform_projection(segment.buffer(distance=buffer_size), True).wkt,
                ]
            )
            last_final_dist = final_dist

        return lines

    def cut_udf(wkt_string, distance, buffer_size):
        line = transform_projection(wkt.loads(wkt_string))
        return cut(line, distance, buffer_size=buffer_size)

    cut_udf = udf(cut_udf, ArrayType(ArrayType(StringType())))
    df_segments = df.withColumn(
        "shape_lists",
        cut_udf(
            col("wkt_shape"),
            lit(dbt.config.get("comprimento_shape")),
            lit(dbt.config.get("buffer_segmento_metros")),
        ),
    )

    df_exploded = (
        df_segments.select(
            "feed_version",
            "feed_start_date",
            "feed_end_date",
            "shape_id",
            explode(col("shape_lists")).alias("shape_list"),
        )
        .withColumn("id_segmento", col("shape_list").getItem(0))
        .withColumn("wkt_segmento", col("shape_list").getItem(1))
        .withColumn("comprimento_segmento", col("shape_list").getItem(2))
        .withColumn("buffer_completo", col("shape_list").getItem(3))
        .drop("shape_list")
    )

    return df_exploded


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"aux_shapes_geom_filtrada": "rj-smtr.planejamento_staging.aux_shapes_geom_filtrada"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {'projecao_wgs_84': 'EPSG:4326', 'projecao_sirgas_2000': 'EPSG:31983', 'comprimento_shape': 1000, 'buffer_segmento_metros': 30}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "rj-smtr"
    schema = "planejamento_staging"
    identifier = "aux_segmento_shape"
    
    def __repr__(self):
        return 'rj-smtr.planejamento_staging.aux_segmento_shape'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


