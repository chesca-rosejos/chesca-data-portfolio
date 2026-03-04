def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    dfc_0=dfc.select(list(dfc.keys())[0])
    df_file=dfc_0.toDF()
    import pyspark.sql.functions as F
    # basic transformation logic
    df_file=df_file.drop('column_z','column_y','column_w')
    df_file=df_file.withColumnRenamed('column_a', 'mainhashkey')
    df_file=df_file.withColumn('flag', F.lit(1))
    newdyc=DynamicFrame.fromDF(df_file, glueContext, "newdyc")
    return(DynamicFrameCollection({"newdyc": newdyc}, glueContext))
