package com.pokemon.processor

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, desc, max, regexp_replace, sum, when}
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}

object ETLProcessorUpdated {

  def iniciarProcessorUpdated(pathUpdated: String)(implicit spark: SparkSession): DataFrame = {
    val logger: Logger = Logger.getLogger(getClass.getName)

    logger.info("Cargando DataFrame Pokemon Updated")
    val pokemonUpdatedDF = spark.read.option("header", "true")
      .schema(definirEstructura()).csv(pathUpdated)

    logger.info(s"Total de filas: ${pokemonUpdatedDF.count()}")
    logger.info(s"Total de columnas: ${pokemonUpdatedDF.columns.length}")

    // contar todos los nulos
    logger.info("Consulta filtrado de nulos")
    val totalNulos = pokemonUpdatedDF.select(pokemonUpdatedDF.columns.map(c =>
        count(when(col(c).isNull, c)).alias(c)
      ): _*
    )
    totalNulos.show()
    //////////////////////////////////////////////////////////////////

    // identificar columnas multivalor (NO HAY)
    logger.info("Consulta filtrado de columnas multivalor")
    val columnasMultivalor = pokemonUpdatedDF.select(pokemonUpdatedDF.columns.map(c =>
        max(when(col(c).startsWith("[") || col(c).startsWith("{"), 1).otherwise(0)).cast("boolean").alias(c)
      ): _*
    )
    columnasMultivalor.show()
    //////////////////////////////////////////////////////////////////


    // LIMPIAMOS PRIMERO LOS FEMENIMOS Y MASCULINOS
    logger.info("Limpiando los nombres de los pokemons:")
    val limpiezaNombresDF = pokemonUpdatedDF.withColumn("Name", regexp_replace(col("Name"), "[♀]", "F"))
      .withColumn("Name", regexp_replace(col("Name"), "[♂]", "M"))
      .withColumn("Name", regexp_replace(col("Name"), "[^a-zA-Z]", ""))
      .withColumnRenamed("Name", "NamePokedex")

    limpiezaNombresDF.show()

    //   detectar pokemons duplicados
    logger.info("Mostrando pokemons duplicados:")
    limpiezaNombresDF.groupBy("NamePokedex")
      .count()
      .orderBy(desc("count"))
      .show()

    // FIN DE LA LIMPIEZA
    limpiezaNombresDF
  }

  private def definirEstructura(): StructType = {
    val customSchema = StructType(Seq(
      StructField("Id", IntegerType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Type 1", StringType, nullable = true),
      StructField("Type 2", StringType, nullable = true),
      StructField("HP", IntegerType, nullable = true),
      StructField("Attack", IntegerType, nullable = true),
      StructField("Defense", IntegerType, nullable = true),
      StructField("SpAtk", IntegerType, nullable = true),
      StructField("SpDef", IntegerType, nullable = true),
      StructField("Speed", IntegerType, nullable = true),
      StructField("Generacion", IntegerType, nullable = true),
      StructField("Legendary", BooleanType, nullable = true),
      StructField("Total", IntegerType, nullable = true)
    ))
    customSchema
  }

}
