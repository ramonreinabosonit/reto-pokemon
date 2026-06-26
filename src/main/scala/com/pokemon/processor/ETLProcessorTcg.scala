package com.pokemon.processor

import com.pokemon.Main.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, desc, max, regexp_extract, regexp_replace, split, when}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}

// claves primarias del Dataset:
// Name
object ETLProcessorTcg {

  def iniciarProcessorPokemonTcg(pokemonTcgPath: String)(implicit spark: SparkSession): DataFrame = {
    val logger: Logger = Logger.getLogger(getClass.getName)

    // para aceptar multicampos y no salte de linea
    // IMPORTANTE multiline y escape
    val pokemonTcgDF = spark.read.option("header", "true")
      .schema(definirEstructura())
      .option("multiline", "true")
      .option("escape", "\"")
      .csv(pokemonTcgPath)
    logger.info(s"Total columnas: ${pokemonTcgDF.columns.length}")
    logger.info(s"Total filas: ${pokemonTcgDF.count()}")

    // contar todos los nulos
    logger.info("Consulta filtrado de nulos")
    val totalNulos = pokemonTcgDF.select(pokemonTcgDF.columns.map(c =>
        count(when(col(c).isNull, c)).alias(c)
      ): _*
    )
    totalNulos.show(false)

    // identificar columnas multivalor
    logger.info("Consulta filtrado de columnas multivalor")
    val columnasMultivalor = pokemonTcgDF.select(pokemonTcgDF.columns.map(c =>
      max(when(col(c).startsWith("[") || col(c).startsWith("{"), 1).otherwise(0)).cast("boolean").alias(c)
      ): _*
    )
    columnasMultivalor.show()

    logger.info("Limpiando el DataFrame: Campos Id y Name")
    val pokemonTcfClearDF =
      pokemonTcgDF.withColumn("id", split(col("id"), "-").getItem(1).cast(LongType))
        .withColumn("name", regexp_replace(col("name"), "[♀]", "F"))
        .withColumn("name", regexp_replace(col("name"), "[♂]", "M"))
        .withColumn("name", regexp_replace(col("name"), "[^a-zA-Z]", ""))
        .withColumnRenamed("name", "nameCartas")

        // esta expresion regular elimina siempre el [' '] de la columna types
//        .withColumn("types", regexp_replace(col("types"), "[\\[\\]']", ""))
//        .withColumn("subtypes", regexp_replace(col("subtypes"), "[\\[\\]']", ""))
//        .withColumn("evolvesTo", regexp_replace(col("evolvesTo"), "[\\[\\]']", ""))

//        .withColumn("abilitiesName", regexp_extract(col("abilities"), "'name'\\s*:\\s'([^']+)'", 1))
//        .withColumn("abilitiesDescription", regexp_extract(col("abilities"), "'text'\\s*:\\s'([^']+)'", 1))
//        .withColumn("abilitiesType", regexp_extract(col("abilities"), "'type'\\s*:\\s'([^']+)'", 1))
//        .withColumn("abilitiesValue", regexp_extract(col("abilities"), "'value'\\s*:\\s'([^']+)'", 1))

    pokemonTcfClearDF.show(10)

//    logger.info("Revisando que se han evitado duplicados")
//    pokemonTcfClearDF.filter(col("name").contains("Nidoran")).show()

//    logger.info(s"Schema columnas: ${pokemonTcfClearDF.printSchema()}")

    // ESTE ES EL TOTAL DE CARTAS REPETIDAS
    logger.info("Mostrando pokemons duplicados:")
    pokemonTcfClearDF.groupBy("nameCartas").count().orderBy(desc("count")).show()

    // DIVIDIR LOS ATAQUES
    val ataqueTcgDF = pokemonTcfClearDF
//      withColumn("ataque", split(split(col("attacks"), ":")(1), ",")(0))
      .withColumn("ataque", regexp_extract(col("attacks"), "'name'\\s*:\\s*'([^']+)'", 1))
    ataqueTcgDF.show()

    // FIN DE LA LIMPIEZA
    ataqueTcgDF
  }

  private def definirEstructura(): StructType = {
    val customSchema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("set", StringType, nullable = true),
      StructField("series", StringType, nullable = true),
      StructField("publisher", StringType, nullable = true),
      StructField("generation", StringType, nullable = true),
      StructField("release_date", StringType, nullable = true),
      StructField("artist", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("set_num", IntegerType, nullable = true),
      StructField("types", StringType, nullable = true),
      StructField("supertype", StringType, nullable = true),
      StructField("subtypes", StringType, nullable = true),
      StructField("level", StringType, nullable = true),
      StructField("hp", IntegerType, nullable = true),
      StructField("evolvesFrom", StringType, nullable = true),
      StructField("evolvesTo", StringType, nullable = true),
      StructField("abilities", StringType, nullable = true),
      StructField("attacks", StringType, nullable = true),
      StructField("weaknesses", StringType, nullable = true),
      StructField("retreatCost", StringType, nullable = true),
      StructField("convertedRetreatCost", IntegerType, nullable = true),
      StructField("rarity", StringType, nullable = true),
      StructField("flavorText", StringType, nullable = true),
      StructField("nationalPokedexNumbers", StringType, nullable = true),
      StructField("legalities", StringType, nullable = true),
      StructField("resistances", StringType, nullable = true),
      StructField("rules", StringType, nullable = true),
      StructField("regulationMark", StringType, nullable = true),
      StructField("ancientTrait", StringType, nullable = true)
    ))
    customSchema
  }
}
