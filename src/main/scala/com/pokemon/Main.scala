package com.pokemon

import com.pokemon.processor.{ETLProcessorTcg, ETLProcessorUpdated}
import org.apache.spark.sql.functions.{asc, avg, col, desc, explode, split, sum}
import org.apache.spark.sql.{DataFrame, SparkSession, functions, types}

object Main {

  def main(args: Array[String]): Unit = {

    println("Cargando Librerías Spark, por favor espere...")
    implicit val spark: SparkSession = SparkSession.builder().appName("Streamflix").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println("Librerías cargadas!")

    val pokemonTcgPath = args(0)
    val pokemonUpdatedPath = args(1)

    val tcgCleanDF = ETLProcessorTcg.iniciarProcessorPokemonTcg(pokemonTcgPath)
    val pokedexCleanDF = ETLProcessorUpdated.iniciarProcessorUpdated(pokemonUpdatedPath)

    //////////////// EJERCICIO, JOIN Y TRABAJO CON JOIN
    // este seria el dataset maestro
    val joinDF = tcgCleanDF.join(pokedexCleanDF,
      tcgCleanDF("nameCartas") === pokedexCleanDF("NamePokedex"), "inner")

    println("\nJOIN ============")
    joinDF.show(30)
    println(s"Total Resultados INNER JOIN: ${joinDF.count()}")

    println("\nPOKEMON SIN CARTAS")
    val leftJoinDF2 = pokedexCleanDF.join(tcgCleanDF, pokedexCleanDF("NamePokedex") === tcgCleanDF("nameCartas"), "left" )
    leftJoinDF2.filter(col("nameCartas").isNull).select("NamePokedex").alias("Pokemon sin Cartas").show(40)
    println(s"En Total hay ${leftJoinDF2.filter(col("nameCartas").isNull).count()} pokemons sin cartas.")

    println("\nCARTAS SIN POKEMON")
    val rightJoin = pokedexCleanDF.join(tcgCleanDF, pokedexCleanDF("NamePokedex") === tcgCleanDF("nameCartas"), "right" )
    rightJoin.filter(col("NamePokedex").isNull).select("nameCartas").alias("Cartas sin Pokemon").show(40)
    println(s"En Total hay ${rightJoin.filter(col("NamePokedex").isNull).count()} cartas sin pokemon.")

    println("///////////////////////////////////////////////////////////")

    println("////////////////////// TRABAJO SOBRE EL DF MAESTRO")

    // top pokemon attack
    println("\nTOP ATTACK")
    joinDF.select("NamePokedex", "Attack").distinct().orderBy(desc("Attack")).limit(5).show()

    // top pokemon speed
    println("\nTOP SPEED")
    joinDF.select("NamePokedex", "Speed").distinct().orderBy(desc("Speed")).limit(5).show()

    // comparar Lengedary vs no Legendary
    println("\nComparación Legendary")
    joinDF.groupBy("Generacion").count().show()

    // media de stats por generacion
    println("\nMedia de stats por GENERACION")
    joinDF.groupBy("Generacion").agg(avg("Total").alias("Media Stats")).orderBy(asc("Generacion")).show()
//      .withColumn("Total", avg(col("Total")))

    // media stats por tipo
    println("\nMedia de stats por TIPO")
    joinDF.groupBy("Type 1", "Type 2").agg(avg("Total").alias("Media Stats")).orderBy(desc("Type 1")).show()


    // pokemon con mas cartas
    println("\nPokemons con MÁS cartas")
    joinDF.groupBy("nameCartas").count().orderBy(desc("count")).show(5)

    // rarezas mas frecuentes (rarity)
    println("\nRarezas más frecuentes")
    joinDF.groupBy("rarity").count().orderBy(desc("count")).show(5)

    // pokemon con mas ataques
    // PARA HACER ESTE APARTADO TENGO QUE SACAR CADA ATAQUE DEL JSON, POR NAME
    // REVISAR
    println("\nPokemon con mas ataques")
    joinDF.groupBy("NamePokedex", "ataque").count().orderBy(desc("count")).show()
  }
}
