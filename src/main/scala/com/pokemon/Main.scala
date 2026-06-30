package com.pokemon

import com.pokemon.processor.{ETLProcessorTcg, ETLProcessorUpdated}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array_max, asc, avg, col, count, desc, explode, expr, first, greatest, least, max, row_number, size, split, sum}
import org.apache.spark.sql.types.LongType
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
    println("CREACIÓN DF MAESTRO")

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

    println("\nPokemon más balanceados")
    val balance = joinDF.withColumn("balance",
      greatest(col("Attack"), col("Defense"), col("Speed")) - least(col("Attack"), col("Defense"), col("Speed")))
    balance.select("NamePokedex", "Attack", "Defense", "Speed", "balance").orderBy(asc("balance")).show()

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
    joinDF.select(col("NamePokedex"), size(col("ataque")).alias("total")).distinct().orderBy(desc("total")).show(10)

    println("\nCartas sin ataques")
//    joinDF.select("nameCartas", "ataque").distinct().filter(col("ataque") === 0).show(10)
    joinDF.select("nameCartas", "ataque").filter(size(col("ataque")) === 0).show(10)

    println("\nCartas con ataques sin daño")
    joinDF.select("nameCartas", "damage").filter(size(col("damage")) === 0).show(10)

    println("\nMedia de daño por rareza")
    joinDF.groupBy("rarity").agg(avg("damage")).show(10)
    val totalDamage = joinDF.withColumn("damageTotal",
      expr("aggregate(transform(damage, x -> int(x)), 0, (acc, x) -> acc + x)"))
    totalDamage.groupBy("rarity").agg(avg("damageTotal")).show()

    println("\nExtraer ataques de la columna attacks")
    totalDamage.select("ataque").distinct().show()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // dataset carta
    println("\nAtaque máximo por carta")
    val tcgMax = tcgCleanDF.withColumn("damageMax", array_max(col("damage")).cast("int")).withColumn(
      "ataqueMax", expr("""element_at(ataque, CAST(array_position(damage, damageMax) AS INT))"""))
    tcgMax.select("nameCartas", "ataqueMax", "damageMax").orderBy(desc("damageMax")).show(20)

    println("\nAtaque máximo por pokémon")
    // CAST porque devuelve bigint en vez de int
    totalDamage.select(col("NamePokedex"), expr("""element_at(ataque, CAST(array_position(damage, array_max(damage)) AS INT))""")
      .alias("ataqueMax"), array_max(col("damage")).alias("damageMax")).orderBy(desc("damageMax")).show(10)


    println("\nDaño medio por pokemon")
    totalDamage.groupBy("NamePokedex").agg(avg(col("damageTotal")).alias("damage_avg")).show(10)

    println("\nDaño máximo por tipo")
    totalDamage.groupBy("Type 1", "Type 2").agg(max("damageTotal").alias("damage_max")).show(10)
  }
}
