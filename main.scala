// Import the data as a dataframe
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType, IntegerType};
import org.apache.spark.sql.Dataset;

val schema = StructType(Array(
    StructField("analytics_id", StringType),
    StructField("media_id",StringType),
    StructField("viewer_id",StringType),
    StructField("play_id",StringType),
    StructField("eastern_date", StringType),
    StructField("page_domain",StringType),
    StructField("media_title", StringType),
    StructField("plays", StringType, true),
    StructField("completes", StringType, true),
    StructField("watched_duration", StringType, true),
    StructField("watched_pct", StringType, true),
    StructField("page_title", StringType),
    StructField("country_code", StringType),
    StructField("avg_pct_viewable", StringType, true)
))

val plays_df = spark.read.schema(schema).option("header", "false").csv("s3://donato-dev.ltvytics.com/test/littlethings_network/*.gz")
plays_df.cache()
//plays.printSchema()


// Verify the CSV loaded properly
/*
import org.apache.spark.sql.functions.{asc,desc}

val viewer_counts = plays_df.groupBy("viewer_id").count()
viewer_counts.orderBy(desc("count")).limit(10).show()

val media_play_counts = plays_df.groupBy("media_id").count()
media_play_counts.orderBy(desc("count")).limit(10).show()
*/

// initialize graphing!

// NOTE!
// This is a bipartite *directed* graph, with edges from viewers leading to items they've watched
import org.apache.spark.graphx._
import org.apache.spark.rdd.{EmptyRDD, RDD}

import scala.util.MurmurHash

case class Play(analytics_id:String, media_id:String, viewer_id: String,
    play_id:String, eastern_date:String, page_domain:String, media_title:String, plays:String, completes:String,
    watched_duration:String, watched_pct:String, page_title:String, country_code:String, avg_pct_viewable:String)

// Convert csv to Spark DataSet
val plays_ds = plays_df.as[Play]

// Make Vertices
val viewer_ids = plays_ds.map(play => play.viewer_id)
    .distinct()
    .rdd

val media_ids = plays_ds.map(play => play.media_id)
    .distinct()
    .rdd

val both_rdd = viewer_ids.union(media_ids)
val viewers_and_content = both_rdd.map(vp => (MurmurHash.stringHash(vp).asInstanceOf[VertexId], vp))

// Make Edges
val play_relationships = plays_ds.map(play => Edge(MurmurHash.stringHash(play.viewer_id), MurmurHash.stringHash(play.media_id), 1)).rdd

val graph = Graph(viewers_and_content, play_relationships)

// Cache for better performance
graph.persist()




// Filter out some edges
val graph_one_edge_per_cnxn = graph
    .partitionBy(PartitionStrategy.RandomVertexCut)
    .groupEdges( (e1, e2) => 1)



// Generate histogram of network viewers time spent
val viewer_mine_all = plays_ds.map( (p:Play) => {
    val is_mine = if (p.analytics_id == "GbE4zhw_EeWyChJtO5t17w") 1 else 0
    (p.viewer_id, (is_mine, 1))
   })
   .rdd
   .reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )

val t2 = viewer_mine_all
    .filter{ case (key, value) => (value._2 >= 10) }
    .map(r => {
        //              (my_views / all_views)
        val pctInMine = (r._2._1.toDouble / r._2._2.toDouble)
        val bucket = math.round(pctInMine*10)
        (bucket, 1)
    })

val histogram = t2.reduceByKey( (x, y) => (x + y))

histogram.collect()
//  Array((0,831999), (1,969059), (2,504355), (3,392972), (4,289631), (5,255885), (6,197886), (7,169377), (8,201791), (9,213931), (10,351987))



import org.apache.spark.SparkContext

@transient val sc = SparkContext.getOrCreate()

val media_id_category = plays_ds.map( (p:Play) => p.media_id)
    .distinct()
    .map((id:String) => (id, id.toUpperCase().substring(0,1)))

val media_category_map = media_id_category.rdd.collectAsMap()

val bcastEnrichment = sc.broadcast(media_category_map)


val category_to_session = plays_ds.rdd
    // convert to tuple of media id to play_session
    .map{ case(p:Play) =>
        (bcastEnrichment.value.getOrElse(p.media_id, "Unknown"), 1)
    }.collect


category_to_session.cache()
category_to_session.first()
/*
val counts_per_category = category_to_session
    .map( (r:(String, Play)) => (r._1, 1) )
    //.rdd
   // .reduceByKey( (x, y) => x + y)

counts_per_category.collect()
// */