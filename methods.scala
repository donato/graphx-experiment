package donato.experiment


trait PlaySessionUtils {
  type Title = String

  def sanitizeTitle(t: Title): Title = t.replaceAll("'", "")

  def hasFamilyVideo(implicit t: Title): Boolean = {
    val td = t.toLowerCase

    td.contains("dad") || td.contains("mom") || td.contains("father") || td.contains("family")
  }

  def isAnimalVideo(implicit t: Title): Boolean = {
    val td = t.toLowerCase

    td.contains("cat") || td.contains("dog") || td.contains("pet")
  }

  def getVideoType(implicit t: Title): String =
    if (isAnimalVideo && isFamilyVideo) "Both"
    else if (isAnimalVideo) "AnimalVideo"
    else if (isFamilyVideo) "FamilyVideo"
    else "Unknown"


    // Helper function for outputting a graph
    def output_top_media_by_viewers(g : Graph[String, Int]) = {
        val viewers_per_media:VertexRDD[(String, Int)] = g.aggregateMessages[(String, Int)](
            // Map Function
            triplet => {
                // triplet.srcAttr = viewer_id
                // triplet.dstAttr = media_id
                // triplet.attr    = edge value
                // triplet.sendToDst, triplet.sendToSrc = aggregate on from/to vertice
                weight = 1/viewer.total
                triplet.sendToDst(triplet.dstAttr, weight)
            },
            // Reduce Function
            (left, right) => (left._1, left._2 + right._2)
        )

        val temp:RDD[(String, Int)] = viewers_per_media.map[(String, Int)]((t:(VertexId, (String, Int))) => t._2)
        temp.sortBy(_._2, ascending=false).take(5)
    }

}