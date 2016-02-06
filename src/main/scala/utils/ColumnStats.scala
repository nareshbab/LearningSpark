package utils

/**
  * Created by inno on 1/22/2016.
  */
class ColumnStats(var nulls:Long = 0l,
                  var totalCount: Long = 0l,
                  var uniqueValues:Long = 0l,
                  var maxString: Long = 0,
                  var minString: Long = 0,
                  var minWords: Long = 0,
                  var maxWords: Long = 0,
                  var avglong: String ="0",
                  var standardDev: String ="0",
                  var minLong:String = "0",
                  var maxLong: String = "0",
                  var regexStats: String = "{\"format\": \"\", \"name\": \"\", \"blankRowsPercentage\": \"0.0\", \"invalidRowsPercentage\": \"0.0\", \"blankRowsCount\": \"0\", \"validaton\": \"\",\"sampleInvalidRows\": \"\", \"invalidRowsCount\": \"0\", \"datatype\": \"\"}",
                  var topNValues:String = "0") extends Serializable {


  //Part B.1.1
  def +=(colValue: Any, colCount: Long): Unit = {
    totalCount += colCount
    uniqueValues += 1

    if (colValue == null) {
      nulls += 1
    } else if (colValue.isInstanceOf[String]) {
      val colStringValue = colValue.asInstanceOf[String]
      val length = colStringValue.length()
      if(maxString == 0) minString = length
      if (length > maxString) maxString = length
      if (length < minString ) minString = length
      val words = colStringValue.split(" ").length
      if(maxWords == 0) minWords = words
      if (words > maxWords) maxWords = words
      if (words < minWords ) minWords = words
    } else if(colValue.isInstanceOf[Long]) {
      val colLongValue = colValue.asInstanceOf[Long]
      if(colLongValue == 0) nulls += 1
    }

  }

  //Part B.1.2
  def +=(columnStats: ColumnStats): Unit = {
    totalCount += columnStats.totalCount
    uniqueValues += columnStats.uniqueValues
    nulls += columnStats.nulls
  }

  override def toString = s"""{\"regexStats\":$regexStats ,\"standardDev\":$standardDev ,\"avgLong\":$avglong ,\"minLong\":$minLong ,\"maxLong\":$maxLong,\"nulls\":$nulls, \"totalCount\":$totalCount, \"uniqueValues\":$uniqueValues, \"maxString\":$maxString, \"minString\":$minString, \"maxWords\":$maxWords, \"minWords\":$minWords, \"topNValues\":$topNValues}"""
}
