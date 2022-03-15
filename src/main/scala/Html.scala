import scala.util.matching.Regex

class Html(var html: String) {

  def parseOutTitle(): String = {
    val Pattern: Regex = "<Title>.([0-9a-zA-Z]+).</Title>".r
    val urls = Pattern.findAllMatchIn(html).map(x => x.group(1)).toList
    urls.head
  }

  def parseOutKeyWords(): Seq[String] ={
    val words = html.split(' ').toSeq
    words
  }

  def parseOutLinks(): List[String] ={
    val Pattern: Regex = "<a href=.([0-9a-zA-Z]+).>".r
    val urls = Pattern.findAllMatchIn(html).map(x => x.group(1)).toList
    urls
  }



}
