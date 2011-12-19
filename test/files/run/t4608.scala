// #4608
/*@NOPAR*/ import collection.parallel._ /*NOPAR@*/
object Test {
  
  def main(args: Array[String]) {
    ((1 to 100) sliding 10).toList.par.map{_.map{i => i * i}}.flatten
  }
  
}
