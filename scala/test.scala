import scala.util.control.Breaks._

object test{
    def main(args: Array[String]): Unit = {
        var i:Int = 0
        breakable {
            for (i <- 6 until 5) {
                println(i)
                if (i == 3) break
            }
        }
    }
}