package scalagrammar

/**
  * @author xiaoran
  * @date 2019/11/18
  *
  */
object Fib {
  def main(args: Array[String]): Unit = {
    def fib(n: Int): Int = {
      if (n == 0)
        0
      else if (n == 1)
        1
      else
        fib(n - 1) + fib(n - 2)
    }
    println(fib(9))
  }
}
