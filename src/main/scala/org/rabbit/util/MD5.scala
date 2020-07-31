package org.rabbit.util

import java.util

object MD5 {

  val md5Instance = java.security.MessageDigest.getInstance("MD5")

  def main(args: Array[String]): Unit = {
    val a = md5Instance.digest("abc".getBytes())
    println(util.Arrays.toString(a))

    val b = md5Instance.digest("abc".getBytes()).map(0xFF & _)
    println(util.Arrays.toString(b))

    val c = md5Instance.digest("abc".getBytes()).map(0xFF & _).map {
      "%02x".format(_)
    }
   c.foreach(print(_))

    println()
    val res=mk("abc")
    println(res)

  }

  def mk(content: String) = {
    md5Instance.digest(content.getBytes())
      .map(0xFF & _)    // Array[Byte] 转换成 10进制整数 Array[Int]
      .map {
        "%02x".format(_)    // 10进制转16进制，X 表示以十六进制形式输出，02 表示不足两位前面补0输出, Array[Int] 转换成 Array[String]
      }
      .foldLeft("") {
        _ + _           // 将Array[String] 转换成 String
      }
      .toUpperCase
  }

}
