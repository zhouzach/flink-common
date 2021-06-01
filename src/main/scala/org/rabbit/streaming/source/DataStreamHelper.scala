package org.rabbit.streaming.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object DataStreamHelper {

  val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    fromElements()
  }

  def fromElements()={

    import org.apache.flink.api.scala._
    val sourceStream= streamExecutionEnv
//      .fromElements((3L,"lily"), (4L,"tom") ,(4L,"tom1"))
      .fromElements(("apple",3), ("banana",2) ,("banana",4))
//      .fromElements(3L, 4L )
      .name("fromElements").uid("fromElements")

//    sourceStream.print()
    sourceStream.keyBy(0).sum(1).print()

    streamExecutionEnv.execute()
  }

}
