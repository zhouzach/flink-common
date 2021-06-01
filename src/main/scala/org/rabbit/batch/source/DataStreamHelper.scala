package org.rabbit.batch.source

import org.apache.flink.api.scala.ExecutionEnvironment

object DataStreamHelper {

  val env = ExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    fromElements()
  }

  def fromElements()={

    import org.apache.flink.api.scala._
    val sourceStream= env
//      .fromElements((3L,"lily"), (4L,"tom") ,(4L,"tom1"))
      .fromElements(("apple",3), ("banana",2) ,("banana",4))
//      .fromElements(3L, 4L )
      .name("fromElements")


     val data = env.fromElements(2,3,4)
    val v=data.reduce((a,b)=>a+b)
    v.print()

//    sourceStream.print()

  }

}
