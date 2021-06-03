package org.rabbit.util

import com.jayway.jsonpath.JsonPath

object JsonHelper {

  def main(args: Array[String]): Unit = {

    val json =
      """
        |
        |{"@timestamp":"2020-04-29T03:29:37.696Z","@metadata":{"beat":"filebeat","type":"_doc","version":"7.1.0","topic":"qile-report"},"agent":{"ephemeral_id":"65137625-0d8c-47c5-9f17-59538c214549","hostname":"iZuf60wmslifw4wptk9cqeZ","id":"6cdba9e6-4d3d-4909-9562-536decc4e712","version":"7.1.0","name":"test","type":"filebeat"},"log":{"offset":874,"file":{"path":"/data/log/qile-zhibo-game/qile-zhibo-game-treasure_box-2020-04-29.log"}},"message":"85517,20200429,1000,384,3,700","fields":{"tags":"treasure_box","tag":"qile-report"},"input":{"type":"log"},"ecs":{"version":"1.0.0"},"host":{"name":"test"}}
        |""".stripMargin

    try {
      val message = JsonPath.read[String](json, "$.message")
      val offset = JsonPath.read[Int](json, "$.log.offset")
      println(message)
      println(offset)
    } catch {
      case _ => {
        println("read json failed")
      }
    }


  }

}
