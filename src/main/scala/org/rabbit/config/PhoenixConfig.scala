package org.rabbit.config

import java.util.Properties


trait PhoenixConfig {

  val url: String
  val driver: String

  def pop: Properties = {
    val pop = new Properties()
    pop.setProperty("user", "")
    pop.setProperty("password", "")
    pop.setProperty("driver", driver)
    pop
  }

}

object DevPhoenixConfig extends PhoenixConfig {

  val url: String = FileConfig.devPhoenix.getString("url")
  val driver: String = FileConfig.devPhoenix.getString("driver")
}




