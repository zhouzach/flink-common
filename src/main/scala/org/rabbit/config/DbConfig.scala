package org.rabbit.config

import java.util.Properties


trait DbConfig {

  val url: String
  val user: String
  val password: String
  val driver: String

  def pop: Properties = {
    val pop = new Properties()
    pop.setProperty("user", user)
    pop.setProperty("password", password)
    pop.setProperty("driver", driver)
    pop
  }

}

object DevDbConfig extends DbConfig {

  val url: String = FileConfig.devDataSource.getString("url")
  val user: String = FileConfig.devDataSource.getString("user")
  val password: String = FileConfig.devDataSource.getString("password")
  val driver: String = FileConfig.devDataSource.getString("driver")

}

object ProdDbConfig extends DbConfig {
  val url: String = FileConfig.prodDataSource.getString("url")
  val user: String = FileConfig.prodDataSource.getString("user")
  val password: String = FileConfig.prodDataSource.getString("password")
  val driver: String = FileConfig.prodDataSource.getString("driver")

}

object DevCDHDbConfig {


  val url1: String = FileConfig.cdhDataSource.getString("url1")
  val host1: String = FileConfig.cdhDataSource.getString("host1")

  val url2: String = FileConfig.cdhDataSource.getString("url2")
  val host2: String = FileConfig.cdhDataSource.getString("host2")

  val url3: String = FileConfig.cdhDataSource.getString("url3")
  val host3: String = FileConfig.cdhDataSource.getString("host3")

  val user: String = FileConfig.cdhDataSource.getString("user")
  val password: String = FileConfig.cdhDataSource.getString("password")
}
