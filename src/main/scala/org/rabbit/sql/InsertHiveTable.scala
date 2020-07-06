package org.rabbit.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

object InsertHiveTable {

  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tableEnv = TableEnvironment.create(settings)

    val hiveConfDir = "/etc/hive/conf" // a local path
    val hiveVersion = "2.1.1"

    val odsCatalog = "odsCatalog"
    val odsHiveCatalog = new HiveCatalog(odsCatalog, "ods", hiveConfDir, hiveVersion)
    tableEnv.registerCatalog(odsCatalog, odsHiveCatalog)

    val dwdCatalog = "dwdCatalog"
    val dwdHiveCatalog = new HiveCatalog(dwdCatalog, "dwd", hiveConfDir, hiveVersion)
    tableEnv.registerCatalog(dwdCatalog, dwdHiveCatalog)

    //    tableEnv.sqlUpdate("INSERT INTO orders VALUES (5,'xigua',22)")
    //    tableEnv.sqlUpdate("INSERT INTO orders select 6,'caomei',12")
    //    tableEnv.sqlUpdate("INSERT INTO orders1 select * from orders")

    //    tableEnv.sqlUpdate(
    //      """
    //        |
    //        |INSERT INTO dwdCatalog.dwd.orders
    //        |select srcuid, price from (select srcuid, price, gid from dwdCatalog.dwd.bill where `year` = 2020 ) b
    //        |join ( SELECT gid FROM catalog1.ods.config_gift_activity WHERE atype = 2 ) c
    //        |ON b.gid = c.gid
    //        |
    //        |""".stripMargin)



    //dynamic partition:
    //    tableEnv.sqlUpdate(
    //      """
    //        |
    //        |INSERT INTO dwdCatalog.dwd.t1_copy
    //        |select id,name,`p_year`,`p_month` from dwdCatalog.dwd.t1 where `p_year` = 2020 and `p_month` = 4
    //        |
    //        |""".stripMargin)

    //dynamic partition:
    //    tableEnv.sqlUpdate(
    //      """
    //        |
    //        |INSERT INTO dwdCatalog.dwd.t1_copy
    //        |select * from dwdCatalog.dwd.t1 where `p_year` = 2020 and `p_month` = 3
    //        |
    //        |""".stripMargin)



    tableEnv.execute("Flink-1.10 insert hive Table Testing")
  }

}

