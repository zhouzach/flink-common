package org.rabbit.streaming.agg

import org.apache.flink.api.common.functions.AggregateFunction

class SingleDimensionalAggregate extends AggregateFunction[(String, String, String, Int), (String, Int), (String, Int)] {

  override def createAccumulator(): (String, Int) = {
    ("", 0)
  }

  override def merge(a: (String, Int), b: (String, Int)): (String, Int) = {
    (a._1, a._2 + b._2)
  }

  override def add(value: (String, String, String, Int), acc: (String, Int)): (String, Int) = {
    (value._1, value._4 + acc._2)
  }

  override def getResult(acc: (String, Int)): (String, Int) = {
    acc
  }
}
