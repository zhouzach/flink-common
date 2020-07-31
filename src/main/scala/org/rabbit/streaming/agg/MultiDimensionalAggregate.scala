package org.rabbit.streaming.agg

import org.apache.flink.api.common.functions.AggregateFunction
import org.rabbit.models.{BehaviorData, ResultInfo}

class MultiDimensionalAggregate extends AggregateFunction[BehaviorData, ResultInfo, ResultInfo] {

  override def createAccumulator(): ResultInfo = {
    ResultInfo("", "", 0)
  }

  override def merge(acc1: ResultInfo, acc2: ResultInfo): ResultInfo = {
    ResultInfo(acc1.uid, acc1.phone_type, acc1.cnt + acc2.cnt)
  }

  override def add(value: BehaviorData, acc: ResultInfo): ResultInfo = {
    if (acc.uid.equals(value.uid) && acc.phone_type.equals(value.phoneType)) {
      acc.cnt = acc.cnt + value.clickCount
      acc
    } else if (acc.uid.equals(value.uid)) {
      ResultInfo(acc.uid, value.phoneType, value.clickCount)
    } else {
      ResultInfo(value.uid + acc.uid, value.phoneType + acc.phone_type, value.clickCount)
    }
  }

  override def getResult(acc: ResultInfo): ResultInfo = {
    acc
  }
}
