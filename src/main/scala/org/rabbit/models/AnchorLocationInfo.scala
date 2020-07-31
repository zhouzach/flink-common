package org.rabbit.models

case class AnchorLocationInfo(uid: Long,longitude: Double, latitude: Double, tag1: String, ts: Long)
case class AnchorLocationRow(rowKey: String, uid: Long,longitude: Double, latitude: Double, tag1: String, ts: Long)
