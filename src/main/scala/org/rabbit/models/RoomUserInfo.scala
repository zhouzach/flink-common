package org.rabbit.models

case class RoomUserInfo (
                          uid:Long,
                          roomId: Long,
                          action: Int,  //1为进入房间，-1为离开房间
                          ts: Long
                        )
