package org.rabbit.models

import java.sql.Timestamp

case class BehaviorInfo(uid: String,
                        phoneType: String,
                        clickCount: Int,
                        time: Timestamp
                       )
