/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.core.mocks.notify

import za.co.absa.pramen.core.notify.mq.SingleMessageProducer

class SingleMessageProducerSpy extends SingleMessageProducer {
  var connectInvoked = 0
  var sendInvoked = 0
  var closeInvoked = 0
  var lastTopicName = ""
  var lastMessage = ""

  override def send(topic: String, message: String, numberOrRetries: Int): Unit = {
    lastTopicName = topic
    lastMessage = message
    sendInvoked += 1
  }

  override def connect(): Unit = connectInvoked += 1

  override def close(): Unit = closeInvoked += 1
}
