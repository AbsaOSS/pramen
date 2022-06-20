/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.lock.model

import slick.jdbc.H2Profile.api._
import slick.lifted.TableQuery

class LockTickets(tag: Tag) extends Table[LockTicket](tag, "lock_tickets") {
  def token = column[String]("token", O.PrimaryKey, O.Length(255))
  def owner = column[String]("owner", O.Length(255))
  def expires = column[Long]("expires")
  def * = (token, owner, expires) <> (LockTicket.tupled, LockTicket.unapply)
}

object LockTickets {
  lazy val lockTickets = TableQuery[LockTickets]
}
