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

package za.co.absa.pramen.framework.app

import za.co.absa.pramen.api.metastore.Metastore
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.journal.Journal
import za.co.absa.pramen.framework.lock.TokenLockFactory

trait AppContext {
  val appConfig: AppConfig

  def bookkeeper: SyncBookKeeper

  def tokenLockFactory: TokenLockFactory

  def journal: Journal

  def metastore: Metastore

  def close(): Unit
}