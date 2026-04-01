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

package za.co.absa.pramen.core.tests.journal

import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import za.co.absa.pramen.core.journal.JournalDynamoDB

class JournalDynamoDBBuilderSuite extends AnyWordSpec {

  "JournalDynamoDBBuilder" should {
    "use default table prefix when not specified" in {
      val builder = JournalDynamoDB.builder
        .withRegion("us-east-1")

      assert(builder.isInstanceOf[JournalDynamoDB.JournalDynamoDBBuilder])
    }

    "allow setting region" in {
      val builder = JournalDynamoDB.builder
        .withRegion("eu-west-2")

      assert(builder.isInstanceOf[JournalDynamoDB.JournalDynamoDBBuilder])
    }

    "allow setting table ARN" in {
      val builder = JournalDynamoDB.builder
        .withRegion("us-west-1")
        .withTableArn("arn:aws:dynamodb:us-west-1:111222333444:table/")

      assert(builder.isInstanceOf[JournalDynamoDB.JournalDynamoDBBuilder])
    }

    "allow setting table prefix" in {
      val builder = JournalDynamoDB.builder
        .withRegion("ap-south-1")
        .withTablePrefix("qa_pramen")

      assert(builder.isInstanceOf[JournalDynamoDB.JournalDynamoDBBuilder])
    }

    "allow setting credentials provider" in {
      val credentials = AwsBasicCredentials.create("myAccessKey", "mySecretKey")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = JournalDynamoDB.builder
        .withRegion("us-east-2")
        .withCredentialsProvider(credentialsProvider)

      assert(builder.isInstanceOf[JournalDynamoDB.JournalDynamoDBBuilder])
    }

    "allow setting endpoint for local development" in {
      val builder = JournalDynamoDB.builder
        .withRegion("local")
        .withEndpoint("http://localhost:8000")

      assert(builder.isInstanceOf[JournalDynamoDB.JournalDynamoDBBuilder])
    }

    "support fluent API chaining" in {
      val credentials = AwsBasicCredentials.create("testKey", "testSecret")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = JournalDynamoDB.builder
        .withRegion("ca-central-1")
        .withTableArn("arn:aws:dynamodb:ca-central-1:555666777888:table/")
        .withTablePrefix("prod_journal")
        .withCredentialsProvider(credentialsProvider)
        .withEndpoint("http://dynamodb-local:8000")

      assert(builder.isInstanceOf[JournalDynamoDB.JournalDynamoDBBuilder])
    }

    "throw IllegalArgumentException when region is not set" in {
      val builder = JournalDynamoDB.builder

      val ex = intercept[IllegalArgumentException] {
        builder.build()
      }

      assert(ex.getMessage.contains("Region"))
    }
  }
}
