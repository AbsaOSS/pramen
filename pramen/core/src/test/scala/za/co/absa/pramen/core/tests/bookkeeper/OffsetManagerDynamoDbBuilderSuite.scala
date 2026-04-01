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

package za.co.absa.pramen.core.tests.bookkeeper

import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import za.co.absa.pramen.core.bookkeeper.OffsetManagerDynamoDb

class OffsetManagerDynamoDbBuilderSuite extends AnyWordSpec {

  "OffsetManagerDynamoDbBuilder" should {
    "use default table prefix when not specified" in {
      val builder = OffsetManagerDynamoDb.builder
        .withRegion("us-east-1")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[OffsetManagerDynamoDb.OffsetManagerDynamoDbBuilder])
    }

    "allow setting region" in {
      val builder = OffsetManagerDynamoDb.builder
        .withRegion("eu-central-1")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[OffsetManagerDynamoDb.OffsetManagerDynamoDbBuilder])
    }

    "allow setting table ARN" in {
      val builder = OffsetManagerDynamoDb.builder
        .withRegion("us-west-2")
        .withTableArn("arn:aws:dynamodb:us-west-2:123456789012:table/")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[OffsetManagerDynamoDb.OffsetManagerDynamoDbBuilder])
    }

    "allow setting table prefix" in {
      val builder = OffsetManagerDynamoDb.builder
        .withRegion("ap-northeast-1")
        .withTablePrefix("staging_pramen")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[OffsetManagerDynamoDb.OffsetManagerDynamoDbBuilder])
    }

    "allow setting credentials provider" in {
      val credentials = AwsBasicCredentials.create("testAccessKey", "testSecretKey")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = OffsetManagerDynamoDb.builder
        .withRegion("us-east-1")
        .withCredentialsProvider(credentialsProvider)
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[OffsetManagerDynamoDb.OffsetManagerDynamoDbBuilder])
    }

    "allow setting endpoint for local testing" in {
      val builder = OffsetManagerDynamoDb.builder
        .withRegion("us-east-1")
        .withEndpoint("http://localhost:4566")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[OffsetManagerDynamoDb.OffsetManagerDynamoDbBuilder])
    }

    "allow setting batch ID" in {
      val batchId = 1234567890123L
      val builder = OffsetManagerDynamoDb.builder
        .withRegion("us-east-1")
        .withBatchId(batchId)

      assert(builder.isInstanceOf[OffsetManagerDynamoDb.OffsetManagerDynamoDbBuilder])
    }

    "support fluent API with all parameters" in {
      val credentials = AwsBasicCredentials.create("key", "secret")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)
      val batchId = System.currentTimeMillis()

      val builder = OffsetManagerDynamoDb.builder
        .withRegion("sa-east-1")
        .withTableArn("arn:aws:dynamodb:sa-east-1:999888777666:table/")
        .withTablePrefix("dev_pramen")
        .withCredentialsProvider(credentialsProvider)
        .withEndpoint("http://dynamodb.local:8000")
        .withBatchId(batchId)

      assert(builder.isInstanceOf[OffsetManagerDynamoDb.OffsetManagerDynamoDbBuilder])
    }

    "throw IllegalArgumentException when region is missing" in {
      val builder = OffsetManagerDynamoDb.builder
        .withBatchId(123456789L)

      val ex = intercept[IllegalArgumentException] {
        builder.build()
      }

      assert(ex.getMessage.contains("Region"))
    }
  }
}
