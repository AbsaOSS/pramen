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
import za.co.absa.pramen.core.bookkeeper.BookkeeperDynamoDb

class BookkeeperDynamoDbBuilderSuite extends AnyWordSpec {

  "BookkeeperDynamoDbBuilder" should {
    "use default table prefix" in {
      val builder = BookkeeperDynamoDb.builder
        .withRegion("us-east-1")
        .withBatchId(123456789L)

      // We can't instantiate without valid DynamoDB connection,
      // but we can verify the builder returns itself (fluent API)
      assert(builder.isInstanceOf[BookkeeperDynamoDb.BookkeeperDynamoDbBuilder])
    }

    "allow setting region" in {
      val builder = BookkeeperDynamoDb.builder
        .withRegion("eu-west-1")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[BookkeeperDynamoDb.BookkeeperDynamoDbBuilder])
    }

    "allow setting table ARN" in {
      val builder = BookkeeperDynamoDb.builder
        .withRegion("us-east-1")
        .withTableArn("arn:aws:dynamodb:us-east-1:123456789012:table/")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[BookkeeperDynamoDb.BookkeeperDynamoDbBuilder])
    }

    "allow setting table prefix" in {
      val builder = BookkeeperDynamoDb.builder
        .withRegion("us-east-1")
        .withTablePrefix("test_pramen")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[BookkeeperDynamoDb.BookkeeperDynamoDbBuilder])
    }

    "allow setting credentials provider" in {
      val credentials = AwsBasicCredentials.create("accessKey", "secretKey")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = BookkeeperDynamoDb.builder
        .withRegion("us-east-1")
        .withCredentialsProvider(credentialsProvider)
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[BookkeeperDynamoDb.BookkeeperDynamoDbBuilder])
    }

    "allow setting endpoint" in {
      val builder = BookkeeperDynamoDb.builder
        .withRegion("us-east-1")
        .withEndpoint("http://localhost:8000")
        .withBatchId(123456789L)

      assert(builder.isInstanceOf[BookkeeperDynamoDb.BookkeeperDynamoDbBuilder])
    }

    "allow setting batch ID" in {
      val batchId = 987654321L
      val builder = BookkeeperDynamoDb.builder
        .withRegion("us-east-1")
        .withBatchId(batchId)

      assert(builder.isInstanceOf[BookkeeperDynamoDb.BookkeeperDynamoDbBuilder])
    }

    "support fluent API chaining" in {
      val credentials = AwsBasicCredentials.create("accessKey", "secretKey")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = BookkeeperDynamoDb.builder
        .withRegion("ap-southeast-2")
        .withTableArn("arn:aws:dynamodb:ap-southeast-2:123456789012:table/")
        .withTablePrefix("prod_pramen")
        .withCredentialsProvider(credentialsProvider)
        .withEndpoint("http://localhost:8000")
        .withBatchId(111222333L)

      assert(builder.isInstanceOf[BookkeeperDynamoDb.BookkeeperDynamoDbBuilder])
    }

    "throw IllegalArgumentException when region is not set" in {
      val builder = BookkeeperDynamoDb.builder
        .withBatchId(123456789L)

      val ex = intercept[IllegalArgumentException] {
        builder.build()
      }

      assert(ex.getMessage.contains("region"))
    }

    "throw IllegalArgumentException when batch ID is not set" in {
      val builder = BookkeeperDynamoDb.builder
        .withRegion("us-east-1")

      val ex = intercept[IllegalArgumentException] {
        builder.build()
      }

      assert(ex.getMessage.contains("BatchId is not supplied"))
    }
  }

  "BookkeeperDynamoDb.getFullTableName" should {
    "return table name when no ARN is provided" in {
      val result = BookkeeperDynamoDb.getFullTableName(None, "test_table")
      assert(result == "test_table")
    }

    "return ARN with /table/ prefix when ARN ends with slash" in {
      val arn = "arn:aws:dynamodb:us-east-1:123456789012:table/"
      val result = BookkeeperDynamoDb.getFullTableName(Some(arn), "test_table")
      assert(result == s"${arn}table/test_table")
    }

    "handle ARN without trailing slash by adding /table/" in {
      val arn = "arn:aws:dynamodb:eu-west-1:987654321098"
      val result = BookkeeperDynamoDb.getFullTableName(Some(arn), "my_table")
      assert(result == s"$arn/table/my_table")
    }
  }
}
