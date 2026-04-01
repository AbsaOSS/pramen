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

package za.co.absa.pramen.core.tests.lock

import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import za.co.absa.pramen.core.lock.TokenLockFactoryDynamoDb

class TokenLockFactoryDynamoDbBuilderSuite extends AnyWordSpec {

  "TokenLockFactoryDynamoDbBuilder" should {
    "use default table prefix when not set" in {
      val builder = TokenLockFactoryDynamoDb.builder
        .withRegion("us-east-1")

      assert(builder.isInstanceOf[TokenLockFactoryDynamoDb.TokenLockFactoryDynamoDbBuilder])
    }

    "allow setting region" in {
      val builder = TokenLockFactoryDynamoDb.builder
        .withRegion("eu-west-3")

      assert(builder.isInstanceOf[TokenLockFactoryDynamoDb.TokenLockFactoryDynamoDbBuilder])
    }

    "allow setting table ARN" in {
      val builder = TokenLockFactoryDynamoDb.builder
        .withRegion("ap-east-1")
        .withTableArn("arn:aws:dynamodb:ap-east-1:999888777666:table/")

      assert(builder.isInstanceOf[TokenLockFactoryDynamoDb.TokenLockFactoryDynamoDbBuilder])
    }

    "allow setting table prefix" in {
      val builder = TokenLockFactoryDynamoDb.builder
        .withRegion("sa-east-1")
        .withTablePrefix("lock_pramen")

      assert(builder.isInstanceOf[TokenLockFactoryDynamoDb.TokenLockFactoryDynamoDbBuilder])
    }

    "allow setting credentials provider" in {
      val credentials = AwsBasicCredentials.create("lockKey", "lockSecret")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = TokenLockFactoryDynamoDb.builder
        .withRegion("us-west-1")
        .withCredentialsProvider(credentialsProvider)

      assert(builder.isInstanceOf[TokenLockFactoryDynamoDb.TokenLockFactoryDynamoDbBuilder])
    }

    "allow setting endpoint for testing" in {
      val builder = TokenLockFactoryDynamoDb.builder
        .withRegion("local")
        .withEndpoint("http://dynamodb.local:8888")

      assert(builder.isInstanceOf[TokenLockFactoryDynamoDb.TokenLockFactoryDynamoDbBuilder])
    }

    "support full fluent API" in {
      val credentials = AwsBasicCredentials.create("fluentKey", "fluentSecret")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = TokenLockFactoryDynamoDb.builder
        .withRegion("cn-north-1")
        .withTableArn("arn:aws-cn:dynamodb:cn-north-1:123456789012:table/")
        .withTablePrefix("distributed_locks")
        .withCredentialsProvider(credentialsProvider)
        .withEndpoint("http://private-dynamodb:8000")

      assert(builder.isInstanceOf[TokenLockFactoryDynamoDb.TokenLockFactoryDynamoDbBuilder])
    }

    "throw IllegalArgumentException when region is not provided" in {
      val builder = TokenLockFactoryDynamoDb.builder

      val ex = intercept[IllegalArgumentException] {
        builder.build()
      }

      assert(ex.getMessage.contains("Region"))
    }
  }
}
