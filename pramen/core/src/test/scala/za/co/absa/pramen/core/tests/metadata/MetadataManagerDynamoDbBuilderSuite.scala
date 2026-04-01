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

package za.co.absa.pramen.core.tests.metadata

import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import za.co.absa.pramen.core.metadata.MetadataManagerDynamoDb

class MetadataManagerDynamoDbBuilderSuite extends AnyWordSpec {

  "MetadataManagerDynamoDbBuilder" should {
    "use default table prefix" in {
      val builder = MetadataManagerDynamoDb.builder
        .withRegion("us-east-1")

      assert(builder.isInstanceOf[MetadataManagerDynamoDb.MetadataManagerDynamoDbBuilder])
    }

    "allow setting region" in {
      val builder = MetadataManagerDynamoDb.builder
        .withRegion("eu-north-1")

      assert(builder.isInstanceOf[MetadataManagerDynamoDb.MetadataManagerDynamoDbBuilder])
    }

    "allow setting table ARN" in {
      val builder = MetadataManagerDynamoDb.builder
        .withRegion("ap-southeast-1")
        .withTableArn("arn:aws:dynamodb:ap-southeast-1:123123123123:table/")

      assert(builder.isInstanceOf[MetadataManagerDynamoDb.MetadataManagerDynamoDbBuilder])
    }

    "allow setting table prefix" in {
      val builder = MetadataManagerDynamoDb.builder
        .withRegion("me-south-1")
        .withTablePrefix("test_metadata")

      assert(builder.isInstanceOf[MetadataManagerDynamoDb.MetadataManagerDynamoDbBuilder])
    }

    "allow setting credentials provider" in {
      val credentials = AwsBasicCredentials.create("access", "secret")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = MetadataManagerDynamoDb.builder
        .withRegion("af-south-1")
        .withCredentialsProvider(credentialsProvider)

      assert(builder.isInstanceOf[MetadataManagerDynamoDb.MetadataManagerDynamoDbBuilder])
    }

    "allow setting endpoint" in {
      val builder = MetadataManagerDynamoDb.builder
        .withRegion("us-west-2")
        .withEndpoint("http://localstack:4566")

      assert(builder.isInstanceOf[MetadataManagerDynamoDb.MetadataManagerDynamoDbBuilder])
    }

    "support complete fluent API chain" in {
      val credentials = AwsBasicCredentials.create("myKey", "mySecret")
      val credentialsProvider = StaticCredentialsProvider.create(credentials)

      val builder = MetadataManagerDynamoDb.builder
        .withRegion("ap-northeast-2")
        .withTableArn("arn:aws:dynamodb:ap-northeast-2:444333222111:table/")
        .withTablePrefix("metadata_manager")
        .withCredentialsProvider(credentialsProvider)
        .withEndpoint("http://custom-endpoint:9000")

      assert(builder.isInstanceOf[MetadataManagerDynamoDb.MetadataManagerDynamoDbBuilder])
    }

    "throw IllegalArgumentException when region is missing" in {
      val builder = MetadataManagerDynamoDb.builder

      val ex = intercept[IllegalArgumentException] {
        builder.build()
      }

      assert(ex.getMessage.contains("Region"))
    }
  }
}
