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

package za.co.absa.pramen.core.integration

class IncrementalPipelineDeltaLongSuite extends IncrementalPipelineLongFixture {
  private val format = "delta"

  "For inputs without information date the pipeline" should {
    "work end to end as a normal run" in {
      testOffsetOnlyNormalRun(format)
    }

    "work for offsets out of order" in {
      testOffsetOnlyRunningOutOfOrderOffsets(format)
    }

    "work with incremental ingestion and normal transformer" in {
      testOffsetOnlyIncrementalIngestionNormalTransformer(format)
    }

    "work end to end as rerun" in {
      testOffsetOnlyRerun(format)
    }

    "work end to end as rerun with deletion of records" in {
      testOffsetOnlyRerunWithRecordsDeletion(format)
    }

    "work end to end as rerun with deletion of records with previous data present" in {
      testOffsetOnlyRerunWithRecordsDeletionAndPreviousDataPresent(format)
    }

    "skip when rerunning for a day which does not have offsets" in {
      testOffsetOnlySkipRerunWithoutOffsets(format)
    }

    "run for a historical date range with force update" in {
      testOffsetOnlyHistoricalDateRangeWithForceUpdate(format)
    }

    "run for a historical date range with fill gaps update" in {
      testOffsetOnlyHistoricalDateRangeWithFillGaps(format)
    }

    "deal with uncommitted offsets when no path" in {
      testOffsetOnlyDealWithUncommittedOffsetsWithNoPath(format)
    }

    "deal with uncommitted offsets when no data" in {
      testOffsetOnlyDealWithUncommittedOffsetsWithNoData(format)
    }

    "deal with uncommitted changes when there is data" in {
      testOffsetOnlyDealWithUncommittedOffsetsWithData(format)
    }

    "fail is the input data type does not conform" in {
      testOffsetOnlyFailWhenInputDataDoesNotConform(format)
    }

    "fail if the output table does not have the offset field" in {
      testOffsetOnlyFailWhenInputTableDoestHaveOffsetField(format)
    }
  }

  "For inputs with information date the pipeline" should {
    "work end to end as a normal run" in {
      testWithInfoDateNormalRun(format)
    }

    "work with incremental ingestion and normal transformer" in {
      testWithInfoDateIncrementalIngestionNormalTransformer(format)
    }

    "work end to end as rerun" in {
      testWithInfoDateRerun(format)
    }

    "work for historical runs" in {
      testWithInfoDateHistoricalDateRange(format)
    }
  }

  "When the input column is timestamp" should {
    "work end to end as a normal run" in {
      testWithTimestampNormalRun(format)
    }
  }

  "Edge cases" should {
    "offsets cross info days" in {
      testOffsetCrossInfoDateEdgeCase(format)
    }
  }
}
