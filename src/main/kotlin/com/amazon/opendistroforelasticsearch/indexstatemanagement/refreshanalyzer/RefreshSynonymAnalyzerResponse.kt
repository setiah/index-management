package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.xcontent.ConstructingObjectParser
import org.elasticsearch.common.xcontent.XContentParser
import java.io.IOException
import java.util.*
import java.util.function.Function

class RefreshSynonymAnalyzerResponse : BroadcastResponse {

    @Throws(IOException::class)
    constructor(inp: StreamInput?): super(inp)

    constructor(totalShards: Int,
                successfulShards: Int,
                failedShards: Int,
                shardFailures: List<DefaultShardOperationFailedException>
    ): super(
            totalShards, successfulShards, failedShards, shardFailures
    )

    companion object {
//        private val PARSER: ConstructingObjectParser<RefreshSynonymAnalyzerResponse, Void> =
//        ConstructingObjectParser("refresh_synonym_analyzer", true) {
//            arg: Array<Any> ->
//            val response: BroadcastResponse = (arg as List<BroadcastResponse>)[0]
//            RefreshSynonymAnalyzerResponse(response.getTotalShards(), response.getSuccessfulShards(),
//            response.getFailedShards(), response.getShardFailures().toList())
//        }

        private val PARSER = ConstructingObjectParser<RefreshSynonymAnalyzerResponse, Void>("refresh_synonym_analyzer", true,
                Function { arg: Array<Any> ->
                    val response = arg[0] as BroadcastResponse
                    RefreshSynonymAnalyzerResponse(response.totalShards, response.successfulShards, response.failedShards,
                            Arrays.asList(*response.shardFailures))
                })

        init {
            declareBroadcastFields(PARSER)
        }

        fun fromXContent(parser: XContentParser?): RefreshSynonymAnalyzerResponse? {
            return PARSER.apply(parser, null)
        }
    }
}
