package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ConstructingObjectParser
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.rest.action.RestActions
import java.io.IOException
import java.util.*
import java.util.function.Function

class RefreshSynonymAnalyzerResponse : BroadcastResponse {

    private var results: MutableMap<String, List<String>> = HashMap()

    protected var logger = LogManager.getLogger(javaClass)

    @Throws(IOException::class)
    constructor(inp: StreamInput) : super(inp) {
        val size: Int = inp.readVInt()
        for(i in 0..size) {
            results.put(inp.readString(), inp.readStringArray().toList())
        }
    }

    constructor(
        totalShards: Int,
        successfulShards: Int,
        failedShards: Int,
        shardFailures: List<DefaultShardOperationFailedException>,
        results: MutableMap<String, List<String>>
    ) : super(
        totalShards, successfulShards, failedShards, shardFailures
    ) {
        this.results = results
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: Params?): XContentBuilder? {
        builder.startObject()
        RestActions.buildBroadcastShardsHeader(builder, params, totalShards, successfulShards, -1, failedShards, null)

        for (index in results.keys) {
            val reloadedAnalyzers: List<String> = results.get(index)!!
            builder.startObject(index)
            builder.startArray("refreshed_analyzers")
            for (analyzer in reloadedAnalyzers) {
                logger.info(analyzer)
                builder.value(analyzer)
            }
            builder.endArray()
            builder.endObject()
        }

        builder.endObject()
        return builder
    }

    companion object {
        private val PARSER = ConstructingObjectParser<RefreshSynonymAnalyzerResponse, Void>("refresh_synonym_analyzer", true,
                Function { arg: Array<Any> ->
                    val response = arg[0] as RefreshSynonymAnalyzerResponse
                    RefreshSynonymAnalyzerResponse(response.totalShards, response.successfulShards, response.failedShards,
                            Arrays.asList(*response.shardFailures), response.results)
                })

        init {
            declareBroadcastFields(PARSER)
        }

//        fun fromXContent(parser: XContentParser?): RefreshSynonymAnalyzerResponse? {
//            return PARSER.apply(parser, null)
//        }
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeVInt(results.size)
        for ((key, value) in results.entries) {
            out.writeString(key)
            out.writeStringArray(value.toTypedArray())
        }
    }
}
