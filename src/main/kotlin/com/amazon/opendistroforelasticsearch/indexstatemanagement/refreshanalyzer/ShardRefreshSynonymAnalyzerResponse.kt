package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.index.shard.ShardId
import java.io.IOException

class ShardRefreshSynonymAnalyzerResponse : BroadcastShardResponse {
    var indexName: String
    var reloadedAnalyzers: List<String>

    constructor(`in`: StreamInput) : super(`in`) {
        indexName = `in`.readString()
        reloadedAnalyzers = `in`.readStringList()
    }

    constructor(shardId: ShardId?, indexName: String, reloadedAnalyzers: List<String>) : super(shardId) {
        this.indexName = indexName
        this.reloadedAnalyzers = reloadedAnalyzers
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeOptionalString(indexName)
        out.writeStringArray(reloadedAnalyzers.toTypedArray())
    }
}