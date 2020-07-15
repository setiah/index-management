package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import org.elasticsearch.action.support.ActiveShardCount
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.broadcast.BroadcastRequest
import org.elasticsearch.common.io.stream.StreamInput
import java.io.IOException

class RefreshSynonymAnalyzerRequest : BroadcastRequest<RefreshSynonymAnalyzerRequest> {
    constructor(vararg indices: String) : super(*indices)

    @Throws(IOException::class)
    constructor(`in`: StreamInput) : super(`in`) {
        indices = `in`.readStringArray()
        //indicesOptions = IndicesOptions.readIndicesOptions(`in`)
    }
}