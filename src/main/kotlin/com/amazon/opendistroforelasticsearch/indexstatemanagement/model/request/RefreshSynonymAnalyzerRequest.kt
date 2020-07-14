package com.amazon.opendistroforelasticsearch.indexstatemanagement.model.request

import org.elasticsearch.action.support.broadcast.BroadcastRequest

class RefreshSynonymAnalyzerRequest : BroadcastRequest<RefreshSynonymAnalyzerRequest> {
    constructor(vararg indices: String) : super(*indices)
}