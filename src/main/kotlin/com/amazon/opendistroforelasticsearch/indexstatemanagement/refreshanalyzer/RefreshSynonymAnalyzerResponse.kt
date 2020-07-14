package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.common.xcontent.ConstructingObjectParser
import java.util.*

class RefreshSynonymAnalyzerResponse(
        totalShards: Int,
        successfulShards: Int,
        failedShards: Int,
        shardFailures: List<DefaultShardOperationFailedException>) : BroadcastResponse(totalShards, successfulShards, failedShards, shardFailures) {

    companion object {
        var lambda = {arg: List<BroadcastResponse> -> {
            println("Hello")
            arg[0]
        }}

        var lmbda = {arg: Array<BroadcastResponse> ->
            val response: BroadcastResponse = arg[0]
            RefreshSynonymAnalyzerResponse(response.getTotalShards(), response.getSuccessfulShards(), response.getFailedShards(), response.getShardFailures().toList())
        }
        val PARSER: ConstructingObjectParser<RefreshSynonymAnalyzerResponse, Void> = ConstructingObjectParser<RefreshSynonymAnalyzerResponse, Void>("refresh_synonym_analyzer", true, lmbda)
    }
}

//{ arg -> {
//    val response: BroadcastResponse = arg[0]
//    return RefreshSynonymAnalyzerResponse(response.getTotalShards(), response.getSuccessfulShards(), response.getFailedShards(), Arrays.asList(response.getShardFailures()))
//}}