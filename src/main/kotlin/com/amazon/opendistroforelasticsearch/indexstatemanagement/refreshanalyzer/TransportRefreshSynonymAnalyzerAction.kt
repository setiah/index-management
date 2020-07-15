package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.DefaultShardOperationFailedException
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.ShardsIterator
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.index.analysis.AnalysisRegistry
import org.elasticsearch.index.shard.IndexShard
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException

class TransportRefreshSynonymAnalyzerAction :
        TransportBroadcastByNodeAction<
                RefreshSynonymAnalyzerRequest,
                RefreshSynonymAnalyzerResponse,
                TransportBroadcastByNodeAction.EmptyResult> {

    @Inject
    constructor(
                clusterService: ClusterService,
                transportService: TransportService,
                indicesService: IndicesService,
                actionFilters: ActionFilters,
                analysisRegistry: AnalysisRegistry,
                indexNameExpressionResolver: IndexNameExpressionResolver?
    ): super (
            "reload",
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            Writeable.Reader { RefreshSynonymAnalyzerRequest() },
            ThreadPool.Names.MANAGEMENT
    ) {
        this.analysisRegistry = analysisRegistry
        this.indicesService = indicesService
    }

    private val indicesService: IndicesService
    private val analysisRegistry: AnalysisRegistry

    @Throws(IOException::class)
    override fun readShardResult(`in`: StreamInput?): EmptyResult? {
        return EmptyResult.readEmptyResultFrom(`in`)
    }

    override fun newResponse(request: RefreshSynonymAnalyzerRequest?, totalShards: Int, successfulShards: Int, failedShards: Int,
                             responses: List<EmptyResult?>?, shardFailures: List<DefaultShardOperationFailedException>,
                             clusterState: ClusterState?): RefreshSynonymAnalyzerResponse? {
        return RefreshSynonymAnalyzerResponse(totalShards, successfulShards, failedShards, shardFailures)
    }

    @Throws(IOException::class)
    override fun readRequestFrom(`in`: StreamInput): RefreshSynonymAnalyzerRequest? {
        return RefreshSynonymAnalyzerRequest(`in`)
    }

    @Throws(IOException::class)
    override fun shardOperation(request: RefreshSynonymAnalyzerRequest?, shardRouting: ShardRouting): EmptyResult? {
        val indexShard: IndexShard = indicesService.indexServiceSafe(shardRouting.shardId().index).getShard(shardRouting.shardId().id())
        //TODO: Write code here
        logger.info("Himanshu: refreshing search analyzers " + indexShard.shardId().toString())
        indexShard.mapperService().reloadSearchAnalyzers(analysisRegistry)
        //indexShard.mapperService().getIndexAnalyzers();
        return EmptyResult.INSTANCE
    }

    /**
     * The refresh request works against *all* shards.
     */
    override fun shards(clusterState: ClusterState, request: RefreshSynonymAnalyzerRequest?, concreteIndices: Array<String?>?): ShardsIterator? {
        return clusterState.routingTable().allShards(concreteIndices)
    }

    override fun checkGlobalBlock(state: ClusterState, request: RefreshSynonymAnalyzerRequest?): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun checkRequestBlock(state: ClusterState, request: RefreshSynonymAnalyzerRequest?, concreteIndices: Array<String?>?):
            ClusterBlockException? {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices)
    }
}
