package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.RestStatus
import java.io.File

class RestRefreshSynonymAnalyzerActionIT : IndexManagementRestTestCase() {

    fun `test missing indices`() {
        try {
            client().makeRequest(POST.toString(), "//_refresh_synonym_analyzer")
            fail("Expected a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectedErrorMessage = mapOf(
                    "error" to mapOf(
                            "root_cause" to listOf<Map<String, Any>>(
                                    mapOf("type" to "illegal_argument_exception", "reason" to "Missing indices")
                            ),
                            "type" to "illegal_argument_exception",
                            "reason" to "Missing indices"
                    ),
                    "status" to 400
            )
            assertEquals(expectedErrorMessage, actualMessage)
        }
    }

    fun `test closed index`() {
        val indexName = "testindex"
        val settings = Settings.builder().build()
        createIndex(indexName, settings)
        closeIndex(indexName)

        try {
            client().makeRequest(POST.toString(), "/${indexName}/_refresh_synonym_analyzer")
            fail("Expected a failure")
        } catch (e: ResponseException) {
            val response = e.response.asMap()
            assertEquals(400, response.get("status"))
            assertEquals("index_closed_exception", (response.get("error") as HashMap<*, *>).get("type"))
        }
    }

    fun `test index time analyzer`() {
        // https://www.programiz.com/kotlin-programming/examples/current-working-directory
        val path = System.getProperty("user.dir")
        println("Working Directory = $path")
        val fileName = "synonyms.txt"
        var file = File(fileName)
        // create a new file
        file.writeText("hello, hola")
        fail("debug")
    }

    fun `test search time analyzer`() {
        val indexName = "testindex"
//        val settings = Settings.builder()
//                .put("index.number_of_shards", 1)
//                .put("index.number_of_replicas", 0)
//                .put("index.analysis.analyzer.my_synonyms.tokenizer", "whitespace")
//                .putList("index.analysis.analyzer.my_synonyms.filter", listOf("synonym"))
//                .put("index.analysis.analyzer.filter.synonym.type", "synonym_graph")
//                .putList("index.analysis.analyzer.filter.synonym.synonyms", listOf("hello, hola"))
//                .build()

        createIndex(indexName, settings)
    }

    fun `test multiple search analyzers`() {

    }

    fun `test index alias`() {

    }
}