package com.amazon.opendistroforelasticsearch.indexmanagement.refreshanalyzer

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.makeRequest
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.RestStatus

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
            client().makeRequest(POST.toString(), "/$indexName/_refresh_synonym_analyzer")
            fail("Expected a failure")
        } catch (e: ResponseException) {
            val response = e.response.asMap()
            assertEquals(400, response.get("status"))
            assertEquals("index_closed_exception", (response.get("error") as HashMap<*, *>).get("type"))
        }
    }
}