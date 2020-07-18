package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import org.elasticsearch.client.ResponseException
import org.elasticsearch.test.rest.ESRestTestCase
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

    }

    fun `test missing analyzer`() {

    }

    fun `test index time analyzer`() {

    }

    fun `test search time analyzer`() {

    }

    fun `test multiple search analyzers`() {

    }

    fun `test index alias`() {

    }

    fun createIndex() {

    }
}