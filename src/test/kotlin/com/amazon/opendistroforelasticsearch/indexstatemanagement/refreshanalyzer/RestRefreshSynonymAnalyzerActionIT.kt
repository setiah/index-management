package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.makeRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.RestStatus
import org.junit.Assert
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
        // path = /Users/setiah/projects/odfe/index-management/build/testrun/integTestRunner

        val fileName = "synonyms.txt"
        var file = File(fileName)
        // create a new file
        file.writeText("hello, hola")
        fail("debug")
    }

    fun `test search time analyzer`() {
        val indexName = "testindex"
        // TODO: change path logic
        val fileName = "/Users/setiah/projects/odfe/index-management/build/testclusters/integTest-0/config/pacman_synonyms.txt"
        var file = File(fileName)
        file.writeText("hello, hola")   // create a new file
        val source: String = """
            {
                "index" : {
                    "analysis" : {
                        "analyzer" : {
                            "my_synonyms" : {
                                "tokenizer" : "whitespace",
                                "filter" : ["synonym"]
                            }
                        },
                        "filter" : {
                            "synonym" : {
                                "type" : "synonym_graph",
                                "synonyms_path" : "pacman_synonyms.txt", 
                                "updateable" : true 
                            }
                        }
                    }
                }
            }
        """.trimIndent()

        val settings: Settings = Settings.builder().loadFromSource(source, XContentType.JSON).build()
        val mappings: String = """
            "properties": {
                    "title": {
                        "type": "text",
                        "analyzer" : "standard",
                        "search_analyzer": "my_synonyms"
                    }
                }
        """.trimIndent()

        // val mappings: String = "\"properties\":{\"title\":{\"type\": \"text\",\"analyzer\" : \"standard\",\"search_analyzer\": \"my_synonyms\"}}"
        createIndex(indexName, settings, mappings)
        ingestData(indexName)
        queryData(indexName, "hello")
        fail("check")
    }

    fun `test multiple search analyzers`() {

    }

    fun `test index alias`() {

    }

    companion object {
        fun ingestData(indexName: String) {
            val request = Request("POST", "/$indexName/_doc")
            val data: String = """
                {
                  "title": "hello world..."
                }
            """.trimIndent()
            request.setJsonEntity(data)
            client().performRequest(request)
        }

        fun queryData(indexName: String, query: String) {
            val request = Request("GET", "/$indexName/_search?q=$query")
            val result = client().performRequest(request)
            println(result.entity)
        }
    }
}