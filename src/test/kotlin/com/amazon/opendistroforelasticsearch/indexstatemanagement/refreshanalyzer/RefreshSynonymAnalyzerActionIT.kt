package com.amazon.opendistroforelasticsearch.indexstatemanagement.refreshanalyzer

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexManagementRestTestCase
import org.elasticsearch.client.Request
import org.elasticsearch.common.io.Streams
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.rest.ESRestTestCase
import java.io.File
import java.io.InputStreamReader

class RefreshSynonymAnalyzerActionIT : IndexManagementRestTestCase() {
    fun `test index time analyzer`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "testindex"

        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            file.writeText("hello, hola")   // create a new file
        }

        val settings: Settings = Settings.builder()
                .loadFromSource(getIndexAnalyzerSettings(), XContentType.JSON)
                .build()
        createIndex(indexName, settings, getAnalyzerMapping())
        ingestData(indexName)
        Thread.sleep(1000) // wait for refresh_interval

        val result1 = queryData(indexName, "hello")
        assertTrue(result1.contains("hello world"))

        // check synonym
        val result2 = queryData(indexName, "hola")
        assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(indexName, "namaste")
        assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            file.writeText("hello, hola, namaste")   // Append to file
        }

        // New added synonym should NOT match
        val result4 = queryData(indexName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(indexName)

        // New added synonym should NOT match
        val result5 = queryData(indexName, "namaste")
        assertFalse(result5.contains("hello world"))

        // clean up
        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            if (file.exists()) {
                file.delete()
            }
        }
    }

    fun `test search time analyzer`() {
        val buildDir = System.getProperty("buildDir")
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val indexName = "testindex"

        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            file.writeText("hello, hola")   // create a new file
        }

        val settings: Settings = Settings.builder()
                .loadFromSource(getSearchAnalyzerSettings(), XContentType.JSON)
                .build()
        // val mappings: String = "\"properties\":{\"title\":{\"type\": \"text\",\"analyzer\" : \"standard\",\"search_analyzer\": \"my_synonyms\"}}"
        createIndex(indexName, settings, getAnalyzerMapping())
        ingestData(indexName)
        Thread.sleep(1000) // wait for refresh_interval

        val result1 = queryData(indexName, "hello")
        assertTrue(result1.contains("hello world"))

        // check synonym
        val result2 = queryData(indexName, "hola")
        assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(indexName, "namaste")
        assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            file.writeText("hello, hola, namaste")   // Append to file
        }

        // New added synonym should NOT match
        val result4 = queryData(indexName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(indexName)

        // New added synonym should match
        val result5 = queryData(indexName, "namaste")
        assertTrue(result5.contains("hello world"))

        // clean up
        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            if (file.exists()) {
                file.delete()
            }
        }
    }

    fun `test alias`() {
        val indexName = "testindex"
        val numNodes = System.getProperty("cluster.number_of_nodes", "1").toInt()
        val buildDir = System.getProperty("buildDir")
        val aliasName = "test"
        val aliasSettings = "\"$aliasName\": {}"

        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            file.writeText("hello, hola")   // create a new file
        }

        val settings: Settings = Settings.builder()
                .loadFromSource(getSearchAnalyzerSettings(), XContentType.JSON)
                .build()
        ESRestTestCase.createIndex(indexName, settings, getAnalyzerMapping(), aliasSettings)
        ingestData(indexName)
        Thread.sleep(1000)

        val result1 = queryData(aliasName, "hello")
        ESRestTestCase.assertTrue(result1.contains("hello world"))

        // check synonym
        val result2 = queryData(aliasName, "hola")
        ESRestTestCase.assertTrue(result2.contains("hello world"))

        // check non synonym
        val result3 = queryData(aliasName, "namaste")
        ESRestTestCase.assertFalse(result3.contains("hello world"))

        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            file.writeText("hello, hola, namaste")   // Append to file
        }

        // New added synonym should NOT match
        val result4 = queryData(aliasName, "namaste")
        assertFalse(result4.contains("hello world"))

        // refresh synonyms
        refreshAnalyzer(aliasName)

        // New added synonym should match
        val result5 = queryData(aliasName, "namaste")
        assertTrue(result5.contains("hello world"))

        for (i in 0 until numNodes) {
            var file = File("$buildDir/testclusters/integTest-$i/config/pacman_synonyms.txt")
            if (file.exists()) {
                file.delete()
            }
        }
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
            ESRestTestCase.client().performRequest(request)
        }

        fun queryData(indexName: String, query: String): String {
            val request = Request("GET", "/$indexName/_search?q=$query")
            val response = ESRestTestCase.client().performRequest(request)
            return Streams.copyToString(InputStreamReader(response.entity.content))
        }

        fun refreshAnalyzer(indexName: String) {
            val request = Request("POST", "/$indexName/_refresh_synonym_analyzer")
            ESRestTestCase.client().performRequest(request)
        }

        fun getSearchAnalyzerSettings(): String {
            return """
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
        }

        fun getIndexAnalyzerSettings(): String {
            return """
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
                                "synonyms_path" : "pacman_synonyms.txt"
                            }
                        }
                    }
                }
            }
            """.trimIndent()
        }

        fun getAnalyzerMapping(): String {
            return """
            "properties": {
                    "title": {
                        "type": "text",
                        "analyzer" : "standard",
                        "search_analyzer": "my_synonyms"
                    }
                }
            """.trimIndent()
        }
    }
}