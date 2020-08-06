package com.amazon.opendistroforelasticsearch.indexstatemanagement

import org.elasticsearch.client.Response
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.test.rest.ESRestTestCase

abstract class IndexManagementRestTestCase : ESRestTestCase() {

    fun Response.asMap(): Map<String, Any> = entityAsMap(this)

    protected fun Response.restStatus(): RestStatus = RestStatus.fromCode(this.statusLine.statusCode)

    protected fun getRepoPath(): String = System.getProperty("tests.path.repo")
}