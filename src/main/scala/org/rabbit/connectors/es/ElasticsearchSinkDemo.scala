package org.rabbit.connectors.es

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.{ElasticsearchSink, RestClientFactory}
import org.apache.flink.util.ExceptionUtils
import org.apache.http.HttpHost
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HTTP
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.rabbit.models.Record1

object ElasticsearchSinkDemo {
  val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment

//  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

  import org.apache.flink.api.scala._
  val sourceStream= streamExecutionEnv
    .fromElements(Record1("a",1),Record1("b",2))
//    .fromElements(("apple"), ("banana") ,("banana"))
    .name("fromElements").uid("fromElements")

  def main(args: Array[String]): Unit = {

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
    httpHosts.add(new HttpHost("127.0.0.1", 9201, "http"))
    httpHosts.add(new HttpHost("127.0.0.1", 9202, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[Record1](
      httpHosts,
      indexSinkFunction()
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)

    // provide a RestClientFactory for custom configuration on the internally created REST client
    esSinkBuilder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
        val headers = Seq(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"))
        restClientBuilder.setDefaultHeaders(headers.toArray)
//        restClientBuilder.setPathPrefix(...)
//        restClientBuilder.setHttpClientConfigCallback(...)
      }
    })

//    esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler())
    val failureHandler = new ActionRequestFailureHandler {
        @throws(classOf[Throwable])
        override def onFailure( action: ActionRequest,
         failure:Throwable,
        restStatusCode: Int,
         indexer: RequestIndexer) {

          if (ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]).isPresent) {
            // full queue; re-add document for indexing
            indexer.add(action)
          } else if (ExceptionUtils.findThrowable(failure, classOf[ElasticsearchParseException]).isPresent) {
            // malformed document; simply drop request without failing sink
          } else {
            // for all other failures, fail the sink
            // here the failure is simply rethrown, but users can also choose to throw custom exceptions
            throw failure
          }
        }
      }
    esSinkBuilder.setFailureHandler(failureHandler)

    // finally, build and add the sink to the job's pipeline
    sourceStream.addSink(esSinkBuilder.build)

    streamExecutionEnv.execute()
  }


  def indexSinkFunction() ={
    new ElasticsearchSinkFunction[Record1] {
      def process(element: Record1, ctx: RuntimeContext, indexer: RequestIndexer) {
        val json = new java.util.HashMap[String, Any]
        json.put("field1", element.field1)
        json.put("field2", element.field2)

        val rqst: IndexRequest = Requests.indexRequest
          .index("log1")
          .source(json)

        indexer.add(rqst)
      }
    }
  }



}
