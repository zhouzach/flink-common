package org.rabbit.connectors.es

import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HTTP
import org.elasticsearch.client.RestClientBuilder

class RestClientFactoryImpl extends RestClientFactory {
  @Override
  def configureRestClientBuilder( restClientBuilder:RestClientBuilder) {

    val headers = Seq(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"))
  restClientBuilder.setDefaultHeaders(headers.toArray); //以数组的形式可以添加多个header
}
}
