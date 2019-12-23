package com.socrata.soda.message

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.model.{PutRecordRequest, PutRecordResult}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.util.logging.LazyStringLogger
import javax.jms.Destination
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

class KinesisMessageProducer(streamName: String) extends MessageProducer {

  private val log = new LazyStringLogger(getClass)

  val client: AmazonKinesis = AmazonKinesisClientBuilder.defaultClient

  def start(): Unit = {}
  def setServiceNames(serviceNames: Set[String]): Unit = {}
  def send(message: Message, destination: Option[Destination] = None): Unit = {
    val putRecordRequest = new PutRecordRequest

//    =====
//    {
//      "domainCname": "localhost",
//      "assetUid": "fbaf-z9rb",
//      "apiReads": "1",
//      "userAgent": "curl/7.54.0",
//      "domainId": "1",
//      "userSegment": "anonymous",
//      "observingFileName": "RowsService.java",
//      "result": "success",
//      "queryParameter.resourceId": "fbaf-z9rb",
//      "datetime": "2019-12-21T21:03:30.892Z",
//      "provenance": "official",
//      "observingClass": "com.blist.services.resources.RowsService",
//      "requestId": "7kdgblsrv492ogr8sl4966k21",
//      "observingMethod": "index",
//      "observingLineNumber": "419",
//      "isPublic": "true",
//      "ownerUid": "2",
//      "fileType": "text/csv"
//    }
// what are the requirements of json keys?
//
    val now = new DateTime()
    //val patternFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
    val patternFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

    val ts = now.toString(ISODateTimeFormat.dateTime())
    log.info("kinesis3  " + JsonUtil.renderJson(message, false))
    val record: String = s""""{"fileType": "text/csv","ownerUid": "2","isPublic": "true","requestId": "7kdgblsrv492ogr8sl4966k21","observingClass": "com.blist.services.resources.RowsService","provenance": "official","queryParameter.resourceId": "aaaa-aaa1","observingFileName": "RowsService.java","domainCname": "cheetah.test-socrata.com","assetUid":"aaaa-aaa1", "apiReads": "2","result": "success", "datetime": "${ts}", "observingMethod": "index", "observingLineNumber": "10","userAgent": "soda-fountain","domainId": "76","userSegment": "anonymous"}"""
    putRecordRequest.setStreamName(streamName)
    putRecordRequest.setData(ByteBuffer.wrap(record.getBytes))
    putRecordRequest.setPartitionKey("A") //RandomStringUtils.random(4))
    val putRecordResult = client.putRecord(putRecordRequest)
    log.info("kinesis3 result " + putRecordResult.toString)
  }
  def close(): Unit = {}
}

