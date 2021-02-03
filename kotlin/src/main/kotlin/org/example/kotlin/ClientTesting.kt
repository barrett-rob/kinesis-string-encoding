package org.example.kotlin

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType
import software.amazon.awssdk.utils.AttributeMap
import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant


fun main() {
    val kinesisClientBuilder = KinesisClient.builder()
    val endpointUrl: String? = System.getenv("ENDPOINT_URL")
    if (endpointUrl != null) {
        kinesisClientBuilder
            .endpointOverride(URI.create(endpointUrl))
            .httpClient(UrlConnectionHttpClient.builder().buildWithDefaults(AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true).build()))
    }
    val kinesisClient = kinesisClientBuilder.build()
    val streamName = "reservation-stream"
    val describeStreamResponse = kinesisClient.describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
    val streamArn = describeStreamResponse.streamDescription().streamARN()
    val shardId = describeStreamResponse.streamDescription().shards().first().shardId()
    println("using kinesis stream [$streamName :: $streamArn]")

    val client = Client(streamName, kinesisClient, streamArn, shardId)

    client.publishAndReadString("abc") // ascii string)
    client.publishAndReadString("""😊😊""") // emojis (kotlin string)
    client.publishAndReadString("\uD83D\uDE0A\uD83D\uDE0A") // emojis (java string)

    // jackson encoded strings

    data class Message(val msg: String)

    val objectMapper = jacksonObjectMapper()

    client.publishAndReadString(objectMapper.writeValueAsString(Message("abc"))) // ascii string
    client.publishAndReadString(objectMapper.writeValueAsString(Message("""😊😊"""))) // emojis (kotlin string)
    client.publishAndReadString(objectMapper.writeValueAsString(Message("\uD83D\uDE0A\uD83D\uDE0A"))) // emojis (java string)

    // jackson encoded bytes

    client.publishAndReadBytes(objectMapper.writeValueAsBytes(Message("abc"))) // ascii string
    client.publishAndReadBytes(objectMapper.writeValueAsBytes(Message("""😊😊"""))) // emojis (kotlin string)
    client.publishAndReadBytes(objectMapper.writeValueAsBytes(Message("\uD83D\uDE0A\uD83D\uDE0A"))) // emojis (java string)
}

class Client(
    private val streamName: String,
    private val kinesisClient: KinesisClient,
    private val streamArn: String,
    private val shardId: String
) {


    fun publishAndReadString(publishingString: String) {
        val publishingBytes = publishingString.toByteArray(StandardCharsets.UTF_8)
        println("\n\npublishing string: $publishingString, bytes: ${publishingBytes.contentToString()}")
        publishAndRead(publishingBytes)
    }

    fun publishAndReadBytes(publishingBytes: ByteArray) {
        val publishingString = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(publishingBytes))
        println("\n\npublishing bytes: ${publishingBytes.contentToString()}, string $publishingString")
        publishAndRead(publishingBytes)
    }

    private fun publishAndRead(publishingBytes: ByteArray) {

        Thread.sleep(1000)
        val startAt = Instant.now()

        val putRecordRequest = PutRecordRequest.builder().partitionKey("pk").streamName(streamName).data(SdkBytes.fromByteArray(publishingBytes)).build()
        kinesisClient.putRecord(putRecordRequest)

        println("reading from kinesis stream [$streamName :: $streamArn]")
        val getShardIteratorRequest = GetShardIteratorRequest.builder().streamName(streamName).shardId(shardId).shardIteratorType(ShardIteratorType.AT_TIMESTAMP).timestamp(startAt).build()
        val shardIterator = kinesisClient.getShardIterator(getShardIteratorRequest).shardIterator()
        val getRecordsResponse = kinesisClient.getRecords(GetRecordsRequest.builder().shardIterator(shardIterator).build())

        getRecordsResponse.records().forEachIndexed { index, record ->
            val recordBytes = record.data().asByteArray()
            val recordString = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(recordBytes))
            println(" -> ($index) retrieved string: $recordString, bytes: ${recordBytes.contentToString()}")
        }
    }

}