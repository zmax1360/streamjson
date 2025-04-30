package com.example.jsonstream

import com.example.jsonstream.JacksonStreamingParser
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import org.mockito.MockitoSugar
import org.scalatest.funspec.AnyFunSpec
import rx.lang.scala.Subscriber

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

class JacksonStreamingParserBufferTest extends AnyFunSpec with MockitoSugar {

  describe("JacksonStreamingParser.parseJsonStream") {

    it("should parse a large JSON array file streamed as Vert.x Buffers") {
      val path = Paths.get(getClass.getClassLoader.getResource("your-large-json-file.json").toURI)
      val lines = Files.readAllLines(path).asScala
      val fullJson = lines.mkString("\n")

      // Break JSON into chunks (simulate streaming)
      val chunks = fullJson.grouped(2048).map(Buffer.buffer).toSeq

      val subscriber = mock[Subscriber[JsonObject]]

      val handleChunk = JacksonStreamingParser.parseJsonStream(subscriber)

      // Simulate response.handler -> calling chunk by chunk
      chunks.foreach(handleChunk)

      // Wait a bit for background thread to finish
      Thread.sleep(1000)

      // Validate basic streaming behavior
      verify(subscriber, atLeastOnce()).onNext(any[JsonObject])
      verify(subscriber).onCompleted()
    }
  }
}
