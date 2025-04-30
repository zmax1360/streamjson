package com.example.jsonstream

import com.example.jsonstream.JacksonStreamingParser
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import org.mockito.MockitoSugar
import org.scalatest.funspec.AnyFunSpec
import rx.lang.scala.Subscriber

import java.io.{File, FileInputStream}
import java.nio.file.Paths

class JacksonStreamingParserRealStreamingTest extends AnyFunSpec with MockitoSugar {

  describe("JacksonStreamingParser with large file") {

    it("should stream and parse a 2GB JSON file without heap overflow") {
      val filePath = Paths.get(getClass.getClassLoader.getResource("your-2gb-file.json").toURI)
      val inputStream = new FileInputStream(new File(filePath.toUri))

      val subscriber = mock[Subscriber[JsonObject]]
      val handleChunk = JacksonStreamingParser.parseJsonStream(subscriber)

      val buffer = new Array  // 8KB chunk size
      var bytesRead = 0

      while ({ bytesRead = inputStream.read(buffer); bytesRead != -1 }) {
        handleChunk(Buffer.buffer(buffer.slice(0, bytesRead)))
      }

      inputStream.close()

      // Allow background thread to finish
      Thread.sleep(2000)

      verify(subscriber, atLeastOnce()).onNext(any[JsonObject])
      verify(subscriber).onCompleted()
    }
  }
}

