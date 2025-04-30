package com.example.jsonstream

import com.example.jsonstream.JacksonStreamingParser
import io.vertx.core.json.JsonObject
import org.mockito.MockitoSugar
import org.scalatest.funspec.AnyFunSpec
import rx.lang.scala.Subscriber

import java.io.InputStream

class JacksonStreamingParserTest extends AnyFunSpec with MockitoSugar {

  describe("JacksonStreamingParser") {

    it("should stream and process all JSON objects from a resource file") {
      // Load your existing file from src/test/resources
      val resourceStream: InputStream =
        getClass.getClassLoader.getResourceAsStream("your-large-json-file.json")

      assert(resourceStream != null, "Test file not found in resources")

      val subscriber = mock[Subscriber[JsonObject]]

      // Stream and parse the file
      JacksonStreamingParser.parseLargeJsonArray(resourceStream, subscriber)

      // You can change the expected count below based on your file size
      verify(subscriber, atLeastOnce()).onNext(any[JsonObject])
      verify(subscriber).onCompleted()
    }
  }
}

