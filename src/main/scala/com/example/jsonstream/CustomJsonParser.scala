import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject

class CustomJsonParser {
  private val jsonFactory = new JsonFactory()
  private val mapper = new ObjectMapper()

  def parse(buffer: Buffer): Buffer = {
    val parser = jsonFactory.createParser(buffer.getBytes)
    val jsonBuffer = Buffer.buffer()

    while (parser.nextToken() != null) {
      parser.currentToken() match {
        case JsonToken.VALUE_NUMBER_INT | JsonToken.VALUE_NUMBER_FLOAT =>
          // Handle large numbers
          val numericValue = parser.getText
          try {
            val bigIntValue = new BigInteger(numericValue)
            val jsonObject = new JsonObject().put("value", bigIntValue.toString)
            jsonBuffer.appendString(jsonObject.encode())
          } catch {
            case e: NumberFormatException =>
              throw new RuntimeException(s"Failed to parse large numeric value: $numericValue", e)
          }
        case JsonToken.START_OBJECT =>
          // Handle JSON objects
          val jsonObject = mapper.readValue[JsonObject](parser, classOf[JsonObject])
          jsonBuffer.appendString(jsonObject.encode())
        case _ =>
        // Ignore other tokens
      }
    }

    jsonBuffer
  }
}