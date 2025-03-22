import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.json.JsonObject

class CustomJsonParser {
  private val jsonFactory = new JsonFactory()
  private val mapper = new ObjectMapper()

  // Helper method to check if a number exceeds the range of a long
  private def isOutOfLongRange(number: String): Boolean = {
    try {
      number.toLong // Try to parse as long
      false // If successful, the number is within the range
    } catch {
      case _: NumberFormatException => true // If parsing fails, the number is out of range
    }
  }

  def parse(jsonString: String): JsonObject = {
    val parser = jsonFactory.createParser(jsonString)
    val jsonObject = new JsonObject()

    while (parser.nextToken() != null) {
      parser.currentToken() match {
        case JsonToken.FIELD_NAME =>
          val fieldName = parser.getCurrentName
          parser.nextToken() // Move to the value
          parser.currentToken() match {
            case JsonToken.VALUE_NUMBER_INT | JsonToken.VALUE_NUMBER_FLOAT =>
              // Handle numbers
              val numericValue = parser.getText
              if (isOutOfLongRange(numericValue)) {
                // Convert to BigInteger if the number is out of range
                try {
                  val bigIntValue = new BigInteger(numericValue)
                  jsonObject.put(fieldName, bigIntValue.toString)
                } catch {
                  case e: NumberFormatException =>
                    throw new RuntimeException(s"Failed to parse large numeric value: $numericValue", e)
                }
              } else {
                // Leave the number as-is if it's within the range of a long
                jsonObject.put(fieldName, numericValue)
              }
            case JsonToken.START_OBJECT =>
              // Handle nested JSON objects
              val nestedObject = mapper.readValue[JsonObject](parser, classOf[JsonObject])
              jsonObject.put(fieldName, nestedObject)
            case _ =>
              // Handle other types (e.g., strings, booleans)
              jsonObject.put(fieldName, parser.getValueAsString)
          }
        case _ =>
        // Ignore other tokens
      }
    }

    jsonObject
  }
}