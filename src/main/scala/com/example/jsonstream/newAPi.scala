import com.example.jsonstream.{MessageEnvelop, SurfaceMessage, SurfaceMessageAdapter}
import com.fasterxml.jackson.core.JsonFactory
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClient
import org.slf4j.LoggerFactory
import java.util.concurrent.{CompletableFuture, Executors}
import scala.util.{Failure, Success, Try}
import org.json.JSONObject

class ReactBootstrapApiConnector(
                                  host: String,
                                  port: Int,
                                  prefix: String,
                                  isSSL: Boolean,
                                  vertx: Vertx,
                                  surfaceMessageAdapter: SurfaceMessageAdapter
                                ) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val jsonFactory = new JsonFactory()
  private val client: HttpClient = vertx.createHttpClient()
  private val executorService = Executors.newSingleThreadExecutor()

  private val APPEND_JSON_START = "{\"surfaceResult\":{"
  private val APPEND_JSON_END = "}"
  private val splitOn = "},\\{\"surfaceResult\":\\{"
  private val REACT_BOOTSTRAP_API_RESPONSE_COMPLETED = "REACT_BOOTSTRAP_API_RESPONSE_COMPLETED"
  private val REACT_REST_API_EXCEPTION = "REACT_REST_API_EXCEPTION"

  private var buffer = new StringBuilder()

  def streamRatesFromReact(): CompletableFuture[List[String]] = {
    val promise = new CompletableFuture[List[String]]()

    client.request().onComplete { reqAr =>
      if (reqAr.succeeded()) {
        val request = reqAr.result()
        request.send().onComplete { respAr =>
          if (respAr.succeeded()) {
            val response = respAr.result()
            if (response.statusCode() == 200) {
              response.handler { b =>
                executorService.execute(() => buffer.append(b.toString))
              }.exceptionHandler { e =>
                logger.error("Error in response", e)
                promise.completeExceptionally(e)
              }.endHandler { _ =>
                val completedResponse = buffer.toString()
                val messages =
                  if (completedResponse.contains(REACT_BOOTSTRAP_API_RESPONSE_COMPLETED))
                    completedResponse.split(splitOn).toList
                  else List.empty
                promise.complete(messages)
              }
            } else {
              promise.completeExceptionally(new RuntimeException(response.statusMessage()))
            }
          } else promise.completeExceptionally(respAr.cause())
        }
      } else promise.completeExceptionally(reqAr.cause())
    }

    promise
  }

  def bootstrapReactApi(): CompletableFuture[List[MessageEnvelop[SurfaceMessage]]] = {
    streamRatesFromReact().thenApply(messages => {
      messages.map { msg =>
        if (msg.contains(REACT_REST_API_EXCEPTION)) {
          new MessageEnvelop[SurfaceMessage](false, None, None, Some("Error"))
        } else {
          Try {
            val json = new JSONObject(APPEND_JSON_START + msg + APPEND_JSON_END)
            val adapted = surfaceMessageAdapter.adapt(Array(json))
            new MessageEnvelop[SurfaceMessage](true, Some(adapted), None, Some("Success"))
          } match {
            case Success(envelop) => envelop
            case Failure(_) => new MessageEnvelop[SurfaceMessage](false, None, None, Some("Parsing Error"))
          }
        }
      }
    })
  }
}
