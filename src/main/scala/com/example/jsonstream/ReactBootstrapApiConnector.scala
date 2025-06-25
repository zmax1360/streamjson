def retryWithExponentialBackoff[T](
                                    source: Observable[T],
                                    backoff: ExponentialBackoff,
                                    retryKey: String
                                  ): Observable[T] = {
  source.retryWhen { errors =>
    errors.flatMap {
      case _: TooManyRequestsException =>
        val (isAllowed, waitMillis) = backoff.isRetryAllowedWithWaitIntervalUntilSeconds(retryKey)
        if (isAllowed) {
          println(s"[RETRY] Waiting $waitMillis ms before retrying for $retryKey")
          Observable.timer(Duration(waitMillis, MILLISECONDS))
        } else {
          println(s"[FAILURE] Max retries exceeded for $retryKey")
          Observable.error(new RuntimeException(s"Max retries exceeded for $retryKey"))
        }

      case ex =>
        println(s"[ERROR] Not retryable: ${ex.getMessage}")
        Observable.error(ex)
    }
  }
}
case 429 =>
logger.warn("Received 429 Too Many Requests")
subscriber.onError(new TooManyRequestsException("Rate limit hit"))



def bootstrapReactApi(): Observable[MessageEnvelop[SurfaceMessage]] = {
  val backoff = ExponentialBackoff(base = 1, multiplier = 2, maxALlowedFailure = 5)
  val retryKey = "react-api"

  retryWithExponentialBackoff(streamRatesFromReact(), backoff, retryKey)
    .onBackpressureBuffer
    .observeOn(creditRiskWorkerSchedulerFactory.reactEndpointTableLoaderScheduler)
    .map { json =>
      // You can add a guard if `json` contains an "error" field
      Try(surfaceMessageAdapter.adapt(json)) match {
        case Success(surfaceMessage) =>
          new MessageEnvelop[SurfaceMessage](true, Some(surfaceMessage), None, Some("Loading"))
        case Failure(ex) =>
          new MessageEnvelop[SurfaceMessage](false, None, None, Some(REACT_REST_API_EXCEPTION))
      }
    }
    .onErrorReturn { ex =>
      // Ensure any final unexpected error is still returned downstream
      new MessageEnvelop[SurfaceMessage](false, None, None, Some(s"Final failure: ${ex.getMessage}"))
    }
}
