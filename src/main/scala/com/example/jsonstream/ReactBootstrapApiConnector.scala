def retryWithExponentialBackoff[T](
                                    source: Observable[T],
                                    backoff: ExponentialBackoff,
                                    retryKey: String
                                  ): Observable[T] = {
  source.retryWhen { errors =>
    errors.flatMap { ex =>
      val retryAllowed = backoff.isRetryAllowedWithWaitIntervalUntilSeconds(retryKey)
      if (retryAllowed.allowed) {
        println(s"[Retry] Attempt #${retryAllowed.retriesAttempted}, retrying in ${retryAllowed.waitIntervalMs} ms")
        Observable.timer(Duration(retryAllowed.waitIntervalMs, MILLISECONDS))
      } else {
        println(s"[Give Up] Max retry attempts reached for $retryKey")
        Observable.error(new RuntimeException(s"Retry exhausted for $retryKey: ${ex.getMessage}", ex))
      }
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
