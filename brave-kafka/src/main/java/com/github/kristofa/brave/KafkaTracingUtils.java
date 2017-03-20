
/**
 * Copyright 2016-2017 SmartThings
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.github.kristofa.brave;


import com.twitter.zipkin.gen.Endpoint;
import com.twitter.zipkin.gen.Span;

public class KafkaTracingUtils {

  public static SpanId reportClientSentSpan(
    ClientTracer clientTracer,
    String spanName,
    Endpoint server,
    Integer partition) {
    SpanId spanId = clientTracer.startNewSpan(spanName);
    if (spanId != null) {
      clientTracer.setClientSent(server);
      if (partition != null) {
        clientTracer.submitBinaryAnnotation("kafka.partition", partition.toString());
      }
      ClientSpanThreadBinder clientSpanThreadBinder = clientTracer.currentSpan();
      Span currentClientSpan = clientSpanThreadBinder.getCurrentClientSpan();
      if (currentClientSpan != null) {
        clientTracer.recorder().flush(currentClientSpan);
      }
      clientSpanThreadBinder.setCurrentSpan(null);
    }
    return spanId;
  }

  public static void reportServerReceivedSpan(
    ServerTracer serverTracer,
    SpanId traceContext,
    String spanName,
    Endpoint client,
    Integer partition) {
    if (traceContext != null) {
      serverTracer.setStateCurrentTrace(traceContext, spanName);
      serverTracer.setServerReceived(client);
      if (partition != null) {
        serverTracer.submitBinaryAnnotation("kafka.partition", partition.toString());
      }
      serverTracer.recorder().flush(serverTracer.currentSpan().get());
      serverTracer.clearCurrentSpan();
    }
  }
}
