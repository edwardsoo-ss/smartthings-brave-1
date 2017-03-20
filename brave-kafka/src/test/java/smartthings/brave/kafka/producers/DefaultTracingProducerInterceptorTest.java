
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
package smartthings.brave.kafka.producers;

import brave.propagation.TraceContext;
import com.github.kristofa.brave.*;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.twitter.zipkin.gen.Endpoint;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import smartthings.brave.kafka.EnvelopeProtos;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;

public class DefaultTracingProducerInterceptorTest {

  private final TestClientTracer clientTracer = mock(TestClientTracer.class);
  private final SpanNameProvider<String> nameProvider = mock(SpanNameProvider.class);
  private final Endpoint endpoint = Endpoint.builder().serviceName("test-service").build();
  private final ClientSpanState clientSpanState = mock(ClientSpanState.class);
  private final com.twitter.zipkin.gen.Span span = mock(com.twitter.zipkin.gen.Span.class);
  private final TestRecorder recorder = mock(TestRecorder.class);

  private DefaultTracingProducerInterceptor<String> interceptor;

  @Before
  public void setUp() {
    interceptor = new DefaultTracingProducerInterceptor<>();
    interceptor.configure(ImmutableMap.of(
      "brave.client.tracer", clientTracer,
      "brave.span.name.provider", nameProvider,
      "brave.span.remote.endpoint", endpoint
    ));
  }

  @Test
  public void testOnSend() throws InvalidProtocolBufferException {
    String topic = "my-topic";
    String key = "ayyy";
    byte[] value = "lmao".getBytes();
    long traceId = UUID.randomUUID().getLeastSignificantBits();
    long traceIdHigh = UUID.randomUUID().getLeastSignificantBits();
    long spanId = UUID.randomUUID().getLeastSignificantBits();
    String spanName = "span-name";
    TraceContext traceContext = TraceContext.newBuilder()
      .traceIdHigh(traceIdHigh)
      .traceId(traceId)
      .spanId(spanId)
      .shared(false)
      .parentId(traceId)
      .build();


    SpanId spanIdCtx = SpanUtils.traceContextToSpanId(traceContext);

    when(nameProvider.spanName(any())).thenReturn(spanName);
    when(clientTracer.startNewSpan(spanName)).thenReturn(spanIdCtx);
    when(clientTracer.currentSpan()).thenReturn(new ClientSpanThreadBinder(clientSpanState));
    when(clientTracer.recorder()).thenReturn(recorder);
    when(clientSpanState.getCurrentClientSpan()).thenReturn(span);

    // method under test
    ProducerRecord<String, byte[]> injectedRecord = interceptor
      .onSend(new ProducerRecord<>(topic, null, null, key, value));

    EnvelopeProtos.Envelope envelope = EnvelopeProtos.Envelope.parseFrom(injectedRecord.value());
    assertEquals(topic, injectedRecord.topic());
    assertEquals(null, injectedRecord.partition());

    assertEquals(traceId, envelope.getTraceContext().getTraceId());
    assertEquals(traceIdHigh, envelope.getTraceContext().getTraceIdHigh());
    assertEquals(traceId, envelope.getTraceContext().getParentId().getValue());
    assertEquals(ByteString.copyFrom(value), envelope.getPayload());
    assertNotEquals(traceId, envelope.getTraceContext().getSpanId());
    assert(envelope.getTraceContext().getSpanId() != 0);

    verify(recorder).flush(span);
  }
}
