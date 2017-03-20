
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
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.KafkaTracingUtils;
import com.github.kristofa.brave.SpanId;
import com.github.kristofa.brave.SpanUtils;
import com.twitter.zipkin.gen.Endpoint;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

/**
 * An abstract interceptor that annotates and flush "ws" for each producer record,
 * and injects the tracing context by mutating {@link ProducerRecord} before it is sent.
 * Injection is left abstract to allow implementations for different strategies.
 *
 * Required: A {@link ClientTracer} in config as "brave.client.tracer" to create child span instead of new.
 * Optional: A {@link SpanNameProvider} in config as "brave.span.name.provider" to customize span name.
 * Optional: A {@link Endpoint} in config as "brave.span.remote.endpoint" to customize span remote endpoint.
 * @param <K> key type
 */
public abstract class BaseTracingProducerInterceptor<K> implements ProducerInterceptor<K, byte[]> {

  protected abstract ProducerRecord<K, byte[]> getTracedProducerRecord(TraceContext traceContext,
                                                                       ProducerRecord<K, byte[]> originalRecord);

  private ClientTracer clientTracer;
  private SpanNameProvider<K> nameProvider;
  private com.twitter.zipkin.gen.Endpoint remoteEndpoint;

  @Override
  public ProducerRecord<K, byte[]> onSend(ProducerRecord<K, byte[]> record) {
    SpanId spanId = KafkaTracingUtils.reportClientSentSpan(
      clientTracer,
      nameProvider.spanName(record),
      remoteEndpoint,
      record.partition()
    );
    TraceContext ctx = SpanUtils.spanIdToTraceContext(spanId);
    return getTracedProducerRecord(ctx, record);
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configs.get("brave.client.tracer") != null
      && configs.get("brave.client.tracer") instanceof ClientTracer) {
      clientTracer = (ClientTracer) configs.get("brave.client.tracer");
    } else {
      throw new ConfigException("brave.client.tracer", configs.get("brave.client.tracer"), "Must an be instance of com.github.kristofa.brave.ClientTracer");
    }

    if (configs.get("brave.span.name.provider") != null
      && configs.get("brave.span.name.provider") instanceof SpanNameProvider) {
      nameProvider = (SpanNameProvider<K>) configs.get("brave.span.name.provider");
    } else {
      nameProvider = new DefaultSpanNameProvider<>();
    }

    if (configs.get("brave.span.remote.endpoint") != null
      && configs.get("brave.span.remote.endpoint") instanceof Endpoint) {
      remoteEndpoint = (Endpoint) configs.get("brave.span.remote.endpoint");
    } else {
      remoteEndpoint = Endpoint.builder().serviceName("Kafka").build();
    }
  }
}
