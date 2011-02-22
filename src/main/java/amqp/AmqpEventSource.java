/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package amqp;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.util.CommandLineParser;
import com.cloudera.util.Pair;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to consume message from an AMQP broker and create {@link Event}s from the raw
 * body of the message. Uses RabbitMQ's AQMP client to connect to said broker.
 * <p/>
 * Note that the majority of the work is done in {@link AmqpConsumer}.
 */
public class AmqpEventSource extends EventSource.Base {

  private static final Logger LOG = LoggerFactory.getLogger(AmqpEventSource.class);

  /**
   * Time to wait in between polls for event
   *
   * @see #next()
   */
  private static final int WAIT_IN_MILLS = 100;

  private final AmqpConsumer consumer;

  public AmqpEventSource(String host, int port, String virtualHost, String userName, String password,
                         String exchangeName, String exchangeType, boolean durableExchange,
                         String queueName, boolean durable, boolean exclusive, boolean autoDelete,
                         boolean useMessageTimestamp, String... bindings) {
    consumer = new AmqpConsumer(host, port, virtualHost, userName, password,
        exchangeName, exchangeType, durableExchange, queueName, durable, exclusive, autoDelete, useMessageTimestamp, bindings);
  }

  public AmqpEventSource(ConnectionFactory connectionFactory, String exchangeName, String queueName, String... bindings) {
    consumer = new AmqpConsumer(connectionFactory, exchangeName, queueName, bindings);
  }

  @Override
  public void close() throws IOException {
    if (!consumer.isRunning()) {
      LOG.warn("AmqpEventSource is already closed. Ignoring second close.");
    } else {
      consumer.stopConsumer();
    }
  }

  @Override
  public void open() throws IOException {
    if (consumer.isRunning()) {
      LOG.warn("AmqpEventSource is already open. Ignoring second open.");
    } else {
      consumer.startConsumer();
    }
  }

  /**
   * This method will return the next {@link Event} available from the configured AMQP Queue blocking until
   * one is available. Note that this method will return early if the thread from which next is called
   * is interrupted.
   *
   * @return event or null if we are interrupted or closed
   * @throws IOException
   */
  public Event next() throws IOException {
    Event event = null;

    // as long as the consumer is running, or has pending events, we need to drain them
    while ((consumer.isRunning() || consumer.hasPendingEvents()) && event == null) {
      try {
        event = consumer.getNextEvent(WAIT_IN_MILLS, TimeUnit.MILLISECONDS);

        if (event != null) {
          updateEventProcessingStats(event);
        }
      } catch (InterruptedException e) {
        // someone interrupted us - return null event
      }
    }

    return event;
  }

  public static SourceFactory.SourceBuilder builder() {
    return new SourceFactory.SourceBuilder() {
      @Override
      public EventSource build(String...args) {
        return build(null, args);        
      }

      @Override
      public EventSource build(Context ctx, String... args) {
        if (args.length < 1 || args.length > 13) {
          throw new IllegalArgumentException(
              "amqp(exchangeName=\"exchangeName\" " +
                  "[,host=\"host\"] " +
                  "[,port=port] " +
                  "[,virtualHost=\"virtualHost\"] " +
                  "[,userName=\"user\"] " +
                  "[,password=\"password\"] " +
                  "[,exchangeType=\"direct\"] " +
                  "[,durableExchange=false] " +
                  "[,queueName=\"queueName\"] " +
                  "[,durableQueue=false] " +
                  "[,exclusiveQueue=false] " +
                  "[,autoDeleteQueue=false] " +
                  "[,bindings=\"binding1,binding2,bindingN\"] " +
                  "[,useMessageTimestamp=false])");
        }

        CommandLineParser parser = new CommandLineParser(args);

        String host = parser.getOptionValue("host", ConnectionFactory.DEFAULT_HOST);
        int port = parser.getOptionValue("port", ConnectionFactory.DEFAULT_AMQP_PORT);
        String virtualHost = parser.getOptionValue("virtualHost", ConnectionFactory.DEFAULT_VHOST);
        String userName = parser.getOptionValue("userName", ConnectionFactory.DEFAULT_USER);
        String password = parser.getOptionValue("password", ConnectionFactory.DEFAULT_PASS);
        String exchangeName = parser.getOptionValue("exchangeName");
        String exchangeType = parser.getOptionValue("exchangeType", AmqpConsumer.DEFAULT_EXCHANGE_TYPE);
        boolean durableExchange = parser.getOptionValue("durableExchange", true);
        String queueName = parser.getOptionValue("queueName");
        boolean durableQueue = parser.getOptionValue("durableQueue", false);
        boolean exclusiveQueue = parser.getOptionValue("exclusiveQueue", false);
        boolean autoDeleteQueue = parser.getOptionValue("autoDeleteQueue", false);
        String[] bindings = parser.getOptionValues("bindings");
        boolean useMessageTimestamp = parser.getOptionValue("useMessageTimestamp", false);

        // exchange name is the only required parameter
        if (exchangeName == null) {
          throw new IllegalArgumentException("exchangeName must be set for AMQP source");
        }

        return new AmqpEventSource(host, port, virtualHost, userName, password,
            exchangeName, exchangeType, durableExchange, queueName, durableQueue,
            exclusiveQueue, autoDeleteQueue, useMessageTimestamp, bindings);
      }
    };
  }

  /**
   * This is a special function used by the SourceFactory to pull in this class
   * as a plugin source.
   */
  public static List<Pair<String, SourceFactory.SourceBuilder>> getSourceBuilders() {
    List<Pair<String, SourceFactory.SourceBuilder>> builders =
        new ArrayList<Pair<String, SourceFactory.SourceBuilder>>();
    builders.add(new Pair<String, SourceFactory.SourceBuilder>("amqp", builder()));
    return builders;
  }
}
