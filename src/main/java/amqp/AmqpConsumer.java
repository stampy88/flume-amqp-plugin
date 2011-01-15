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

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Subclasses of {@link AmqpClient} that will bind to a queue and collect
 * messages from said queue and create {@link Event}s from them. These events are made available via the
 * blocking method {@link #getNextEvent(long, java.util.concurrent.TimeUnit)} ()}.
 * <p/>
 * The cancellation/shutdown policy for the consumer is to either interrupt the thread, or set the
 * {@link #running} flag to false.
 *
 * @see AmqpClient
 */
class AmqpConsumer extends AmqpClient implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(AmqpConsumer.class);

  private static final long MAX_BODY_SIZE = FlumeConfiguration.get().getEventMaxSizeBytes();

  private static final String DIRECT_EXCHANGE = "direct";
  static final String DEFAULT_EXCHANGE_TYPE = DIRECT_EXCHANGE;

  /**
   * Exchange level properties
   */
  private final String exchangeName;
  private String exchangeType = DEFAULT_EXCHANGE_TYPE;
  /**
   * true if we are declaring a durable exchange (the exchange will survive a server restart)
   */
  private boolean durableExchange;


  /**
   * Queue level properties
   */

  /**
   * if left unspecified, the server chooses a name and provides this to the client. Generally, when
   * applications share a message queue they agree on a message queue name beforehand, and when an
   * application needs a message queue for its own purposes, it lets the server provide a name.
   */
  private String queueName;
  /**
   * if set, the message queue remains present and active when the server restarts. It may lose transient
   * messages if the server restarts.
   */
  private boolean durable;
  /**
   * if set, the queue belongs to the current connection only, and is deleted when the connection closes.
   */
  private boolean exclusive;
  /**
   * true if we are declaring an autodelete queue (server will delete it when no longer in use)
   */
  private boolean autoDelete;
  private final String[] bindings;

  private Channel channel;

  private final BlockingQueue<Event> events = new LinkedBlockingQueue<Event>();

  private Thread consumerThread;

  private volatile CountDownLatch startSignal;

  /**
   * This exception will be populated in the case where there was an unexpected shutdown error. This can
   * happen when trying to declare an exchange, queue, etc. that already exists but with different attributes, e.g.
   * durable, exchange type.
   * <p/>
   * This will be thrown from {@link #getNextEvent(long, java.util.concurrent.TimeUnit)} if not null
   */
  private volatile ShutdownSignalException exception;

  public AmqpConsumer(String host, int port, String virutalHost, String userName, String password,
                      String exchangeName, String exchangeType, boolean durableExchange,
                      String queueName, boolean durable, boolean exclusive, boolean autoDelete, String... bindings) {
    super(host, port, virutalHost, userName, password);

    this.exchangeName = exchangeName;
    this.exchangeType = exchangeType;
    this.durableExchange = durableExchange;
    this.queueName = queueName;
    this.durable = durable;
    this.exclusive = exclusive;
    this.autoDelete = autoDelete;
    this.bindings = bindings;
  }

  public AmqpConsumer(ConnectionFactory connectionFactory, String exchangeName, String queueName, String... bindings) {
    super(connectionFactory);

    this.exchangeName = exchangeName;
    this.queueName = queueName;
    this.bindings = bindings;
  }

  /**
   * Returns the next event in the {@link #events} queue, or null if it timeouts out before an event arrives.
   * Note that once the queue is empty, and an {@link #exception} has occured, this method will throw said
   * exception.
   *
   * @param timeout how long to wait before giving up, in units of
   *                <tt>unit</tt>
   * @param unit    a <tt>TimeUnit</tt> determining how to interpret the
   *                <tt>timeout</tt> parameter
   * @return the head of this queue, or <tt>null</tt> if the
   *         specified waiting time elapses before an element is available
   * @throws InterruptedException if interrupted while waiting
   * @throws com.rabbitmq.client.ShutdownSignalException
   *                              if there was an unexpected shutdown of the consumer
   */
  public Event getNextEvent(long timeout, TimeUnit unit) throws InterruptedException, ShutdownSignalException {
    Event e = events.poll(timeout, unit);

    if (e == null && exception != null) {
      throw exception;
    }

    return e;
  }

  /**
   * Returns true if they are events in the {@link #events} queue or if the {@link #exception} is set. This is
   * included in the check because we want the subsequent {@link #getNextEvent(long, java.util.concurrent.TimeUnit)}
   * to throw the exception when the queue is emptied.
   *
   * @return true if there are pending events
   */
  public boolean hasPendingEvents() {
    return events.size() > 0 || exception != null;
  }

  /**
   * This method will start this consumer's {@link #consumerThread} which will start the consumption of
   * messages. This method blocks until the {@link #startSignal} is set in the {@link #run()} method
   * signaling that the thread has actually started.
   *
   * @throws IllegalStateException throw if the consumer is already running
   */
  void startConsumer() throws IllegalStateException {
    if (isRunning()) {
      throw new IllegalStateException("AmqpConsumer is already started");
    }

    // need a new start signal
    startSignal = new CountDownLatch(1);

    consumerThread = new Thread(this, "AmqpConsumer");
    consumerThread.start();

    try {
      // wait until the thread has start
      startSignal.await();
    } catch (InterruptedException e) {
      // someone interrupted us, take this as a shutdown signal
    }
  }

  /**
   * This method will stop this consumer's {@link #consumerThread} and block until it exits.
   *
   * @throws IllegalStateException throw if the consumer is not running
   */
  void stopConsumer() {
    if (!isRunning()) {
      throw new IllegalStateException("AmqpConsumer is not running");
    }

    setRunning(false);
    consumerThread.interrupt();
    try {
      consumerThread.join();
    } catch (InterruptedException e) {
      // someone interrupted us, take this as a shutdown signal
    }
    consumerThread = null;
  }

  @Override
  public void run() {
    LOG.info("AMQP Consumer is starting...");
    setRunning(true);
    // signal that we are running - this will unblock the startConsumer method
    startSignal.countDown();

    try {
      runConsumeLoop();
    } finally {
      closeChannelSilently(channel);
    }

    LOG.info("AMQP Consumer has shut down successfully.");
  }

  /**
   * Main run loop for consuming messages
   */
  private void runConsumeLoop() {
    QueueingConsumer consumer = null;
    Thread currentThread = Thread.currentThread();

    while (isRunning() && !currentThread.isInterrupted()) {

      try {
        if (channel == null) {
          // this will block until a channel is established or we are told to shutdown
          channel = getChannel();

          if (channel == null) {
            // someone set the running flag to false
            break;
          }

          // make declarations for consumer
          String queueName = declarationsForChannel(channel);

          consumer = new QueueingConsumer(channel);
          boolean noAck = false;
          String consumerTag = channel.basicConsume(queueName, noAck, consumer);
          LOG.info("Starting new consumer. Server generated {} as consumerTag", consumerTag);
        }

        // this blocks until a message is ready
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();

        byte[] body = delivery.getBody();
        if (body != null) {
          if(body.length > MAX_BODY_SIZE) {
            LOG.warn("Received message with body size of {} which is above the {} of {}, ignoring message",
               new Object[]{body.length, FlumeConfiguration.EVENT_MAX_SIZE, MAX_BODY_SIZE});
          } else {
            // create a new flume event based on the message body
            Event event = new EventImpl(delivery.getBody());

            // add to queue
            events.add(event);
          }
        } else {
          LOG.warn("Received message with null body, ignoring message");
        }

        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      } catch (InterruptedException e) {
        LOG.info("Consumer Thread was interrupted, shutting down...");
        setRunning(false);

      } catch (IOException e) {
        if (e.getCause() instanceof ShutdownSignalException) {
          logShutdownSignalException((ShutdownSignalException) e.getCause());
          setRunning(false);
        } else {
          LOG.info("IOException caught in Consumer Thread. Closing channel and waiting to reconnect", e);
          closeChannelSilently(channel);
          channel = null;
        }

      } catch (ShutdownSignalException e) {
        logShutdownSignalException(e);
        setRunning(false);
      }
    }

    LOG.info("Exited runConsumeLoop with running={} and interrupt status={}", isRunning(), currentThread.isInterrupted());
  }

  /**
   * Will log the specified exception and will set the {@link #exception} field if the
   * {@link com.rabbitmq.client.ShutdownSignalException#isInitiatedByApplication()} is false meaning
   * that the shutdown wasn't client initiated.
   *
   * @param e exception
   */
  private void logShutdownSignalException(ShutdownSignalException e) {
    if (e.isInitiatedByApplication()) {
      LOG.info("Consumer Thread caught ShutdownSignalException, shutting down...");
    } else {
      LOG.error("Unexpected ShutdownSignalException caught in Consumer Thread , shutting down...", e);
      this.exception = e;
    }
  }

  /**
   * This method declares the exchange, queue and bindings needed by this consumer.
   * The method returns the queue name that will be consumed from by this class.
   *
   * @param channel channel used to issue AMQP commands
   * @return queue that will have messages consumed from
   * @throws IOException thrown if there is any communication exception
   */
  protected String declarationsForChannel(Channel channel) throws IOException {
    // setup exchange, queue and binding
    channel.exchangeDeclare(exchangeName, exchangeType, durableExchange);
    // named queue?
    if (queueName == null) {
      queueName = channel.queueDeclare().getQueue();

    } else {
      channel.queueDeclare(queueName, durable, exclusive, autoDelete, null);
    }

    if (bindings != null) {
      // multiple bindings
      for (String binding : bindings) {
        channel.queueBind(queueName, exchangeName, binding);
      }
    } else {
      // no binding given - this could be the case if it is a fanout exchange
      channel.queueBind(queueName, exchangeName, SERVER_GENERATED_QUEUE_NAME);
    }

    return queueName;
  }
}
