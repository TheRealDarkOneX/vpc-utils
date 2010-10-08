/*
 * Copyright 2010 by J. Brisbin <jon@jbrisbin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jbrisbin.vpc.jobsched;

import groovy.lang.Closure;
import groovy.lang.GroovyObjectSupport;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class ListenClosure extends Closure {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private ConnectionFactory connectionFactory;
  private RabbitAdmin rabbitAdmin;
  private RabbitTemplate rabbitTemplate;
  private SimpleMessageListenerContainer listener = null;
  private ObjectMapper mapper = new ObjectMapper();

  public ListenClosure(Object owner) {
    super(owner);
  }

  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  public void setConnectionFactory(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  public RabbitAdmin getRabbitAdmin() {
    return rabbitAdmin;
  }

  public void setRabbitAdmin(RabbitAdmin rabbitAdmin) {
    this.rabbitAdmin = rabbitAdmin;
  }

  public RabbitTemplate getRabbitTemplate() {
    return rabbitTemplate;
  }

  public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
    this.rabbitTemplate = rabbitTemplate;
  }

  /**
   * Tie a Groovy closure to a message callback.
   *
   * @param args
   * @return The BlockingQueue to poll for results
   */
  @Override
  public Object call(Object[] args) {
    final Closure callback = (args.length > 0 && args[0] instanceof Closure ? (Closure) args[0] : null);
    if (null == callback) {
      throw new IllegalStateException(
          "You must provide a callback to listen() (otherwise, what's the point?)");
    }
    Queue replyQueue = rabbitAdmin.declareQueue();
    listener = new SimpleMessageListenerContainer(connectionFactory);
    listener.setQueues(replyQueue);
    final Listener helper = new Listener(replyQueue.getName());
    listener.setMessageListener(new MessageListener() {
      public void onMessage(Message message) {
        byte[] body = message.getBody();
        try {
          Object obj = mapper.readValue(body, 0, body.length, Map.class);
          helper.getResultsQueue().add(callback.call(new Object[]{obj, message.getMessageProperties()}));
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
        if ("end".equals(message.getMessageProperties().getType())) {
          helper.getResultsQueue().add(false);
        }
      }
    });
    listener.start();

    return helper;
  }

  public class Listener extends GroovyObjectSupport {

    private String queueName;
    private BlockingQueue<Object> resultsQueue = new LinkedBlockingQueue<Object>();

    public Listener(String queueName) {
      this.queueName = queueName;
    }

    public String getQueueName() {
      return queueName;
    }

    public BlockingQueue<Object> getResultsQueue() {
      return resultsQueue;
    }

    public void close() {
      if (null != listener) {
        listener.stop();
      }
    }
  }
}
