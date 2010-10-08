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

package com.jbrisbin.vpc.jobsched.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class BatchMessageHandler implements ApplicationContextAware {

  private Logger log = LoggerFactory.getLogger(getClass());
  private ApplicationContext appCtx;
  @Autowired
  private ConnectionFactory connectionFactory;
  @Autowired
  private RabbitAdmin rabbitAdmin;
  @Autowired
  private RabbitTemplate template;

  public void setApplicationContext(
      ApplicationContext applicationContext) throws BeansException {
    this.appCtx = applicationContext;
  }

  public BatchMessage handleMessage(BatchMessage batch) throws Exception {
    log.debug("handling message: " + batch.toString());

    final BatchMessage results = new BatchMessage();
    results.setId(batch.getId());

    // For waiting till our results are all back
    final CountDownLatch latch = new CountDownLatch(batch.getMessages().size());

    Queue resultsQueue = rabbitAdmin.declareQueue();
    SimpleMessageListenerContainer listener = new SimpleMessageListenerContainer(
        connectionFactory);
    listener.setAutoAck(true);
    listener.setQueues(resultsQueue);
    listener.setMessageListener(new MessageListener() {
      public void onMessage(Message message) {
        String messageId = new String(message.getMessageProperties().getCorrelationId());
        String body = new String(message.getBody());
        results.getMessages().put(messageId, body);
        latch.countDown();
      }
    });
    listener.start();

    for (Map.Entry<String, String> msg : batch.getMessages().entrySet()) {
      final String[] parts = msg.getKey().split(":");
      template.send(parts[0],
          parts[1],
          new MessageCreator(parts[2], parts[3], resultsQueue.getName(),
              msg.getValue().getBytes()));
    }

    // Wait the timeout value per message for all results to be collected
    latch.await((batch.getTimeout() * batch.getMessages().size()), TimeUnit.MINUTES);

    return results;
  }

  class MessageCreator implements org.springframework.amqp.core.MessageCreator {

    String id;
    String securityKey;
    String replyTo;
    byte[] body;

    MessageCreator(String id, String securityKey, String replyTo, byte[] body) {
      this.id = id;
      this.securityKey = securityKey;
      this.replyTo = replyTo;
      this.body = body;
    }

    public Message createMessage() {
      MessageProperties props = new SimpleMessageProperties();
      props.setContentType("application/json");
      props.setReplyTo(new Address(replyTo));
      props.setCorrelationId(id.getBytes());
      props.getHeaders().put("security.key", securityKey);

      return new Message(body, props);
    }
  }
}
