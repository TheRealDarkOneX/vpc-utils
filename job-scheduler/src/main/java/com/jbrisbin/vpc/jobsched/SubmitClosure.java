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
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class SubmitClosure extends Closure {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Autowired
  private RabbitAdmin rabbitAdmin;
  @Autowired
  private RabbitTemplate rabbitTemplate;
  private ObjectMapper mapper = new ObjectMapper();

  public SubmitClosure(Object owner) {
    super(owner);
  }

  @Override
  public Object call(Object[] args) {
    log.debug("args: " + args);
    String exch = args[0].toString();
    String route = args[1].toString();
    final Object body = args[2];
    Map headers = null;

    final BlockingQueue<Object> resultsQueue = new LinkedBlockingQueue<Object>();
    Queue replyQueue = rabbitAdmin.declareQueue();
    SimpleMessageListenerContainer listener = new SimpleMessageListenerContainer();
    listener.setQueues(replyQueue);
    if (args.length > 3) {
      for (int i = 3; i <= args.length; i++) {
        if (args[i] instanceof MessageListener) {
          MessageListener callback = (MessageListener) args[3];
          listener.setMessageListener(callback);
        } else if (args[i] instanceof Map) {
          headers = (Map) args[i];
        }
      }
    } else {
      listener.setMessageListener(new MessageListener() {
        public void onMessage(Message message) {
          byte[] body = message.getBody();
          try {
            resultsQueue.add(mapper.readValue(body, 0, body.length, Map.class));
          } catch (IOException e) {
            log.error(e.getMessage(), e);
          }
        }
      });
    }

    final Map msgHdrs = headers;
    rabbitTemplate.send(exch, route, new MessageCreator() {
      public Message createMessage() {
        MessageProperties props = new RabbitMessageProperties();
        props.setContentType("application/json");
        if (null != msgHdrs) {
          props.getHeaders().putAll(msgHdrs);
        }
        String uuid = UUID.randomUUID().toString();
        props.setCorrelationId(uuid.getBytes());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
          mapper.writeValue(out, body);
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
        Message msg = new Message(out.toByteArray(), props);
        return msg;
      }
    });

    Object results = null;
    try {
      results = resultsQueue.poll(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }
    listener.stop();

    return results;
  }
}
