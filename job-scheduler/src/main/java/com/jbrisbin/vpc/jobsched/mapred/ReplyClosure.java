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

package com.jbrisbin.vpc.jobsched.mapred;

import groovy.lang.Closure;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageCreator;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class ReplyClosure extends Closure {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private RabbitTemplate rabbitTemplate;
  private ObjectMapper mapper = new ObjectMapper();

  public ReplyClosure(Object owner) {
    super(owner);
  }

  public RabbitTemplate getRabbitTemplate() {
    return rabbitTemplate;
  }

  public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
    this.rabbitTemplate = rabbitTemplate;
  }

  @Override
  public Object call(final Object obj) {
    log.debug("obj: " + obj);
    String replyTo = (String) getProperty("replyTo");
    if (null != replyTo) {
      rabbitTemplate.send("", replyTo, new MessageCreator() {
        public Message createMessage() {
          MessageProperties props = new RabbitMessageProperties();
          props.setContentType("application/json");
          props.setCorrelationId(getProperty("id").toString().getBytes());
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          try {
            mapper.writeValue(out, obj);
          } catch (IOException e) {
            log.error(e.getMessage(), e);
          }
          Message msg = new Message(out.toByteArray(), props);
          if (log.isDebugEnabled()) {
            log.debug("Sending reply: " + msg);
          }
          return msg;
        }
      });
    } else {
      log.warn("Reply requested, but replyTo was null!");
    }
    return null;
  }
}
