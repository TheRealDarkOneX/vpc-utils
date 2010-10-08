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

import com.jbrisbin.vpc.jobsched.SecureMessageConverter;
import groovy.lang.Closure;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageCreator;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class MapReduceClosure extends Closure {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private ObjectMapper mapper = new ObjectMapper();
  private ZooKeeper zookeeper;
  private RabbitAdmin rabbitAdmin;
  private RabbitTemplate rabbitTemplate;
  private String mapreduceExchange;
  private String mapreduceControlExchange;
  private String mapRoutingKey;
  private String reduceRoutingKey;
  private String mapreduceSecurityKey;

  public MapReduceClosure(Object owner) {
    super(owner);
  }

  public ZooKeeper getZookeeper() {
    return zookeeper;
  }

  public void setZookeeper(ZooKeeper zookeeper) {
    this.zookeeper = zookeeper;
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

  public String getMapreduceExchange() {
    return mapreduceExchange;
  }

  public void setMapreduceExchange(String mapreduceExchange) {
    this.mapreduceExchange = mapreduceExchange;
  }

  public String getMapreduceControlExchange() {
    return mapreduceControlExchange;
  }

  public void setMapreduceControlExchange(String mapreduceControlExchange) {
    this.mapreduceControlExchange = mapreduceControlExchange;
  }

  public String getMapRoutingKey() {
    return mapRoutingKey;
  }

  public void setMapRoutingKey(String mapRoutingKey) {
    this.mapRoutingKey = mapRoutingKey;
  }

  public String getReduceRoutingKey() {
    return reduceRoutingKey;
  }

  public void setReduceRoutingKey(String reduceRoutingKey) {
    this.reduceRoutingKey = reduceRoutingKey;
  }

  public String getMapreduceSecurityKey() {
    return mapreduceSecurityKey;
  }

  public void setMapreduceSecurityKey(String mapreduceSecurityKey) {
    this.mapreduceSecurityKey = mapreduceSecurityKey;
  }

  @Override
  public Object call(Object[] args) {
    if (args.length != 5) {
      throw new IllegalArgumentException(
          "mapreduce() takes 5 parameters: Job id <String|byte[]>, replyTo <String>, Groovy src <String>, key <String>, value <Object>)");
    }
    final String id = args[0].toString();
    final String replyTo = (null != args[1] ? args[1].toString() : null);
    final String src = args[2].toString();
    final String key = args[3].toString();
    final Object value = args[4];

    try {
      zookeeper.exists(String.format("/%s", id), new Watcher() {
        public void process(WatchedEvent watchedEvent) {
          log.debug("exists watcher: " + watchedEvent);
        }
      });
    } catch (KeeperException e) {
      log.error(e.getMessage(), e);
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }
    rabbitTemplate.send(mapreduceExchange, mapRoutingKey, new MessageCreator() {
      public Message createMessage() {
        MessageProperties props = new RabbitMessageProperties();
        props.setContentType("application/json");
        if (null != replyTo) {
          props.setReplyTo(new Address(replyTo));
        }
        props.getHeaders().put(SecureMessageConverter.SECURITY_KEY_HDR, mapreduceSecurityKey);
        props.getHeaders().put("mapreduce.key", key);
        props.getHeaders().put("mapreduce.src", src);
        props.setCorrelationId(id.getBytes());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
          mapper.writeValue(out, value);
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
        Message msg = new Message(out.toByteArray(), props);
        log.debug("outgoing map: " + msg);
        return msg;
      }
    });
    return null;
  }
}
