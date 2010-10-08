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

import com.jbrisbin.vpc.jobsched.mapred.MapReduceClosure;
import com.jbrisbin.vpc.jobsched.mapred.ReplyClosure;
import com.jbrisbin.vpc.zk.GroovyZooKeeperHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class ClosureFactory {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Autowired
  private ConnectionFactory connectionFactory;
  @Autowired
  private RabbitAdmin rabbitAdmin;
  @Autowired
  RabbitTemplate rabbitTemplate;

  private String mapreduceExchange;
  private String mapreduceControlExchange;
  private String mapRoutingKey;
  private String reduceRoutingKey;
  private String mapreduceSecurityKey;
  private String zooKeeperUrl;

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

  public String getZooKeeperUrl() {
    return zooKeeperUrl;
  }

  public void setZooKeeperUrl(String zooKeeperUrl) {
    this.zooKeeperUrl = zooKeeperUrl;
  }

  public MapReduceClosure createMapReduceClosure(Object owner) {
    MapReduceClosure cl = new MapReduceClosure(owner);
    cl.setMapreduceExchange(mapreduceExchange);
    cl.setMapreduceControlExchange(mapreduceControlExchange);
    cl.setMapreduceSecurityKey(mapreduceSecurityKey);
    cl.setMapRoutingKey(mapRoutingKey);
    cl.setReduceRoutingKey(reduceRoutingKey);
    cl.setRabbitAdmin(rabbitAdmin);
    cl.setRabbitTemplate(rabbitTemplate);
    try {
      cl.setZookeeper(new GroovyZooKeeperHelper(zooKeeperUrl));
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }

    return cl;
  }

  public ListenClosure createListenClosure(Object owner) {
    ListenClosure cl = new ListenClosure(owner);
    cl.setConnectionFactory(connectionFactory);
    cl.setRabbitAdmin(rabbitAdmin);
    cl.setRabbitTemplate(rabbitTemplate);
    return cl;
  }

  public ReplyClosure createReplyClosure(Object owner) {
    ReplyClosure cl = new ReplyClosure(owner);
    cl.setRabbitTemplate(rabbitTemplate);
    return cl;
  }

}
