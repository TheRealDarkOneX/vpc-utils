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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.AbstractRabbitConfiguration;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@Configuration
@ImportResource("classpath:/common.xml")
public class QueueConfiguration extends AbstractRabbitConfiguration {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Value("${mq.host}")
  private String brokerHost;
  @Value("${mq.port}")
  private Integer brokerPort;
  @Value("${mq.user}")
  private String brokerUser;
  @Value("${mq.pass}")
  private String brokerPassword;
  @Value("${mq.vhost}")
  private String brokerVirtualHost;

  @Value("${jobsched.exchange.name}")
  private String exchangeName = "jobsched";

  @Value("${jobsched.sql_queue.name}")
  private String sqlQueueName = "sql";
  @Value("${jobsched.sql_route.name}")
  private String sqlRouteName = "sql.request";

  @Value("${jobsched.requeue_queue.name}")
  private String requeueQueueName = "requeue";
  @Value("${jobsched.requeue_route.name}")
  private String requeueRouteName = "requeue";

  @Value("${jobsched.exe_queue.name}")
  private String exeQueueName;
  @Value("${jobsched.exe_route.name}")
  private String exeRouteName;

  @Value("${jobsched.batch_queue.name}")
  private String batchQueueName;
  @Value("${jobsched.batch_route.name}")
  private String batchRouteName;

  @Value("${mapred.exchange.name}")
  private String mapreduceExchName;
  @Value("${mapred_control.exchange.name}")
  private String mapreduceControlExchName;
  @Value("${mapred.map.queue}")
  private String mapreduceMapQueueName;
  @Value("${mapred.map.route}")
  private String mapreduceMapRouteName;
  @Value("${mapred.reduce.queue}")
  private String mapreduceReduceQueueName;
  @Value("${mapred.reduce.route}")
  private String mapreduceReduceRouteName;

  private SingleConnectionFactory connectionFactory;

  @Bean
  public ConnectionFactory connectionFactory() {
    if (null == connectionFactory) {
      com.rabbitmq.client.ConnectionFactory rabbitFactory = new com.rabbitmq.client.ConnectionFactory();
      rabbitFactory.setHost(brokerHost);
      rabbitFactory.setPort(brokerPort);
      rabbitFactory.setUsername(brokerUser);
      rabbitFactory.setPassword(brokerPassword);
      rabbitFactory.setVirtualHost(brokerVirtualHost);
      connectionFactory = new SingleConnectionFactory(rabbitFactory);
    }
    return connectionFactory;
  }

  @Bean
  public TopicExchange exchange() {
    TopicExchange x = new TopicExchange(exchangeName, true, false);
    return x;
  }

  @Bean
  public Queue sqlQueue() {
    Queue q = new Queue(sqlQueueName);
    q.setDurable(true);
    q.setAutoDelete(false);
    return q;
  }

  @Bean
  public Binding sqlBinding() {
    Binding b = new Binding(sqlQueue(), exchange(), sqlRouteName);
    return b;
  }

  @Bean
  public Queue requeueQueue() {
    Queue q = new Queue(requeueQueueName);
    q.setDurable(true);
    q.setAutoDelete(false);
    return q;
  }

  @Bean
  public Binding requeueBinding() {
    Binding b = new Binding(requeueQueue(), exchange(), requeueRouteName);
    return b;
  }

  @Bean
  public Queue exeQueue() {
    Queue q = new Queue(exeQueueName);
    q.setDurable(true);
    q.setAutoDelete(false);
    return q;
  }

  @Bean
  public Binding exeBinding() {
    Binding b = new Binding(exeQueue(), exchange(), exeRouteName);
    return b;
  }

  @Bean
  public Queue batchQueue() {
    Queue q = new Queue(batchQueueName);
    q.setDurable(true);
    q.setAutoDelete(false);
    return q;
  }

  @Bean
  public Binding batchBinding() {
    Binding b = new Binding(batchQueue(), exchange(), batchRouteName);
    return b;
  }

  @Bean
  public TopicExchange mapreduceExchange() {
    TopicExchange x = new TopicExchange(mapreduceExchName);
    return x;
  }

  @Bean
  public FanoutExchange mapreduceControlExchange() {
    FanoutExchange x = new FanoutExchange(mapreduceControlExchName);
    return x;
  }

  @Bean
  public Queue mapreduceMapQueue() {
    Queue q = new Queue(mapreduceMapQueueName);
    return q;
  }

  @Bean
  public Queue mapreduceReduceQueue() {
    Queue q = new Queue(mapreduceReduceQueueName);
    return q;
  }

  @Bean
  public Binding mapreduceMapBinding() {
    Binding b = new Binding(mapreduceMapQueue(), mapreduceExchange(), mapreduceMapRouteName);
    return b;
  }

  @Bean
  public Binding mapreduceReduceBinding() {
    Binding b = new Binding(mapreduceReduceQueue(), mapreduceExchange(),
        mapreduceReduceRouteName);
    return b;
  }

  @Override
  public RabbitTemplate rabbitTemplate() {
    return new RabbitTemplate(connectionFactory());
  }

  @Bean
  public RabbitAdmin rabbitAdmin() {
    return new RabbitAdmin(connectionFactory());
  }


}
