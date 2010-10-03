package com.jbrisbin.vpc.jobsched;

import groovy.lang.Closure;
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
import java.util.LinkedHashMap;
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
    final BlockingQueue<Object> resultsQueue = new LinkedBlockingQueue<Object>();

    Queue replyQueue = rabbitAdmin.declareQueue();
    SimpleMessageListenerContainer listener = new SimpleMessageListenerContainer(
        connectionFactory);
    listener.setQueues(replyQueue);
    listener.setMessageListener(new MessageListener() {
      public void onMessage(Message message) {
        byte[] body = message.getBody();
        try {
          Object obj = mapper.readValue(body, 0, body.length, Map.class);
          resultsQueue.add(callback.call(new Object[]{obj, message.getMessageProperties()}));
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
        if ("end".equals(message.getMessageProperties().getType())) {
          resultsQueue.add(false);
        }
      }
    });
    listener.start();

    Map<String, Object> rtnVal = new LinkedHashMap<String, Object>();
    rtnVal.put("results", resultsQueue);
    rtnVal.put("queue", replyQueue.getName());
    return rtnVal;
  }
}
