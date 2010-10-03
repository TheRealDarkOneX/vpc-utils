package com.jbrisbin.vpc.jobsched;

import com.jbrisbin.vpc.jobsched.mapred.MapReduceClosure;
import com.jbrisbin.vpc.jobsched.mapred.ReplyClosure;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class ClosureFactory {

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

  public MapReduceClosure createMapReduceClosure(Object owner) {
    MapReduceClosure cl = new MapReduceClosure(owner);
    cl.setMapreduceExchange(mapreduceExchange);
    cl.setMapreduceControlExchange(mapreduceControlExchange);
    cl.setMapreduceSecurityKey(mapreduceSecurityKey);
    cl.setMapRoutingKey(mapRoutingKey);
    cl.setReduceRoutingKey(reduceRoutingKey);
    cl.setRabbitAdmin(rabbitAdmin);
    cl.setRabbitTemplate(rabbitTemplate);
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
