package com.jbrisbin.vpc.jobsched.mapred;

import com.jbrisbin.vpc.jobsched.ClosureFactory;
import com.jbrisbin.vpc.jobsched.SecureMessageConverter;
import com.jbrisbin.vpc.jobsched.util.BeanClosure;
import groovy.lang.Closure;
import groovy.lang.MissingPropertyException;
import groovy.util.GroovyScriptEngine;
import groovy.util.ResourceException;
import groovy.util.ScriptException;
import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class MapReduceMessageHandler implements ApplicationContextAware, SmartLifecycle {

  public static final int STARTING = 0;
  public static final int STARTED = 1;
  public static final int STOPPING = 2;
  public static final int STOPPED = 3;

  private final Logger log = LoggerFactory.getLogger(getClass());

  private boolean autostart = true;
  private int phase = STOPPED;
  private ObjectMapper mapper = new ObjectMapper();
  private ApplicationContext appCtx;

  @Autowired
  private RabbitAdmin rabbitAdmin;
  @Autowired
  RabbitTemplate rabbitTemplate;
  @Autowired
  private ConnectionFactory connectionFactory;
  @Autowired
  private GroovyScriptEngine groovyScriptEngine;
  @Autowired
  private BeanClosure beanClosure;
  @Autowired
  private ClosureFactory groovyClosureFactory;

  private Timer delayTimer = new Timer(true);
  private Queue controlQueue;
  private SimpleMessageListenerContainer listeners;
  private ConcurrentSkipListMap<String, Closure> contextCache = new ConcurrentSkipListMap<String, Closure>();
  private ConcurrentSkipListMap<String, SimpleMessageListenerContainer> listenerCache = new ConcurrentSkipListMap<String, SimpleMessageListenerContainer>();
  private ConcurrentSkipListMap<String, Boolean> finishedWithToken = new ConcurrentSkipListMap<String, Boolean>();
  private AtomicReference<MapReduceMessage> lastMessage = new AtomicReference<MapReduceMessage>();
  private AtomicReference<String> lastToken = new AtomicReference<String>();
  private ExecutorService workers = Executors.newCachedThreadPool();
  private String mapreduceExchange;
  private String mapreduceControlExchange;
  private String mapRoutingKey;
  private String reduceRoutingKey;
  private String mapreduceSecurityKey;

  public void setApplicationContext(
      ApplicationContext applicationContext) throws BeansException {
    this.appCtx = applicationContext;
  }

  public boolean isAutoStartup() {
    return autostart;
  }

  public void stop(Runnable callback) {
    callback.run();
  }

  public void start() {
    phase = STARTING;
    controlQueue = rabbitAdmin.declareQueue();
    FanoutExchange x = new FanoutExchange(mapreduceControlExchange);
    rabbitAdmin.declareExchange(x);
    rabbitAdmin.declareBinding(new Binding(controlQueue, x));

    listeners = new SimpleMessageListenerContainer(connectionFactory);
    listeners.setMessageListener(new MapReduceControlHandler());
    listeners.setQueues(controlQueue);
    listeners.start();

    phase = STARTED;
  }

  public void stop() {
    phase = STOPPING;
    listeners.stop();
    phase = STOPPED;
  }

  public boolean isRunning() {
    return (phase == STARTED);
  }

  public int getPhase() {
    return phase;
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

  public void handleMessage(final MapReduceMessage message) throws Exception {

    final String id = message.getId();
    final String key = message.getKey();
    final String token = id + key;

    String lt = lastToken.get();
    if (null != lt && !token.equals(lt)) {
      contextCache.remove(lt);
      onKeyChange(id, key);
    }
    lastToken.set(token);

    Closure context = contextCache.get(token);
    if (null == context) {
      // Create fresh binding for this lifetime
      context = getMapReduceClosure(message);
      EmitClosure emit = new EmitClosure(message, message.getSrc());
      context.setProperty("emit", emit);
      contextCache.put(key, context);
    }

    try {
      Closure cl = (Closure) context.getProperty(message.getType());
      try {
        cl.setProperty("onKeyChange", cl.getProperty("onKeyChange"));
      } catch (MissingPropertyException ignored) {
      }
      cl.call(new Object[]{message.getKey(), message.getData()});
    } catch (MissingPropertyException ignored) {
    } finally {
      lastMessage.set(message);
    }

  }

  private void onKeyChange(final String id, final String key) {
    final String token = id + key;
    Closure cl = contextCache.remove(token);
    Closure onKeyChange = (Closure) cl.getProperty("onKeyChange");
    onKeyChange.setProperty("key", key);
    final Object o = onKeyChange.call();
    if (null != o) {
      // Send to rereduce
      rabbitTemplate.send("", DigestUtils.md5Hex(id), new MessageCreator() {
        public Message createMessage() {
          MessageProperties props = new RabbitMessageProperties();
          props.setContentType("application/json");
          props.setCorrelationId(id.getBytes());
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          try {
            mapper.writeValue(out, o);
          } catch (IOException e) {
            log.error(e.getMessage(), e);
          }
          Message msg = new Message(out.toByteArray(), props);
          return msg;
        }
      });
    }
  }

  private Closure getMapReduceClosure(
      MapReduceMessage message) throws ScriptException, ResourceException {
    groovy.lang.Binding env = new groovy.lang.Binding();
    env.setVariable("message", message);
    Logger scriptLogger = LoggerFactory.getLogger("mapreduce." + message.getType());
    env.setVariable("log", scriptLogger);
    env.setVariable("bean", beanClosure);
    env.setVariable("reply", groovyClosureFactory.createReplyClosure(message));
    // Get Groovy script
    return (Closure) groovyScriptEngine.run(message.getSrc(), env);
  }

  private void listenForReReduce(
      MapReduceMessage msg) throws ScriptException, ResourceException {
    String hash = "rereduce." + DigestUtils.md5Hex(msg.getId());
    Queue q = new Queue(hash);
    q.setExclusive(true);
    q.setDurable(false);
    q.setAutoDelete(true);
    rabbitAdmin.declareQueue(q);
    SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(connectionFactory);
    c.setQueues(q);
    c.setMessageListener(new ReReduceListener(msg));
    c.start();
    listenerCache.put(hash, c);
  }

  private byte[] serialize(Object obj) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      mapper.writeValue(out, obj);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    return out.toByteArray();
  }

  private Object deserialize(byte[] bytes) throws IOException {
    try {
      return mapper.readValue(bytes, 0, bytes.length, List.class);
    } catch (JsonMappingException e1) {
      try {
        return mapper.readValue(bytes, 0, bytes.length, Map.class);
      } catch (JsonMappingException e2) {
        try {
          return mapper.readValue(bytes, 0, bytes.length, Float.class);
        } catch (JsonMappingException e3) {
          try {
            return mapper.readValue(bytes, 0, bytes.length, String.class);
          } catch (JsonMappingException e4) {
          }
        }
      }
    }
    return null;
  }

  class MapReduceControlHandler implements MessageListener {
    public void onMessage(final Message message) {
      String id = new String(message.getMessageProperties().getCorrelationId());
      String key = new String(message.getBody());
      String token = id + key;
      if ("END".equals(key)) {
        for (Closure cl : contextCache.values()) {
          if (id.equals(cl.getProperty("id"))) {
            contextCache.remove(cl);
            onKeyChange(id, cl.getProperty("key").toString());
          }
        }
      }
    }
  }

  class ReReduceListener implements MessageListener {

    String id;
    String replyTo;
    List<Object> results = new ArrayList<Object>();
    Closure cl;

    ReReduceListener(MapReduceMessage msg) throws ScriptException, ResourceException {
      this.id = msg.getId();
      this.replyTo = msg.getReplyTo();
      this.cl = (Closure) getMapReduceClosure(msg).getProperty("rereduce");
    }

    public void onMessage(Message message) {
      log.debug("rereduce: " + message);
      MessageProperties props = message.getMessageProperties();
      if ("end".equals(props.getType())) {
        final Object o = cl.call(new Object[]{id, results.toArray()});
        listenerCache.remove(DigestUtils.md5Hex(id)).stop();
        rabbitTemplate.send("", replyTo, new MessageCreator() {
          public Message createMessage() {
            MessageProperties outProps = new RabbitMessageProperties();
            outProps.setContentType("applicaiton/json");
            outProps.setCorrelationId(id.getBytes());
            byte[] body = serialize(o);
            log.debug("rereduce result: " + new String(body));
            Message msg = new Message(body, outProps);
            return msg;
          }
        });
      } else {
        try {
          Object o = deserialize(message.getBody());
          results.add(o);
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
      }
    }
  }

  class EmitClosure extends Closure {

    String src;

    EmitClosure(Object owner, String src) {
      super(owner);
      this.src = src;
    }

    @Override
    public Object call(Object[] args) {
      //log.debug("emitting: " + args);
      final byte[] id = (args[0] instanceof String ? ((String) args[0])
          .getBytes() : (byte[]) args[0]);
      final String key = args[1].toString();
      final Object values = args[2];
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        mapper.writeValue(out, values);
        rabbitTemplate.send(mapreduceExchange, reduceRoutingKey, new MessageCreator() {
          public Message createMessage() {
            MessageProperties props = new RabbitMessageProperties();
            props.setContentType("application/json");
            props.getHeaders()
                .put(SecureMessageConverter.SECURITY_KEY_HDR, mapreduceSecurityKey);
            props.getHeaders().put("mapreduce.key", key);
            props.getHeaders().put("mapreduce.src", src);
            props.setCorrelationId(id);
            Message msg = new Message(out.toByteArray(), props);
            return msg;
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }
  }

}
