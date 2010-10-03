package com.jbrisbin.vpc.jobsched;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageCreator;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.IOException;
import java.util.Map;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class RequeueJob implements ApplicationContextAware {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private ApplicationContext appCtx;
  private int maxRetries = 0;
  private String riakBase;
  private String riakBucket = "jobsched.requeue";
  private RiakClient riak;
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public void setApplicationContext(
      ApplicationContext applicationContext) throws BeansException {
    this.appCtx = applicationContext;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public String getRiakBase() {
    return riakBase;
  }

  public void setRiakBase(String riakBase) {
    this.riakBase = riakBase;
    this.riak = new RiakClient(riakBase);
  }

  public String getRiakBucket() {
    return riakBucket;
  }

  public void setRiakBucket(String riakBucket) {
    this.riakBucket = riakBucket;
  }

  public void maybeRequeue() {
    RequestMeta meta = new RequestMeta();
    meta.setQueryParam("keys", "true");
    BucketResponse bucket = riak.listBucket(riakBucket, meta);
    if (bucket.isSuccess()) {
      for (String messageId : bucket.getBucketInfo().getKeys()) {
        FetchResponse response = riak.fetch(riakBucket, messageId);
        if (response.isSuccess()) {
          // Try re-queueing message
          try {
            if (log.isDebugEnabled()) {
              log.debug("Requeueing messageId: " + messageId);
            }
            final Map<String, Object> message = mapper
                .readValue(response.getBodyAsString(), Map.class);
            RabbitTemplate tmpl = appCtx.getBean(RabbitTemplate.class);
            final MessageProperties props = new RabbitMessageProperties();
            Map headers = (Map) message.get("headers");
            for (Object key : headers.keySet()) {
              props.setHeader(key.toString(), headers.get(key));
            }
            props.setCorrelationId(messageId.getBytes());
            props.setContentType("application/json");
            tmpl.send(message.get("exchange").toString(), message.get("route").toString(),
                new MessageCreator() {
                  @Override
                  public Message createMessage() {
                    Message msg = new Message(message.get("body").toString().getBytes(), props);
                    if (log.isDebugEnabled()) {
                      log.debug("Sending AMQP message: " + msg.toString());
                    }
                    return msg;
                  }
                });
            // Delete from nosql store
            if (log.isDebugEnabled()) {
              log.debug("Deleting messageId " + messageId + " from requeue cache");
            }
            riak.delete(riakBucket, messageId);
          } catch (IOException e) {
            log.error(e.getMessage(), e);
          }
        }
      }
    }
  }

}
