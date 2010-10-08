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

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.StoreResponse;
import com.jbrisbin.vpc.jobsched.sql.SqlMessage;
import com.rabbitmq.client.Channel;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RequeueMessageListenerAdapter extends org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private int maxRetries = 3;
  private String requeueExchangeName = "jobsched";
  private String requeueQueueName = "jobsched.requeue";
  private String notifyQueueName = "jobsched.notify";
  private String riakBase;
  private String riakBucket = "jobsched.requeue";
  private RiakClient riak;
  private ObjectMapper mapper = new ObjectMapper();

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public String getRequeueExchangeName() {
    return requeueExchangeName;
  }

  public void setRequeueExchangeName(String requeueExchangeName) {
    this.requeueExchangeName = requeueExchangeName;
  }

  public String getRequeueQueueName() {
    return requeueQueueName;
  }

  public void setRequeueQueueName(String requeueQueueName) {
    this.requeueQueueName = requeueQueueName;
  }

  public String getNotifyQueueName() {
    return notifyQueueName;
  }

  public void setNotifyQueueName(String notifyQueueName) {
    this.notifyQueueName = notifyQueueName;
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

  @Override
  protected void postProcessResponse(Message request, Message response) throws Exception {
    super.postProcessResponse(request, response);
    log.info(new String(request.getBody()));
  }

  @Override
  protected String getReceivedExchange(Message request) {
    return "";
  }

  @Override
  protected void handleResult(Object result, Message request,
                              Channel channel) throws Exception {
    if (channel != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Listener method returned result [" + result +
            "] - generating response message for it");
      }
      Message response = buildMessage(channel, result);
      postProcessResponse(request, response);
      String replyTo = getResponseReplyTo(request, response, channel);
      String exchange = getReceivedExchange(request);

      boolean sendResponse = true;
      if (result instanceof SqlMessage) {
        SqlMessage msg = (SqlMessage) result;
        if (null != msg.getResults().getErrors()) {
          // Has errors, maybe retry
          List<String> errors = msg.getResults().getErrors();
          if ("Connection timed out".equals(errors.get(errors.size() - 1))) {
            String messageId = new String(response.getMessageProperties().getCorrelationId());
            Map<String, Object> obj = new LinkedHashMap<String, Object>();

            obj.put("exchange", request.getMessageProperties().getReceivedExchange());
            obj.put("route", request.getMessageProperties().getReceivedRoutingKey());

            Map<String, String> headers = new LinkedHashMap<String, String>();
            for (Map.Entry<String, Object> itm : request.getMessageProperties().getHeaders()
                .entrySet()) {
              headers.put(new String(itm.getKey()), itm.getValue().toString());
            }
            obj.put("headers", headers);

            obj.put("body", new String(request.getBody()));

            // Store message and update schema
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            mapper.writeValue(bytes, obj);
            RiakObject riakObj = new RiakObject(riak, riakBucket, messageId,
                bytes.toByteArray(), "application/json");
            StoreResponse storeResp = riakObj.store();
            if (storeResp.isSuccess()) {
              // Don't send the response, requeue it
              sendResponse = false;
            }
          }
        }
      }

      if (sendResponse) {
        sendResponse(channel, exchange, replyTo, response);
      }

    } else if (logger.isWarnEnabled()) {
      logger.warn("Listener method returned result [" + result +
          "]: not generating response message for it because of no Rabbit Channel given");
    }
  }
}
