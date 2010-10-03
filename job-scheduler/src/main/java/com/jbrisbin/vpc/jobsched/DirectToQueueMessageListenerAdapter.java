package com.jbrisbin.vpc.jobsched;

import org.springframework.amqp.core.Message;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class DirectToQueueMessageListenerAdapter extends org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter {
  @Override
  protected String getReceivedExchange(Message request) {
    return "";
  }
}
