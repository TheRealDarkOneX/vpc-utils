package com.jbrisbin.vpc.jobsched;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageCreator;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RequeueListener implements ChannelAwareMessageListener<Message>, ApplicationContextAware {

  public static final String REQUEUED = "requeued";

  private final Logger log = LoggerFactory.getLogger(getClass());

  private ApplicationContext appCtx;
  private Timer timer = new Timer(true);
  private int maxRetries = 3;
  private Integer[] retryDelays = new Integer[]{60000, 300000, 1800000};

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

  public Integer[] getRetryDelays() {
    return retryDelays;
  }

  public void setRetryDelays(Integer[] retryDelays) {
    this.retryDelays = retryDelays;
  }

  @Override
  public void onMessage(Message message, Channel channel) throws Exception {
    MessageProperties props = message.getMessageProperties();
    Map<String, Object> headers = props.getHeaders();
    int requeued = 0;
    if (headers.containsKey(REQUEUED)) {
      requeued = (Integer) headers.get(REQUEUED);
    }

    long delay = retryDelays[requeued];
    if (requeued < maxRetries) {
      headers.put(REQUEUED, requeued + 1);
      Object exchange = headers.get("exchange");
      if (null == exchange) {
        exchange = props.getReceivedExchange();
      } else {
        headers.remove("exchange");
      }
      Object route = headers.get("route");
      if (null == route) {
        route = props.getReceivedRoutingKey();
      } else {
        headers.remove("route");
      }
      log.info(String.format("Requeing message %s in %s...",
          new String(props.getCorrelationId()),
          convertMillis(delay)));
      timer.schedule(new DelayedSend(message, exchange.toString(), route.toString()), delay);
    }
  }

  private String convertMillis(long ms) {
    int i = Math.round(ms / 1000);
    if (i > 60) {
      if (i >= 3600) {
        return String.format("%s hours", Math.round(i / 3600));
      } else {
        return String.format("%s mins", Math.round(i / 60));
      }
    } else {
      return String.format("%s secs", i);
    }
  }

  class DelayedSend extends TimerTask {

    private Message message;
    private String exchange;
    private String route;

    DelayedSend(Message message, String exchange, String route) {
      this.message = message;
      this.exchange = exchange;
      this.route = route;
    }

    @Override
    public void run() {
      MessageProperties props = message.getMessageProperties();
      appCtx.getBean(RabbitTemplate.class).send(exchange, route, new MessageCreator() {
        @Override
        public Message createMessage() {
          return message;
        }
      });
    }
  }

}
