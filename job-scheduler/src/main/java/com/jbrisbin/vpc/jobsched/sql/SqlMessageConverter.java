package com.jbrisbin.vpc.jobsched.sql;

import com.jbrisbin.vpc.jobsched.SecureMessageConverter;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;

import java.io.IOException;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class SqlMessageConverter extends SecureMessageConverter {

  public Message toMessage(Object object, MessageProperties props) throws
      MessageConversionException {
    if (object instanceof SqlMessage) {
      SqlMessage msg = (SqlMessage) object;
      props.setCorrelationId(msg.getId().getBytes());
      byte[] bytes;
      try {
        bytes = unmapObject(msg.getResults());
      } catch (IOException e) {
        throw new MessageConversionException(e.getMessage(), e);
      }
      props.setContentType("application/json");

      return new Message(bytes, props);
    } else {
      throw new MessageConversionException(
          "Cannot convert object " + String.valueOf(object) + " using " + getClass()
              .toString());
    }
  }

  public Object fromMessage(Message message) throws MessageConversionException {
    MessageProperties props = message.getMessageProperties();
    if (isAuthorized(props)) {
      //Map<String, Object> headers = props.getHeaders();
      byte[] bytes = message.getBody();
      SqlMessage msg;
      try {
        msg = mapObject(bytes, SqlMessage.class);
        msg.setId(new String(props.getCorrelationId()));
        msg.setReplyTo(props.getReplyTo().toString());
        if (log.isDebugEnabled()) {
          log.debug(String.format(" MSG: %s", msg));
        }
      } catch (IOException e) {
        throw new MessageConversionException(e.getMessage(), e);
      }

      return msg;
    } else {
      throw new MessageConversionException("Invalid security key.");
    }
  }
}
