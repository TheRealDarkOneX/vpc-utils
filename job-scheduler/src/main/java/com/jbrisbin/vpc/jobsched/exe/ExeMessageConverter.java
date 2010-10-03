package com.jbrisbin.vpc.jobsched.exe;

import com.jbrisbin.vpc.jobsched.SecureMessageConverter;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;

import java.io.IOException;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class ExeMessageConverter extends SecureMessageConverter {

  public Message toMessage(Object object,
                           MessageProperties props) throws MessageConversionException {
    if (object instanceof ExeMessage) {
      ExeMessage msg = (ExeMessage) object;
      return new Message(msg.getOutputBytes(), props);
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
      ExeMessage msg;
      try {
        msg = mapObject(bytes, ExeMessage.class);
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
