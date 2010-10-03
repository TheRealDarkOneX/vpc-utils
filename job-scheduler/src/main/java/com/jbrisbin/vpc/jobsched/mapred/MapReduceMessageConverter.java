package com.jbrisbin.vpc.jobsched.mapred;

import com.jbrisbin.vpc.jobsched.SecureMessageConverter;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class MapReduceMessageConverter extends SecureMessageConverter {

  public Message toMessage(Object object,
                           MessageProperties messageProperties) throws MessageConversionException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public Object fromMessage(Message message) throws MessageConversionException {
    MessageProperties props = message.getMessageProperties();
    if (isAuthorized(props)) {
      //Map<String, Object> headers = props.getHeaders();
      byte[] bytes = message.getBody();
      MapReduceMessage msg = new MapReduceMessage();
      msg.setId(new String(props.getCorrelationId()));
      try {
        msg.setReplyTo(props.getReplyTo().toString());
      } catch (NullPointerException ignored) {
      }
      msg.setKey(props.getHeaders().get("mapreduce.key").toString());
      msg.setSrc(props.getHeaders().get("mapreduce.src").toString());
      msg.setType(props.getReceivedRoutingKey());
      try {
        try {
          msg.setData(mapObject(bytes, List.class));
        } catch (JsonMappingException e1) {
          try {
            msg.setData(mapObject(bytes, Map.class));
          } catch (JsonMappingException e2) {
            try {
              msg.setData(mapObject(bytes, Integer.class));
            } catch (JsonMappingException e3) {
              try {
                msg.setData(mapObject(bytes, String.class));
              } catch (JsonMappingException e4) {
              }
            }
          }
        }
      } catch (IOException ioe) {
        throw new MessageConversionException(ioe.getMessage(), ioe);
      }
      return msg;
    } else {
      throw new MessageConversionException("Invalid security key.");
    }
  }

}
