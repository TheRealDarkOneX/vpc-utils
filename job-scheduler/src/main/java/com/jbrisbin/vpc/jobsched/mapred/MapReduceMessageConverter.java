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
