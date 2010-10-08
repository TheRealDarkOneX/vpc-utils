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
