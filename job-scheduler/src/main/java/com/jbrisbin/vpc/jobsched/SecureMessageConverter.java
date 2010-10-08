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

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public abstract class SecureMessageConverter implements MessageConverter {

  public static final String SECURITY_KEY_HDR = "security.key";

  protected Logger log = LoggerFactory.getLogger(getClass());
  protected String securityKey;
  protected ObjectMapper mapper = new ObjectMapper();

  public void setSecurityKey(String securityKey) {
    this.securityKey = securityKey;
  }

  public String getSecurityKey() {
    return this.securityKey;
  }

  public boolean isAuthorized(MessageProperties props) {
    Object incomingKey = props.getHeaders().get(SECURITY_KEY_HDR);
    return (null == incomingKey ? false : incomingKey.toString().equals(securityKey));
  }

  protected <T> T mapObject(byte[] json, Class<T> type) throws IOException {
    return mapper.readValue(json, 0, json.length, type);
  }

  protected byte[] unmapObject(Object obj) throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    mapper.writeValue(bytesOut, obj);
    return bytesOut.toByteArray();
  }
}
