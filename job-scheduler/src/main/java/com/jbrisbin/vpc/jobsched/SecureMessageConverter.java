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
