package com.jbrisbin.vpc.jobsched.batch;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class BatchMessage {

  private String id;
  private int timeout = 2;
  private Map<String, String> messages = new LinkedHashMap<String, String>();

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public Map<String, String> getMessages() {
    return messages;
  }

  public void setMessages(Map<String, String> messages) {
    this.messages = messages;
  }
}
