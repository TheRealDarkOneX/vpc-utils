package com.jbrisbin.vpc.jobsched.mapred;

import groovy.lang.GroovyObjectSupport;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class MapReduceMessage extends GroovyObjectSupport {

  private String id;
  private String replyTo;
  private String key;
  private String type;
  private String src;
  private Object data;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getReplyTo() {
    return replyTo;
  }

  public void setReplyTo(String replyTo) {
    this.replyTo = replyTo;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getSrc() {
    return src;
  }

  public void setSrc(String src) {
    this.src = src;
  }

  public Object getData() {
    return data;
  }

  public void setData(Object data) {
    this.data = data;
  }

  @Override
  public String toString() {
    return "{key=" + (null != key ? new String(key) : "<NULL>")
        + ", type=" + (null != type ? type : "<NULL>")
        + ", src=" + (null != src ? src : "<NULL>")
        + ", data=" + (null != data ? data : "<NULL>")
        + "}";
  }
}
