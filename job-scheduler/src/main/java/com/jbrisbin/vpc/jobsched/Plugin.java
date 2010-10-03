package com.jbrisbin.vpc.jobsched;

import java.util.Map;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public interface Plugin {

  public void setContext(Map<String, Object> vars);

  public Object run() throws Exception;

  public Object get(String key);

}
