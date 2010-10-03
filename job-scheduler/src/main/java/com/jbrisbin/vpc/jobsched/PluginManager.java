package com.jbrisbin.vpc.jobsched;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public interface PluginManager {

  public void init();

  public Plugin getPlugin(String key);

  public void close();
}
