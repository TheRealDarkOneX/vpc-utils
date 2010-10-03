package com.jbrisbin.vpc.jobsched.mapred;

import groovy.lang.Closure;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class MapReduceControlClosure extends Closure {

  private ConcurrentSkipListMap<String, Closure> contextCache;

  public MapReduceControlClosure(Object owner,
                                 ConcurrentSkipListMap<String, Closure> contextCache) {
    super(owner);
    this.contextCache = contextCache;
  }

  @Override
  public Object call(Object[] args) {
    String key = (String) args[0];
    Closure cl = contextCache.remove(key);
    cl.invokeMethod("onKeyChange", new Object[]{key});
    return null;
  }
}
