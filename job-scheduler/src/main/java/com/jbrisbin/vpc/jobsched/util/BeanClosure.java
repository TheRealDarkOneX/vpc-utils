package com.jbrisbin.vpc.jobsched.util;

import groovy.lang.Closure;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class BeanClosure extends Closure implements ApplicationContextAware {

  private ApplicationContext appCtx;

  public void setApplicationContext(
      ApplicationContext applicationContext) throws BeansException {
    this.appCtx = applicationContext;
  }

  public BeanClosure(Object owner) {
    super(owner);
  }

  @Override
  public Object call(Object arg) {
    if (arg instanceof Class) {
      return appCtx.getBean((Class) arg);
    } else {
      return appCtx.getBean(arg.toString());
    }
  }
}
