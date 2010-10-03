package com.jbrisbin.vpc.jobsched;

import com.jbrisbin.vpc.jobsched.util.BeanClosure;
import groovy.lang.Binding;
import groovy.lang.Script;
import groovy.util.GroovyScriptEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class GroovyPluginManager implements PluginManager, ApplicationContextAware {

  protected final Logger log = LoggerFactory.getLogger(getClass());

  protected ApplicationContext appCtx;
  @Autowired
  protected GroovyScriptEngine groovyScriptEngine;
  @Autowired
  protected BeanClosure beanClosure;
  @Autowired
  protected RabbitTemplate rabbitTemplate;

  public void setApplicationContext(
      ApplicationContext applicationContext) throws BeansException {
    this.appCtx = applicationContext;
  }

  public GroovyScriptEngine getGroovyScriptEngine() {
    return groovyScriptEngine;
  }

  public void setGroovyScriptEngine(GroovyScriptEngine groovyScriptEngine) {
    this.groovyScriptEngine = groovyScriptEngine;
  }

  public BeanClosure getBeanClosure() {
    return beanClosure;
  }

  public void setBeanClosure(BeanClosure beanClosure) {
    this.beanClosure = beanClosure;
  }

  public void init() {

  }

  public Plugin getPlugin(String key) {
    Script script = null;
    try {
      Binding b = new Binding();
      b.setVariable("bean", beanClosure);
      script = groovyScriptEngine.createScript(key, b);
      script.setProperty("log", LoggerFactory.getLogger("plugins"));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return null;
    }
    return (null != script ? new GroovyPlugin(script) : null);
  }

  public void close() {
  }

}
