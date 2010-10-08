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
