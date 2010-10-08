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
