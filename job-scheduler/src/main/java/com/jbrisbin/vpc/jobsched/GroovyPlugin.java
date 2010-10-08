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

import groovy.lang.Binding;
import groovy.lang.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class GroovyPlugin implements Plugin {

  protected Logger log = LoggerFactory.getLogger(getClass());
  protected Binding binding;
  protected Script script;

  public GroovyPlugin(Script script) {
    this.script = script;
    this.binding = script.getBinding();
  }

  public Binding getBinding() {
    return binding;
  }

  public Script getScript() {
    return script;
  }

  public void setScript(Script script) {
    this.script = script;
  }

  public void setContext(Map<String, Object> vars) {
    for (Map.Entry<String, Object> entry : vars.entrySet()) {
      binding.setVariable(entry.getKey(), entry.getValue());
    }
  }

  public Object run() throws Exception {
    return script.run();
  }

  public Object get(String key) {
    Object o = null;
    if (binding.getVariables().containsKey(key)) {
      o = binding.getVariable(key);
    } else {
      o = script.getProperty(key);
    }
    return o;
  }

}
