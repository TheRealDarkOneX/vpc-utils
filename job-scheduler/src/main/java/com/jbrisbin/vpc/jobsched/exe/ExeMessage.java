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

package com.jbrisbin.vpc.jobsched.exe;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@JsonIgnoreProperties({"out"})
public class ExeMessage {

  private String dir = ".";
  private String exe;
  private Map<String, String> env = new LinkedHashMap<String, String>();
  private List<String> args = new ArrayList<String>();
  private ByteArrayOutputStream out = new ByteArrayOutputStream();

  public String getDir() {
    return dir;
  }

  public void setDir(String dir) {
    this.dir = dir;
  }

  public String getExe() {
    return exe;
  }

  public void setExe(String exe) {
    this.exe = exe;
  }

  public Map<String, String> getEnv() {
    return env;
  }

  public void setEnv(Map<String, String> env) {
    this.env = env;
  }

  public List<String> getArgs() {
    return args;
  }

  public void setArgs(List<String> args) {
    this.args = args;
  }

  public ByteArrayOutputStream getOut() {
    return out;
  }

  public void setOut(ByteArrayOutputStream out) {
    this.out = out;
  }

  public byte[] getOutputBytes() {
    return out.toByteArray();
  }
}
