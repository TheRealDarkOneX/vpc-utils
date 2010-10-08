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

package com.jbrisbin.vpc.jobsched.patch;

import com.jbrisbin.vpc.jobsched.SecureMessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jon Brisbin <jon.brisbin@npcinternational.com>
 */
public class PatchMessageConverter extends SecureMessageConverter {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private String rootDir = "/tmp";

  public String getRootDir() {
    return rootDir;
  }

  public void setRootDir(String rootDir) {
    this.rootDir = rootDir;
  }

  public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public Object fromMessage(Message message) throws MessageConversionException {

    PatchMessage msg = new PatchMessage();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(message.getBody())));

    List<String> diff = new ArrayList<String>();
    String line = null;
    try {
      while (null != (line = reader.readLine())) {
        diff.add(line);
      }
      msg.setDiff(diff);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }

    Object fileName = message.getMessageProperties().getHeaders().get("file");
    if (null != fileName) {
      if (log.isDebugEnabled()) {
        log.debug(String.format("Patching file %s with %s", fileName.toString(), new String(message.getBody())));
      }
      String s = fileName.toString();
      if (s.startsWith("/")) {
        s = s.substring(1);
      }
      msg.setFile(String.format("%s/%s", rootDir, fileName.toString()));
    }
    return msg;
  }
}
