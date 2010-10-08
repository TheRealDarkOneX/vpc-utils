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

import groovy.util.ResourceConnector;
import groovy.util.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLConnection;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class SpringResourceConnector implements ResourceConnector {

  private Logger log = LoggerFactory.getLogger(getClass());

  public URLConnection getResourceConnection(String name) throws ResourceException {
    Resource res = null;
    if (name.startsWith("classpath:")) {
      res = new ClassPathResource(name.substring(10));
    } else if (name.startsWith("http")) {
      try {
        res = new UrlResource(name);
      } catch (MalformedURLException e) {
        log.error(e.getMessage(), e);
      }
    } else {
      res = new FileSystemResource(name);
    }
    try {
      return res.getURI().toURL().openConnection();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
      throw new ResourceException(e);
    }
  }
}
