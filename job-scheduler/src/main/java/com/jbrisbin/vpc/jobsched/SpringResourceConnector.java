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
