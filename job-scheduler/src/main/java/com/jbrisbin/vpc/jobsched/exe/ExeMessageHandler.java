package com.jbrisbin.vpc.jobsched.exe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.BufferedInputStream;
import java.io.File;
import java.util.List;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class ExeMessageHandler implements ApplicationContextAware {

  private Logger log = LoggerFactory.getLogger(getClass());
  private ApplicationContext appCtx;

  public void setApplicationContext(
      ApplicationContext applicationContext) throws BeansException {
    this.appCtx = applicationContext;
  }

  public ExeMessage handleMessage(final ExeMessage msg) throws Exception {
    log.debug("handling message: " + msg.toString());

    List<String> args = msg.getArgs();
    args.add(0, msg.getExe());

    try {
      ProcessBuilder pb = new ProcessBuilder(args);
      pb.environment().putAll(msg.getEnv());
      pb.directory(new File(msg.getDir()));
      pb.redirectErrorStream(true);
      Process p = pb.start();

      BufferedInputStream stdout = new BufferedInputStream(p.getInputStream());
      byte[] buff = new byte[4096];
      for (int bytesRead = 0; bytesRead > -1; bytesRead = stdout.read(buff)) {
        msg.getOut().write(buff, 0, bytesRead);
      }

      p.waitFor();
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
      Object errmsg = t.getMessage();
      if (null != errmsg) {
        msg.getOut().write(((String) errmsg).getBytes());
      }
    }
    return msg;
  }
}
