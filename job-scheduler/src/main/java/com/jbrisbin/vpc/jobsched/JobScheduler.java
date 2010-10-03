package com.jbrisbin.vpc.jobsched;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class JobScheduler {

  public static void main(String[] args) {
    ApplicationContext appCtx = new ClassPathXmlApplicationContext("/jobsched.xml");
    while (true) {
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }
}
