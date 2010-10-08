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

import difflib.DiffUtils;
import difflib.Patch;
import difflib.PatchFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @author Jon Brisbin <jon.brisbin@npcinternational.com>
 */
@SuppressWarnings({"unchecked"})
public class PatchMessageHandler implements ApplicationContextAware {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd.HHmmss");
  private ApplicationContext appCtx;

  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.appCtx = applicationContext;
  }

  public void handleMessage(final PatchMessage msg) {

    if (null != msg.getFile()) {
      // Read original file
      List<String> original = new ArrayList<String>();
      File originalFile = new File(msg.getFile());
      try {
        BufferedReader reader = new BufferedReader(new FileReader(originalFile));
        String line = null;
        while (null != (line = reader.readLine())) {
          original.add(line);
        }
        reader.close();
      } catch (FileNotFoundException e) {
        log.error(e.getMessage(), e);
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
      if (log.isDebugEnabled()) {
        log.debug("Patching original file: " + original);
      }

      // Patch it
      Patch patch = DiffUtils.parseUnifiedDiff(msg.getDiff());
      try {
        List<String> updated = (List<String>) DiffUtils.patch(original, patch);
        File tmp = File.createTempFile("patcher_", "patched");
        BufferedWriter writer = new BufferedWriter(new FileWriter(tmp));
        for (String line : updated) {
          writer.write(line + "\n");
        }
        writer.flush();
        writer.close();

        // Backup existing file
        File basedir = originalFile.getParentFile();
        if (basedir.canWrite()) {
          File backup = new File(originalFile.getParentFile(),
              originalFile.getName() + ".bkp." + fmt.format(Calendar.getInstance().getTime()));
          originalFile.renameTo(backup);
          // Replace with patched version
          tmp.renameTo(originalFile);
        }

      } catch (PatchFailedException e) {
        log.error(e.getMessage(), e);
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }

  }
}
