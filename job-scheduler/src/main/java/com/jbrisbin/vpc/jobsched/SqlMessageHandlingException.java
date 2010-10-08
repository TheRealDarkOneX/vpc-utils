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

import com.jbrisbin.vpc.jobsched.sql.SqlMessage;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class SqlMessageHandlingException extends Exception {

  private SqlMessage sqlMessage;

  public SqlMessageHandlingException(Throwable cause, SqlMessage sqlMessage) {
    super(cause);
    this.sqlMessage = sqlMessage;
  }

  public SqlMessage getSqlMessage() {
    return sqlMessage;
  }

  public void setSqlMessage(SqlMessage message) {
    this.sqlMessage = message;
  }
}
