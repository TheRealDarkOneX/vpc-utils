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
