package com.jbrisbin.vpc.jobsched.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class SqlResults {

  public int totalRows = 0;
  public List<String> columnNames = new ArrayList<String>();
  public List<List<Object>> data;
  private List<String> errors;

  public int getTotalRows() {
    return (null != data ? data.size() : totalRows);
  }

  public void setTotalRows(int totalRows) {
    this.totalRows = totalRows;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  public List<List<Object>> getData() {
    return data;
  }

  public void setData(List<List<Object>> data) {
    this.data = data;
  }

  public List<String> getErrors() {
    return errors;
  }

  public void setErrors(List<String> errors) {
    this.errors = errors;
  }

}
