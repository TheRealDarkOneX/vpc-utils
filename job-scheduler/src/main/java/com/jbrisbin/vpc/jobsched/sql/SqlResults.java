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
