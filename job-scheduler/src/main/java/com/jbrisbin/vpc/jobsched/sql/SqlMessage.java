package com.jbrisbin.vpc.jobsched.sql;

import groovy.lang.GroovyObjectSupport;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@JsonIgnoreProperties({"id", "replyTo", "results", "retryOnError"})
public class SqlMessage extends GroovyObjectSupport {

  private String id;
  private String replyTo;
  private String sql;
  private String datasource;
  private int start;
  private int limit;
  private List<Object> params = new ArrayList<Object>();
  private SqlResults results = new SqlResults();
  private boolean retryOnError = true;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getReplyTo() {
    return replyTo;
  }

  public void setReplyTo(String replyTo) {
    this.replyTo = replyTo;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getDatasource() {
    return datasource;
  }

  public void setDatasource(String datasource) {
    this.datasource = datasource;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public List<Object> getParams() {
    return params;
  }

  public void setParams(List<Object> params) {
    this.params = params;
  }

  public SqlResults getResults() {
    return results;
  }

  public void setResults(SqlResults results) {
    this.results = results;
  }

  public boolean isRetryOnError() {
    return retryOnError;
  }

  public void setRetryOnError(boolean retryOnError) {
    this.retryOnError = retryOnError;
  }
}
