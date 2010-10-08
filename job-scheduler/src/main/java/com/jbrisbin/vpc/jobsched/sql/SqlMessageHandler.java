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

import com.jbrisbin.vpc.jobsched.ClosureFactory;
import com.jbrisbin.vpc.jobsched.Plugin;
import com.jbrisbin.vpc.jobsched.PluginManager;
import com.jbrisbin.vpc.jobsched.util.BeanClosure;
import groovy.lang.Closure;
import groovy.lang.GString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.SQLExceptionTranslator;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class SqlMessageHandler implements ApplicationContextAware {

  private Logger log = LoggerFactory.getLogger(getClass());
  private ApplicationContext appCtx;
  private String storeDbName;
  private String storeDbLogin;
  private String storeDbPassword;
  @Autowired
  private ConnectionFactory connectionFactory;
  @Autowired
  private PluginManager groovyPluginManager;
  @Autowired
  private RabbitAdmin rabbitAdmin;
  @Autowired
  private RabbitTemplate rabbitTemplate;
  @Autowired
  private BeanClosure beanClosure;
  @Autowired
  private ClosureFactory groovyClosureFactory;

  public String getStoreDbName() {
    return storeDbName;
  }

  public void setStoreDbName(String storeDbName) {
    this.storeDbName = storeDbName;
  }

  public String getStoreDbLogin() {
    return storeDbLogin;
  }

  public void setStoreDbLogin(String storeDbLogin) {
    this.storeDbLogin = storeDbLogin;
  }

  public String getStoreDbPassword() {
    return storeDbPassword;
  }

  public void setStoreDbPassword(String storeDbPassword) {
    this.storeDbPassword = storeDbPassword;
  }

  public void setApplicationContext(
      ApplicationContext applicationContext) throws BeansException {
    this.appCtx = applicationContext;
  }

  public SqlMessage handleMessage(final SqlMessage msg) throws Exception {
    log.debug("handling message: " + msg.toString());

    DataSource ds = appCtx.getBean(msg.getDatasource(), DataSource.class);
    JdbcTemplate tmpl = new JdbcTemplate(ds);

    String sql = msg.getSql();
    CallableStatementCreator stmtCreator = null;
    CallableStatementCallback<SqlMessage> callback = null;
    if (sql.startsWith("plugin:")) {
      // Use a plugin to get the sql
      String pluginName = (sql.contains("?") ? sql.substring(7, sql.indexOf('?')) : sql
          .substring(7));
      final Plugin plugin = groovyPluginManager.getPlugin(pluginName);
      Map<String, Object> vars = new LinkedHashMap<String, Object>();
      vars.put("message", msg);
      vars.put("datasource", ds);
      vars.put("listen", groovyClosureFactory.createListenClosure(msg));
      vars.put("mapreduce", groovyClosureFactory.createMapReduceClosure(msg));
      plugin.setContext(vars);

      // Execute this plugin
      plugin.run();

      Object o = plugin.get("sql");
      if (null != o && o instanceof Closure) {
        sql = ((Closure) o).call(msg).toString();
      } else if (o instanceof String || o instanceof GString) {
        sql = o.toString();
      } else {
        throw new IllegalStateException(
            "Can't convert " + String.valueOf(o) + " to SQL statement.");
      }
      msg.setSql(sql);

      o = plugin.get("statementCreator");
      if (null != o && o instanceof Closure) {
        stmtCreator = new CallableStatementCreator() {
          public CallableStatement createCallableStatement(Connection con) throws SQLException {
            Object obj = ((Closure) plugin.get("statementCreator"))
                .call(new Object[]{con, msg});
            log.debug("from plugin statementCreator: " + String.valueOf(obj));
            return (CallableStatement) obj;
          }
        };
      } else {
        throw new IllegalStateException("Can't convert " + String.valueOf(
            o) + " to CallableStatementCreator. Define a closure named 'statementCreator' in your plugin.");
      }

      o = plugin.get("callback");
      if (null != o && o instanceof Closure) {
        callback = new CallableStatementCallback<SqlMessage>() {
          public SqlMessage doInCallableStatement(
              CallableStatement cs) throws SQLException, DataAccessException {
            Object obj = ((Closure) plugin.get("callback")).call(new Object[]{cs, msg});
            log.debug("from plugin callback: " + String.valueOf(obj));
            return (SqlMessage) obj;
          }
        };
      } else {
        throw new IllegalStateException("Can't convert " + String.valueOf(
            o) + " to CallableStatementCallback. Define a closure named 'callback' in your plugin.");
      }
    } else {
      stmtCreator = new CallableStatementCreator() {
        public CallableStatement createCallableStatement(
            Connection connection) throws SQLException {
          CallableStatement stmt = connection.prepareCall(msg.getSql());
          List<Object> params = msg.getParams();
          if (null != params) {
            int index = 1;
            for (Object obj : params) {
              stmt.setObject(index++, obj);
            }
          }
          return stmt;
        }
      };
      callback = new CallableStatementCallback<SqlMessage>() {
        public SqlMessage doInCallableStatement(CallableStatement callableStatement) throws
            SQLException,
            DataAccessException {
          if (null == msg.getResults().getData()) {
            msg.getResults().setData(new ArrayList<List<Object>>());
          }
          if (callableStatement.execute()) {
            ResultSet results = callableStatement.getResultSet();

            // Pull out column names
            ResultSetMetaData meta = results.getMetaData();
            String[] columns = new String[meta.getColumnCount()];
            for (int i = 1; i <= meta.getColumnCount(); i++) {
              columns[i - 1] = meta.getColumnName(i);
            }
            msg.getResults().getColumnNames().addAll(Arrays.asList(columns));

            int total = 0;
            while (results.next()) {
              List<Object> row = new ArrayList<Object>(columns.length);
              for (int i = 1; i <= columns.length; i++) {
                row.add(results.getObject(i));
              }
              msg.getResults().getData().add(row);
              total++;
            }
            msg.getResults().setTotalRows(total);

          } else {
            msg.getResults().getColumnNames().add("updateCount");
            msg.getResults().setTotalRows(1);
            List<Object> updCnt = new ArrayList<Object>(1);
            updCnt.add(callableStatement.getUpdateCount());
            msg.getResults().getData().add(updCnt);
          }
          return msg;
        }
      };
    }
    try {
      tmpl.setExceptionTranslator(appCtx.getBean(SQLExceptionTranslator.class));
    } catch (NoSuchBeanDefinitionException notfound) {
      // IGNORED
    }

    if (null != stmtCreator && null != callback) {
      try {
        tmpl.execute(stmtCreator, callback);
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
        List<String> errors = new ArrayList<String>();
        errors.add(t.getMessage());
        Throwable cause = t.getCause();
        if (null != cause) {
          do {
            errors.add(cause.getMessage());
          } while (null != (cause = cause.getCause()));
        }
        msg.getResults().setErrors(errors);
      }
    } else {
      log.warn(
          "CallableStatementCreator and/or CallableStatementCallback where empty. " +
              "Make sure your plugin provides these under 'statementCreator' and 'callback' respectively.");
    }
    return msg;
  }

  class SqlHelper {
    String sql;
    List<Object> params;
  }
}
