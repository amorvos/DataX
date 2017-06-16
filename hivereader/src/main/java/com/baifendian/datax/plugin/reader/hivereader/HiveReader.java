package com.baifendian.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static com.baifendian.datax.plugin.reader.hivereader.HiveReaderConst.*;

/**
 * 使用jdbc读 hive 插件
 */
public class HiveReader extends Reader {
  public static class Job extends Reader.Job {

    @Override
    public void init() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public List<Configuration> split(int adviceNumber) {
      //TODO 目前不支持分片读取，有待以后优化。
      List<Configuration> confList = new ArrayList<Configuration>();
      confList.add(super.getPluginJobConf().clone());
      return confList;
    }
  }

  public static class Task extends Reader.Task {
    private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);
    /**
     * jdbc连接
     */
    private Connection connection;
    private Statement stmt;

    /**
     * 用户名
     */
    private String username;
    /**
     * 密码
     */
    private String password;
    /**
     * 数据库连接
     */
    private String jdbcUrl;
    /**
     * 表名
     */
    private String table;
    /**
     * 过滤条件
     */
    private String where;
    /**
     * 请求SQL
     */
    private String querySql;
    /**
     * 字段
     */
    private List<String> column;
    /**
     * task配置
     */
    private Configuration readerSliceConfig;

    @Override
    public void init() {
      LOG.info("Init reader.task...");
      readerSliceConfig = super.getPluginJobConf();
      username = readerSliceConfig.getString(USERNAME, "");
      password = readerSliceConfig.getString(PASSWORD, "");
      jdbcUrl = readerSliceConfig.getString(JDBC_URL);
      table = readerSliceConfig.getString(TABLE);
      where = readerSliceConfig.getString(WHERE);
      querySql = readerSliceConfig.getString(QUERY_SQL);
      column = readerSliceConfig.getList(COLUMN, String.class);
      //进行简单参数校验
      validate();

      LOG.info("Start load hive driver...");
      //加载hive驱动
      try {
        Class.forName(HIVE_DRIVER);
      } catch (ClassNotFoundException e) {
        String message = MessageFormat.format("hive驱动加载失败！异常为：{0}", e.getMessage());
        throw DataXException.asDataXException(HiveReaderErrorCode.HIVE_DRIVER_ERROR, message);
      }
      LOG.info("Finish load hive driver!");

      LOG.info("Start connection to hive...");
      //连接hive
      try {
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        stmt = connection.createStatement();
      } catch (SQLException e) {
        String message = MessageFormat.format("hive连接失败！异常为：{0}", e.getMessage());
        throw DataXException.asDataXException(HiveReaderErrorCode.HIVE_CONN_ERROR, message);
      }
      LOG.info("Finish connection to hive!");

      //TODO 列级的检测暂时无法实现,需要解析hql。。。
      //validateColumn();
      LOG.info("Finish init reader.task!");
    }

    /**
     * 简单参数校验
     */
    public void validate() {
      LOG.info("Start simple param validate...");
      if (StringUtils.isEmpty(jdbcUrl)) {
        String message = "jdbcUrl 参数不得为空!";
        throw DataXException.asDataXException(HiveReaderErrorCode.BAD_CONFIG_VALUE, message);
      }
      if (StringUtils.isEmpty(table) && StringUtils.isEmpty(querySql)) {
        String message = "table 参数不得为空!";
        throw DataXException.asDataXException(HiveReaderErrorCode.BAD_CONFIG_VALUE, message);
      }
      if (column == null || column.size() == 0) {
        String message = "column 参数不得为空!";
        throw DataXException.asDataXException(HiveReaderErrorCode.BAD_CONFIG_VALUE, message);
      }
      LOG.info("Finish simple param validate!");
    }

    /**
     * 校验column配置是否合法
     */
    public void validateColumn() {
      LOG.info("Start column validate...");
      //TODO 暂时无法检测。。。
      /*if (StringUtils.isEmpty(table)) {
        return;
      }

      String sql = MessageFormat.format("DESC `{0}`", table);
      LOG.info("Query hive-sql: {}", sql);
      List<String> targetColumn = new ArrayList<String>();
      try {
        ResultSet res = stmt.executeQuery(getSql());
        while (res.next()) {
          targetColumn.add(res.getString(0).toUpperCase());
        }
      } catch (SQLException e) {
        String message = MessageFormat.format("hive-sql查询执行失败,sql为：{0} ,异常为：{1}", sql, e.getMessage());
        throw DataXException.asDataXException(HiveReaderErrorCode.HIVE_QUERY_ERROR, message);
      }

      for (String col : column) {
        if (!targetColumn.contains(col.toUpperCase())) {
          String message = MessageFormat.format("Column {} 在目标读取表中找不到", col);
          throw DataXException.asDataXException(HiveReaderErrorCode.HIVE_QUERY_ERROR, message);
        }
      }
      // 理论上hivereader支持所有hive类型，这里不检测hive类型*/
      LOG.info("Finish column validate!");
    }

    @Override
    public void destroy() {
      LOG.info("Start destroy hive connection...");
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        String message = MessageFormat.format("hive 链接关闭失败 ,异常为：{0}", e.getMessage());
        throw DataXException.asDataXException(HiveReaderErrorCode.HIVE_CLOSE_ERROR, message);
      }
      LOG.info("Finish destroy hive connection!");
    }

    /**
     * 获取查询sql
     */
    public String getSql() {
      LOG.info("Start create sql...");
      //如果有querySql 优先使用querySql
      if (StringUtils.isNotEmpty(querySql)) {
        LOG.info("Find querySql priority use querySql!");
        table = MessageFormat.format("({0})", querySql);
      }
      if (StringUtils.isEmpty(where)) {
        where = "true";
      }
      String sql = MessageFormat.format(SQL, StringUtils.join(column, ","), table, where);
      LOG.info("Finish create sql: {}", sql);
      return sql;
    }


    @Override
    public void startRead(RecordSender recordSender) {
      LOG.info("Start reader...");
      String sql = getSql();
      //执行查询
      try {
        LOG.info("Start hive-sql query...");
        ResultSet res = stmt.executeQuery(sql);
        LOG.info("Finish hive-sql query!");
        ResultSetMetaData metaData = res.getMetaData();
        LOG.info("Start handle result...");

        //手机字段类型
        List<String> typeList = new ArrayList<String>();
        for (int i = 1, len = metaData.getColumnCount(); i <= len; ++i) {
          typeList.add(metaData.getColumnTypeName(i));
        }

        while (res.next()) {
          Record record = recordSender.createRecord();
          int index = 1;
          for (String type : typeList) {
            SupportHiveDataType supportHiveDataType = SupportHiveDataType.getType(type);
            if (supportHiveDataType == null) {
              //TODO 高级类型待处理。。。
              continue;
            }
            record.addColumn(supportHiveDataType.parseDataXType(res, index++));
          }

          recordSender.sendToWriter(record);
        }
        LOG.info("Finish handle result!");

      } catch (SQLException e) {
        String message = MessageFormat.format("hive-sql查询执行失败,sql为：{0} ,异常为：{1}", sql, e.getMessage());
        throw DataXException.asDataXException(HiveReaderErrorCode.HIVE_QUERY_ERROR, message);
      }
      LOG.info("Finish reader!");
    }
  }
}
