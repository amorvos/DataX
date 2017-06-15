package com.baifendian.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by caojingwei on 2017/6/15.
 */
public enum HiveReaderErrorCode implements ErrorCode {
  BAD_CONFIG_VALUE("HiveReader-00", "您配置的值不合法."),
  HIVE_DRIVER_ERROR("HiveReader-01", "hive驱动加载失败."),
  HIVE_CONN_ERROR("HiveReader-02", "hive连接失败."),
  HIVE_QUERY_ERROR("HiveReader-03","hive-sql查询失败."),
  COLUMN_NOT_FOUNT("HiveReader-04","读取column不存在."),
  HIVE_CLOSE_ERROR("HiveReader-05","hive链接关闭失败.");


  private final String code;
  private final String description;

  private HiveReaderErrorCode(String code, String description) {
    this.code = code;
    this.description = description;
  }

  @Override
  public String getCode() {
    return this.code;
  }

  @Override
  public String getDescription() {
    return this.description;
  }

  @Override
  public String toString() {
    return String.format("Code:[%s], Description:[%s]. ", this.code,
            this.description);
  }
}
