package com.baifendian.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.element.Column;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by caojingwei on 2017/6/15.
 */
public interface DataTypeHelp {
  public Column parseDataXType(ResultSet res, int index) throws SQLException;
}
