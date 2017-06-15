package com.baifendian.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.element.*;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by caojingwei on 2017/6/15.
 */
public enum SupportHiveDataType implements DataTypeHelp {
  //支持所有基本类型
  TINYINT {
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new LongColumn(res.getInt(index));
    }
  },
  SMALLINT {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new LongColumn(res.getInt(index));
    }
  },
  INT {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new LongColumn(res.getInt(index));
    }
  },
  BIGINT {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new LongColumn(res.getLong(index));
    }
  },
  BOOLEAN {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new BoolColumn(res.getBoolean(index));
    }
  },
  FLOAT {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new DoubleColumn(res.getFloat(index));
    }
  },
  DOUBLE {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new DoubleColumn(res.getDouble(index));
    }
  },
  DECIMAL {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new DoubleColumn(res.getBigDecimal(index));
    }
  },
  STRING {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new StringColumn(res.getString(index));
    }
  },
  VARCHAR {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new StringColumn(res.getString(index));
    }
  },
  CHAR {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new StringColumn(res.getString(index));
    }
  },
  TIMESTAMP {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new DateColumn(res.getTimestamp(index));
    }
  },
  DATE {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new DateColumn(res.getDate(index));
    }
  },
  BINARY {
    @Override
    public Column parseDataXType(ResultSet res, int index) throws SQLException {
      return new BytesColumn(res.getBytes(index));
    }
  };

  /**
   * 根据hive类型获取
   *
   * @param type
   * @return
   */
  public static SupportHiveDataType getType(String type) {
    for (SupportHiveDataType supportHiveDataType : values()) {
      if (StringUtils.equalsIgnoreCase(supportHiveDataType.name(), type)) {
        return supportHiveDataType;
      }
    }
    return null;
  }
}
