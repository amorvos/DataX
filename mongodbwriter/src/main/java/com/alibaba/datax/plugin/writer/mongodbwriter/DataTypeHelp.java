package com.alibaba.datax.plugin.writer.mongodbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import org.bson.Document;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by caojingwei on 2017/6/20.
 */
interface DataTypeHelp {
  void parseDataXType(String fileName, Column column, Document document) throws Exception;
}
