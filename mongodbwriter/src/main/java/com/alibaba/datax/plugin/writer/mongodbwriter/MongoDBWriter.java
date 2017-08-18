package com.alibaba.datax.plugin.writer.mongodbwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.writer.mongodbwriter.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;

import javax.print.Doc;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoDBWriter extends Writer {

  public static class Job extends Writer.Job {

    private Configuration originalConfig = null;

    @Override
    public List<Configuration> split(int mandatoryNumber) {
      List<Configuration> configList = new ArrayList<Configuration>();
      for (int i = 0; i < mandatoryNumber; i++) {
        configList.add(this.originalConfig.clone());
      }
      return configList;
    }

    @Override
    public void init() {
      this.originalConfig = super.getPluginJobConf();
    }

    @Override
    public void prepare() {
      super.prepare();
    }

    @Override
    public void destroy() {

    }
  }

  public static class Task extends Writer.Task {

    private static final Logger logger = LoggerFactory.getLogger(Task.class);
    private Configuration writerSliceConfig;

    private MongoClient mongoClient;

    private String userName = null;
    private String password = null;

    private String database = null;
    private String collection = null;
    private Integer batchSize = null;
    private JSONArray mongodbColumnMeta = null;
    private String writeMode = null;
    private static int BATCH_SIZE = 1000;
    private String upsertKey = null;

    @Override
    public void prepare() {
      super.prepare();
      //获取presql配置，并执行
      String preSql = writerSliceConfig.getString(Key.PRE_SQL);
      if (Strings.isNullOrEmpty(preSql)) {
        return;
      }
      Configuration conConf = Configuration.from(preSql);
      if (Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)
          || mongoClient == null || mongodbColumnMeta == null || batchSize == null) {
        throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
            MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
      }
      MongoDatabase db = mongoClient.getDatabase(database);
      MongoCollection col = db.getCollection(this.collection);
      String type = conConf.getString("type");
      if (Strings.isNullOrEmpty(type)) {
        return;
      }
      if (type.equals("drop")) {
        col.drop();
      } else if (type.equals("remove")) {
        String json = conConf.getString("json");
        BasicDBObject query;
        if (Strings.isNullOrEmpty(json)) {
          query = new BasicDBObject();
          List<Object> items = conConf.getList("item", Object.class);
          for (Object con : items) {
            Configuration _conf = Configuration.from(con.toString());
            if (Strings.isNullOrEmpty(_conf.getString("condition"))) {
              query.put(_conf.getString("name"), _conf.get("value"));
            } else {
              query.put(_conf.getString("name"),
                  new BasicDBObject(_conf.getString("condition"), _conf.get("value")));
            }
          }
//              and  { "pv" : { "$gt" : 200 , "$lt" : 3000} , "pid" : { "$ne" : "xxx"}}
//              or  { "$or" : [ { "age" : { "$gt" : 27}} , { "age" : { "$lt" : 15}}]}
        } else {
          query = (BasicDBObject) com.mongodb.util.JSON.parse(json);
        }
        col.deleteMany(query);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("After job prepare(), originalConfig now is:[\n{}\n]",
            writerSliceConfig.toJSON());
      }
    }

    @Override
    public void startWrite(RecordReceiver lineReceiver) {
      if (Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)
          || mongoClient == null || mongodbColumnMeta == null || batchSize == null) {
        throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
            MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
      }
      MongoDatabase db = mongoClient.getDatabase(database);
      MongoCollection<Document> col = db.getCollection(this.collection);
      List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
      Record record = null;
      while ((record = lineReceiver.getFromReader()) != null) {
        writerBuffer.add(record);
        if (writerBuffer.size() >= this.batchSize) {
          doBatchInsert(col, writerBuffer, mongodbColumnMeta);
          writerBuffer.clear();
        }
      }
      if (!writerBuffer.isEmpty()) {
        doBatchInsert(col, writerBuffer, mongodbColumnMeta);
        writerBuffer.clear();
      }
    }

    private void doBatchInsert(MongoCollection<Document> collection, List<Record> writerBuffer,
        JSONArray columnMeta) {

      List<WriteModel<Document>> dataList = new ArrayList<WriteModel<Document>>();

      for (Record record : writerBuffer) {
        Document data = new Document();
        try {
          for (int i = 0; i < record.getColumnNumber(); i++) {
            Column column = record.getColumn(i);
            String type = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_TYPE);
            String fieldName = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME);
            SupportMongodbDataType supportMongodbDataType = SupportMongodbDataType.getType(type);

            if (supportMongodbDataType == null) {
              String msg = MessageFormat.format("Unsupported field type: {0}", type);
              throw new Exception(msg);
            }
            supportMongodbDataType.parseDataXType(fieldName, column, data);

          }

        } catch (Exception e) {
          //处理脏数据
          super.getTaskPluginCollector().collectDirtyRecord(record, e);
          continue;
        }

        //对于不同的写入模式
        switch (writeMode) {
          case KeyConstant.WRITE_MODE_OVER: {
            collection.deleteMany(new Document());
            break;
          }

          case KeyConstant.WRITE_MODE_INSERT: {
            dataList.add(new InsertOneModel<>(data));
            break;
          }
          case KeyConstant.WRITE_MODE_UPDATE: {
            //组装upsert
            Document upsertVal = MongoUtil.getUpsertVal(data, upsertKey);
            dataList.add(new UpdateOneModel<>(upsertVal, new Document("$set", data),
                new UpdateOptions().upsert(false)));
            break;
          }
          case KeyConstant.WRITE_MODE_UPSET: {
            //组装upsert
            Document upsertVal = MongoUtil.getUpsertVal(data, upsertKey);
            dataList.add(
                new UpdateOneModel<>(upsertVal, new Document("$set", data),
                    new UpdateOptions().upsert(true)));
            break;
          }
          default: {
            String message = MessageFormat.format("不支持的写入类型：{0}", writeMode);
            throw DataXException
                .asDataXException(MongoDBWriterErrorCode.UNSUPPORTED_WRITE_MODE, message);
          }
        }
      }

      collection.bulkWrite(dataList);
    }


    @Override
    public void init() {
      this.writerSliceConfig = this.getPluginJobConf();
      this.userName = writerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
      this.password = writerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
      this.database = writerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
      if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
        this.mongoClient = MongoUtil
            .initCredentialMongoClient(this.writerSliceConfig, userName, password, database);
      } else {
        this.mongoClient = MongoUtil.initMongoClient(this.writerSliceConfig);
      }
      this.collection = writerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
      this.batchSize = BATCH_SIZE;
      this.mongodbColumnMeta = JSON
          .parseArray(writerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
      this.writeMode = writerSliceConfig.getString(KeyConstant.WRITE_MODE);

      this.upsertKey = writerSliceConfig.getString(KeyConstant.UPSERT_KEY);

      if (StringUtils.isEmpty(writeMode)) {
        String message = "写入模式(writeMode)不得为空！";
        throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, message);
      }

      //如果writeMode 是update 或者 REPLACE 需要生成条件
      if (Arrays.asList(KeyConstant.WRITE_MODE_UPSET, KeyConstant.WRITE_MODE_UPDATE)
          .contains(writeMode) && StringUtils.isEmpty(upsertKey)) {
        String message = "因为你配置了update/replace写入模式（writeMode），所以upserKey不得为空！";
        throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, message);
      }
    }

    @Override
    public void destroy() {
      mongoClient.close();
    }
  }

}
