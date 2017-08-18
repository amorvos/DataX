package com.alibaba.datax.plugin.writer.mongodbwriter.util;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mongodbwriter.KeyConstant;
import com.alibaba.datax.plugin.writer.mongodbwriter.MongoDBWriter;
import com.alibaba.datax.plugin.writer.mongodbwriter.MongoDBWriterErrorCode;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
import java.net.UnknownHostException;
import java.util.*;

public class MongoUtil {

  private static final Logger logger = LoggerFactory.getLogger(MongoUtil.class);

  public static MongoClient initMongoClient(Configuration conf) {

    List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
    if (addressList == null || addressList.size() <= 0) {
      throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
    }
    try {
      return new MongoClient(parseServerAddress(addressList));
    } catch (UnknownHostException e) {
      throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS, "不合法的地址");
    } catch (NumberFormatException e) {
      throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
    } catch (Exception e) {
      throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
    }
  }

  public static MongoClient initCredentialMongoClient(Configuration conf, String userName,
      String password, String database) {

    List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
    if (!isHostPortPattern(addressList)) {
      throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
    }
    try {
      MongoCredential credential = MongoCredential
          .createCredential(userName, database, password.toCharArray());
      return new MongoClient(parseServerAddress(addressList), Arrays.asList(credential));

    } catch (UnknownHostException e) {
      throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS, "不合法的地址");
    } catch (NumberFormatException e) {
      throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
    } catch (Exception e) {
      throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
    }
  }

  /**
   * 判断地址类型是否符合要求
   */
  private static boolean isHostPortPattern(List<Object> addressList) {
    for (Object address : addressList) {
      String regex = "(\\S+):([0-9]+)";
      if (!((String) address).matches(regex)) {
        return false;
      }
    }
    return true;
  }

  /**
   * 转换为mongo地址协议
   */
  private static List<ServerAddress> parseServerAddress(List<Object> rawAddressList)
      throws UnknownHostException {
    List<ServerAddress> addressList = new ArrayList<ServerAddress>();
    for (Object address : rawAddressList) {
      String[] tempAddress = ((String) address).split(":");
      try {
        ServerAddress sa = new ServerAddress(tempAddress[0], Integer.valueOf(tempAddress[1]));
        addressList.add(sa);
      } catch (Exception e) {
        throw new UnknownHostException();
      }
    }
    return addressList;
  }

  /**
   * 把Record 插入到 BDObject 中
   */
  public static void putDBObject(Document document, String fieldName, Object value) {
    String[] fieldList = fieldName.split("\\.");
    for (int i = 0, len = fieldList.length; i < len; i++) {
      String field = fieldList[i];

      boolean islast = false;
      //检查是不是最后一个
      if (i == len - 1) {
        islast = true;
      }

      if (isFieldList(field)) {
        if (islast) {
          //如果是最后一个尝试赋值。
          handleList(document, field, value);
        } else {
          //如果不是最后一个，就尝试使用docment占位
          document = handleList(document, field, new Document());
        }
      } else {
        //如果不是一个数组
        if (islast) {
          handleObject(document, field, value);
        } else {
          document = handleObject(document, field, new Document());
        }
      }
    }
  }

  /**
   * 向指定的docment中插入一个含下标的键值对。如果已经有值了就放弃插入，如有没有则插入。返回插入的值。
   */
  public static <T> T handleList(Document document, String field, T value) {
    String fieldListName = getFieldListName(field);
    int index = getFieldListIndex(field);
    List<T> documentList = document.get(fieldListName, List.class);
    if (documentList == null) {
      documentList = new ArrayList<T>();
      document.append(fieldListName, documentList);
    }

    //如果数组不够长需要补全数组
    if (documentList.size() < index + 1) {
      while (documentList.size() != index) {
        documentList.add(null);
      }
      documentList.add(value);
    } else {
      //如果值是空的就赋值，如果不是空的，遵循先插入原则，放弃后插入的值
      if (documentList.get(index) == null) {
        documentList.set(index, value);
      }
    }

    return documentList.get(index);
  }

  /**
   * 向指定的docment中插入一个不含下标的键值对。
   */
  public static <T> T handleObject(Document document, String field, T value) {
    T fieldDoc = (T) document.get(field);
    if (fieldDoc == null) {
      fieldDoc = value;
      document.append(field, value);
    }
    return fieldDoc;
  }

  /**
   * 判断字段是否含有下标
   */
  public static boolean isFieldList(final String field) {
    return field.contains("[") && field.contains("]");
  }

  /**
   * 获取一个含有下标的字符串的字段名称
   */
  public static String getFieldListName(final String field) {
    return field.substring(0, field.indexOf("["));
  }

  /**
   * 获取一个含有下标的字符串的下标
   */
  public static int getFieldListIndex(final String field) {
    return Integer.valueOf(field.substring(field.indexOf("[") + 1, field.indexOf("]")));
  }

  /**
   * 获取一个字符串的名称和下标
   */
  public static Map.Entry<String, String> getFieldAndSubscript(String field) {
    //TODO 分析一个字段的名称和下标
    return null;
  }

  /**
   * 取一个Document 指定字段的值
   */
  public static Object getDocumentValue(Document document, String fieldName) {
    Object value = null;
    String[] fieldList = fieldName.split("\\.");
    for (int i = 0, len = fieldList.length; i < len; i++) {
      String field = fieldList[i];

      boolean islast = false;
      //检查是不是最后一个
      if (i == len - 1) {
        islast = true;
      }

      if (isFieldList(field)) {
        String fieldListName = getFieldListName(field);
        int index = getFieldListIndex(field);
        List valList = document.get(fieldListName, List.class);
        if (valList == null) {
          break;
        }
        Object val = valList.get(index);

        if (islast) {
          value = val;
        } else {
          document = (Document) val;
        }

      } else {
        Object val = document.get(field);
        if (islast) {
          value = val;
        } else if (val != null) {
          document = (Document) val;
        } else {
          break;
        }
      }
    }
    return value;
  }

  /**
   * 获取upsert val
   * @param document
   * @param upsertKeys
   * @return
   */
  public static Document getUpsertVal(Document document, String upsertKeys) {
    String[] upsertKeyArr = upsertKeys.split(",");

    Document upsertVal = new Document();

    for (String upsertKey : upsertKeyArr) {
      upsertVal.append(upsertKey, MongoUtil.getDocumentValue(document, upsertKey));
    }
    return upsertVal;
  }


  public static void main(String[] args) {
//    try {
//      ArrayList hostAddress = new ArrayList();
//      hostAddress.add("127.0.0.1:27017");
//      System.out.println(MongoUtil.isHostPortPattern(hostAddress));
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    String fieldList = "abc[11]";
    System.out.println(getFieldListName(fieldList));
    System.out.println(getFieldListIndex(fieldList));

    String field = "user.name.c.e[2].t[3].f[1]";
    String field2 = "user.name.c.e[2].t[3].f[0]";
    String field3 = "a.b.c.e[2].t[2].f[0]";
    String field4 = "user.name.c.e[2].t[3]";
    //String field = "a[10]";
    String value = "helloword!";

    Document document = new Document();

    putDBObject(document, field, value);
    putDBObject(document, field2, value);

    System.out.println(document.toJson());

    System.out.println(getDocumentValue(document, field3));

    Document document1 = Document.parse(
        "{\"job\":{\"setting\":{\"speed\":{\"channel\":1}},\"content\":[{\"reader\":{\"name\":\"mysqlreader\",\"parameter\":{\"username\":\"root\",\"password\":\"root\",\"connection\":[{\"querySql\":[\"select db_id,on_line_flag from db_info where db_id < 10;\"],\"jdbcUrl\":[\"jdbc:mysql://bad_ip:3306/database\",\"jdbc:mysql://127.0.0.1:bad_port/database\",\"jdbc:mysql://127.0.0.1:3306/database\"]}]}},\"writer\":{\"name\":\"streamwriter\",\"parameter\":{\"print\":false,\"encoding\":\"UTF-8\"}}}]}}");

    System.out.println(document1.toJson());
  }


}
