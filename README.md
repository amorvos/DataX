![Datax-logo](https://github.com/alibaba/DataX/blob/master/images/DataX-logo.jpg)



# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、OTS、ODPS 等各种异构数据源之间高效的数据同步功能。



# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。



# DataX详细介绍

##### 请参考：[DataX-Introduction](https://github.com/alibaba/DataX/wiki/DataX-Introduction)

# DataX 编译

在源码根目录下执行命令

```sh
mvn -U clean package assembly:assembly -Dmaven.test.skip=true
```

# Support Data Channels

DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据计算系统都已经接入，目前支持数据如下图，详情请点击：[DataX数据源参考指南](https://github.com/alibaba/DataX/wiki/DataX-all-data-channels)

| 类型           | 数据源        | Reader(读) | Writer(写) |
| ------------ | ---------- | :-------: | :-------: |
| RDBMS 关系型数据库 | Mysql      |     √     |     √     |
|              | Oracle     |     √     |     √     |
|              | SqlServer  |     √     |     √     |
|              | Postgresql |     √     |     √     |
|              | 达梦         |     √     |     √     |
| 阿里云数仓数据存储    | ODPS       |     √     |     √     |
|              | ADS        |           |     √     |
|              | OSS        |     √     |     √     |
|              | OCS        |     √     |     √     |
| NoSQL数据存储    | OTS        |     √     |     √     |
|              | Hbase0.94  |     √     |     √     |
|              | Hbase1.1   |     √     |     √     |
|              | MongoDB    |     √     |     √     |
| 无结构化数据存储     | TxtFile    |     √     |     √     |
|              | FTP        |     √     |     √     |
|              | HDFS       |     √     |     √     |



# 项目成员

核心Contributions:  光戈、一斅、祁然、云时

感谢天烬、巴真对DataX做出的贡献。

# 
请及时提出issue给我们。请前往：[DataxIssue](https://github.com/baifendian/DataX/issues)


