# Text2Hive
__NOTE: This is meant as a learning exercise for me for Scala/sbt/Hadoop APIs.__

Creates a Hive Table and copies the folder to HDFS based on the xml(s) passed via arguments.

Steps to Run -
1. sbt assembly
2. Pass the command
    java -jar Text2Hive-assembly-0.1.jar settings.xml

Example XML Config file
```xml
<?xml version="1.0" encoding="UTF-8"?>
<config>
  <coreXml>/etc/hadoop/conf/core-site.xml</coreXml>
  <hdfsXml>/etc/hadoop/conf/hdfs-site.xml</hdfsXml>
  <hiveXml>/etc/hive/conf/hive-site.xml</hiveXml>
  <userKeytab>/home/neil/neil.keytab</userKeytab>
  <src>/home/neil/movie_metadatav</src>
  <dest>/user/neil/movie_metadata</dest>
  <tableType>internal</tableType>
  <isHeaders>yes</isHeaders>
  <delimiter>,</delimiter>
  <quoteChar>"</quoteChar>
  <escapeChar>\</escapeChar>
  <dbTable>default.movie_metadata</dbTable>
  <thriftServer>localhost:10011/default</thriftServer>
</config>
```
Couple Notes:
 - src Folder is meant to Linux FS and Dest folder is HDFS
 - If tableType is internal then dest (HDFS) is ignored and it instead moves the folder to hive warehouse directory
 - dbTable requires both database and tablename
 - If isHeaders is yes then will assume the file(s) have the header in the first line of the file if not will create its own
 - If Kerberos is required please specify Keytab
