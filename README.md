## QHBase Compact

### Introduction
QHBase Compact is used for DBA make major compaction automatic,
you can run it at backgroud, and it do major compact for a table
at a given time range when the pressure is low.

Each time the QHBase Compact just do one major compact for one region,
So the impact for the cluster is very small.

### How to compile

```
mvn package
mvn dependency:copy-dependencies
```

### How to use
1. Modify the conf/config.properties.
    * tablename: the table you want to do major compaction
    * starttime: the time start to do major compaction
    * endtime: the time major compaction stop
    * marjorfilesize: the max file size for region major compaction.
2. Modify the hbase-site.xml according to your cluster configuration.
3. Run the HBase-Compact.
```
java -cp ./dependency/*:./conf/*:./* com.qunar.dba.QHBaseCompact &
```

