# Flink OushuDB Sink Function使用方法

### 引用
```xml
	<dependency>
		<groupId>org.apache.flink</groupId>
		<artifactId>flink-connector-oushudb_1.11</artifactId>
		<version>1.13.0</version>
	</dependency>

	<dependency>
		<groupId>org.apache.flink</groupId>
		<artifactId>flink-table-common</artifactId>
		<version>1.13.0</version>
	</dependency>
```

### 创建一个OushuDB sinker
```java
import org.apache.flink.table.types.logical.*;

public class Demo {
    public static void main(String[] args){
        // 定义表结构
        OushuDBTableInfo.Field[] fields = new OushuDBTableInfo.Field[]{
                    new OushuDBTableInfo.Field("a", new VarCharType()),
                    new OushuDBTableInfo.Field("b", new IntType())
                };
        
        String schema = "public";
        String table = "test";
        OushuDBTableInfo tableInfo = new OushuDBTableInfo(schema, table, fields);
        
        // 添加oushudb连接信息
        Properties config = new Properties();
        config.setProperty(FlinkOushuDBSink.OushuDB_HOSTS, "aiserver-1");
        config.setProperty(FlinkOushuDBSink.OushuDB_PORT, "5432");
        config.setProperty(FlinkOushuDBSink.OushuDB_DATABASE, "flink");
        config.setProperty(FlinkOushuDBSink.OushuDB_USER, "oushu");
        config.setProperty(FlinkOushuDBSink.OushuDB_PASSWORD, "********");
        config.setProperty(FlinkOushuDBSink.OushuDB_TEMP_PATH, "hdfs://aiserver-1:9000/flink/data");
                
        // 创建一个OushuDB sinker
        FlinkOushuDBSink sink = new FlinkOushuDBSink(tableInfo, config);
        out.addSink(sink).name("testOushuDBSink");
    }
}
```

### 编译插件，将依赖打到jar包
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.0.0</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <artifactSet>
                    <excludes>
                        <exclude>com.google.code.findbugs:jsr305</exclude>
                        <exclude>org.slf4j:*</exclude>
                        <exclude>log4j:*</exclude>
                    </excludes>
                </artifactSet>
                <filters>
                    <filter>
                        <!-- Do not copy the signatures in the META-INF folder.
                        Otherwise, this might cause SecurityExceptions when using the JAR. -->
                        <artifact>*:*</artifact>
                        <excludes>
                            <exclude>META-INF/*.SF</exclude>
                            <exclude>META-INF/*.DSA</exclude>
                            <exclude>META-INF/*.RSA</exclude>
                        </excludes>
                    </filter>
                </filters>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                        <mainClass>oushu.io.flink2oushudb.Flink2OushuDB</mainClass>
                    </transformer>
                </transformers>
            </configuration>
        </execution>
    </executions>
</plugin>
```
