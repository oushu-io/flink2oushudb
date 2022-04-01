package org.apache.flink.streaming.connectors.oushudb.internals;

import org.apache.flink.streaming.connectors.oushudb.FlinkOushuDBErrorCode;
import org.apache.flink.streaming.connectors.oushudb.FlinkOushuDBException;
import org.apache.flink.table.types.logical.*;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class OushuDBTableInfo implements Serializable {
    private static final long serialVersionUID = 4756224197664070827L;

    private final String schema;
    private final String table;
    private final Field[] fields;
    private transient Connection connection;

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public OushuDBTableInfo(String schema, String table, Field[] fields) {
        this.schema = schema;
        this.table = table;
        this.fields = fields;
    }

    public String[] getFieldsName() {
        String[] names = new String[this.fields.length];
        for (int i = 0; i < names.length; i++) {
            names[i] = this.fields[i].name;
        }
        return names;
    }

    public LogicalType[] getFieldsType() {
        LogicalType[] types = new LogicalType[this.fields.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = this.fields[i].type;
        }
        return types;
    }

    public String getSchema() {
        return this.schema;
    }

    public String getTable() {
        return this.table;
    }

    public void createTable() throws Exception {
        checkConnection();
        StringBuilder sb = new StringBuilder();
        sb.append(
                String.format("create table %s.%s (", withQuotation(schema), withQuotation(table)));
        for (Field field : fields) {
            sb.append(String.format("%s %s,", field.name, field.typeStr()));
        }
        sb.setLength(sb.length() - 1);
        sb.append(") WITH (appendonly=true, orientation=orc, compresstype=lz4);");
        PreparedStatement prst = null;
        try {
            prst = connection.prepareStatement(sb.toString());
            prst.execute();
        } catch (SQLException e) {
            if (!e.getMessage().contains("already exists")) {
                throw e;
            }
        } finally {
            if (prst != null) {
                prst.close();
            }
        }
    }

    public void checkConnection() throws FlinkOushuDBException {
        if (connection == null) {
            throw new FlinkOushuDBException(
                    FlinkOushuDBErrorCode.EXTERNAL_ERROR, "connection is null");
        }
    }

    // check table exists, and data type is compatibility
    public boolean exists() throws Exception {
        checkConnection();

        String existsSql =
                String.format(
                        "select count(1) as cnt from information_schema.tables "
                                + "where table_schema = '%s' and table_name = '%s';",
                        schema, table);
        PreparedStatement prst = null;
        try {
            prst = connection.prepareStatement(existsSql);
            ResultSet result = prst.executeQuery();
            while (result.next()) {
                int count = result.getInt("cnt");
                return count > 0;
            }
        } finally {
            if (prst != null) {
                prst.close();
            }
        }
        return false;
    }

    public void checkCompatibility() throws Exception {
        checkConnection();
        String typeSql =
                String.format(
                        "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale from information_schema.columns "
                                + "where table_schema='%s' and table_name = '%s' order by ordinal_position;",
                        schema, table);
        PreparedStatement prst = null;
        try {
            prst = connection.prepareStatement(typeSql);
            ResultSet result = prst.executeQuery();
            List<DBField> fields = new ArrayList<>();
            while (result.next()) {
                fields.add(
                        new DBField(
                                result.getString("column_name"),
                                result.getString("data_type"),
                                result.getInt("character_maximum_length"),
                                result.getInt("numeric_precision"),
                                result.getInt("numeric_scale")));
            }

            for (int i = 0; i < this.fields.length; i++) {
                DBField df = fields.get(i);
                Field f = this.fields[i];
                if (!f.typeStr().equals(df.typeStr())) {
                    throw new FlinkOushuDBException(
                            FlinkOushuDBErrorCode.EXTERNAL_ERROR,
                            "Insert data type "
                                    + f.typeStr()
                                    + " is incompatible with table type "
                                    + df.typeStr());
                }
            }
        } finally {
            if (prst != null) {
                prst.close();
            }
        }
    }

    public String withQuotation(String val) {
        return String.format("\"%s\"", val);
    }

    public void insert(String location) throws Exception {
        checkConnection();
        String externalTable = UUID.randomUUID().toString();
        Statement statement = null;

        try {
            connection.setAutoCommit(false);
            statement =
                    connection.createStatement(
                            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            StringBuilder sb = new StringBuilder();
            // create external table
            sb.append("create readable external table ");
            sb.append(withQuotation(schema));
            sb.append(".");
            sb.append(withQuotation(externalTable));
            sb.append(" (like ");
            sb.append(withQuotation(schema));
            sb.append(".");
            sb.append(withQuotation(table));
            sb.append(") location('");
            sb.append(location);
            sb.append("') format 'orc';");
            statement.execute(sb.toString());
            sb.setLength(0);
            // insert data to inner table
            sb.append("insert into ");
            sb.append(withQuotation(schema));
            sb.append(".");
            sb.append(withQuotation(table));
            sb.append(" select * from ");
            sb.append(withQuotation(schema));
            sb.append(".");
            sb.append(withQuotation(externalTable));
            sb.append(";");
            statement.execute(sb.toString());
            // drop external table
            sb.setLength(0);
            sb.append("drop external table ");
            sb.append(withQuotation(schema));
            sb.append(".");
            sb.append(withQuotation(externalTable));
            sb.append(";");
            statement.execute(sb.toString());
            connection.commit();
            connection.setAutoCommit(true);
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    public static class Field implements Serializable {
        private static final long serialVersionUID = -3723477861407241259L;
        public String name;
        public LogicalType type;

        public Field(String name, LogicalType type) {
            this.name = name;
            this.type = type;
        }

        public String typeStr() {
            switch (type.getTypeRoot()) {
                case CHAR:
                    return String.format("char(%d)", ((CharType) type).getLength());
                case VARCHAR:
                    int len = ((VarCharType) type).getLength();
                    if (len == 2147483647) {
                        return "varchar";
                    }
                    return String.format("varchar(%d)", len);
                case BOOLEAN:
                    return "bool";
                case BINARY:
                case VARBINARY:
                    return "bytea";
                case DECIMAL:
                    {
                        DecimalType dt = (DecimalType) type;
                        return String.format("numeric(%d, %d)", dt.getPrecision(), dt.getScale());
                    }
                case TINYINT:
                case SMALLINT:
                    return "smallint";
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case INTEGER:
                    return "integer";
                case BIGINT:
                    return "bigint";
                case FLOAT:
                    return "real";
                case DOUBLE:
                    return "double precision";
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return "timestamp without time zone";
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return "timestamp with time zone";
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        }
    }

    public static class DBField implements Serializable {
        private static final long serialVersionUID = -8687308305671173380L;
        public String name;
        public String type;
        public int charMaxLen;
        public int precision;
        public int scale;

        public DBField(String name, String type, int charMaxLen, int precision, int scale) {
            this.name = name;
            this.type = type;
            this.charMaxLen = charMaxLen;
            this.precision = precision;
            this.scale = scale;
        }

        public String typeStr() {
            switch (type) {
                case "numeric":
                    return String.format("numeric(%d, %d)", precision, scale);
                case "character varying":
                    if (charMaxLen > 0) {
                        return String.format("varchar(%d)", charMaxLen);
                    }
                    return "varchar";
                case "character":
                    return String.format("char(%d)", charMaxLen);
                default:
                    return type;
            }
        }
    }
}
