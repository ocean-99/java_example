package win.ocean99.util;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OracleQuery {

    private static int toCleanCharLen = 0;

    private static void exportTableDataCommon(String tableName, String restrict, Consumer<ResultSet> consumer) throws IOException {
        // 查询SQL
        StringBuilder sql = new StringBuilder();
        sql.append("select * from ");
        sql.append(tableName);
        if (restrict != null && !restrict.isEmpty()) {
            sql.append(" where ");
            sql.append(restrict);
        }

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = OracleJDBC.getConnection();
            ps = conn.prepareStatement(sql.toString());
            rs = ps.executeQuery();
            consumer.accept(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void exportTableDataAsJson(String tableName, String restrict) throws IOException {
        System.out.printf("导出%s表数据为JSON格式", tableName);
        exportTableDataCommon(tableName, restrict, (resultSet) -> {
            ResultSetMetaData metaData = null;
            int columnCount = 0;
            try (
                    FileWriter writer = new FileWriter(Paths.get("").toAbsolutePath() + "/" + tableName + ".json");
            ) {
                metaData = resultSet.getMetaData();
                columnCount = metaData.getColumnCount();

                System.out.print("已导出：");
                int count = 1;
                // 开始写入文件
                writer.write("{\"" + tableName + "\":");
                writer.write("[");
                while (resultSet.next()) {
                    writer.write("{");
                    StringBuilder kv = new StringBuilder();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        kv.append("\"").append(columnName).append("\"").append(":");
                        String columnValue = converterSqlObj2Str(resultSet.getObject(i));
                        if (columnValue == null) {
                            kv.append("null").append(",");
                        } else {
                            kv.append("\"").append(columnValue).append("\"").append(",");
                        }
                    }
                    kv.deleteCharAt(kv.length() - 1);
                    writer.write(kv + "},");
                    logExportCount(count);
                    count++;
                }
                writer.write("]");
                writer.write("}");
                System.out.println("\n结束");
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void exportTableDataAsSql(String tableName, String restrict) throws IOException {
        System.out.println("导出" + tableName + "表数据为SQL格式");
        exportTableDataCommon(tableName, restrict, (resultSet) -> {
            ResultSetMetaData metaData = null;
            int columnCount = 0;
            try (
                    FileWriter writer = new FileWriter(Paths.get("").toAbsolutePath() + "/" + tableName + ".sql")
            ) {
                metaData = resultSet.getMetaData();
                columnCount = metaData.getColumnCount();

                System.out.print("已导出：");
                int count = 1;
                // 开始写入文件
                while (resultSet.next()) {
                    StringBuilder columnNames = new StringBuilder();
                    StringBuilder columnValues = new StringBuilder();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        columnNames.append("\"").append(columnName).append("\"").append(",");
                        String columnValue = converterSqlObj2Str(resultSet.getObject(i));
                        if (columnValue == null) {
                            columnValues.append("null").append(",");
                        } else {
                            columnValues.append("\"").append(columnValue).append("\"").append(",");
                        }
                    }
                    columnNames.deleteCharAt(columnNames.length() - 1);
                    columnValues.deleteCharAt(columnValues.length() - 1);
                    String exportString = "insert into " +
                            tableName +
                            "(" + columnNames + ")" +
                            "VALUES" +
                            "(" + columnValues + ")";
                    writer.write(exportString + ";\n");
                    logExportCount(count);
                    count++;
                }
                System.out.println("\n结束");
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void exportTableDataAsSerialize(String tableName, String restrict) throws IOException {
        System.out.printf("导出%s表数据为Java序列化", tableName);
        exportTableDataCommon(tableName, restrict, (resultSet) -> {
            ResultSetMetaData metaData = null;
            int columnCount = 0;
            try (
                    FileOutputStream fileOutputStream = new FileOutputStream(Paths.get("").toAbsolutePath() + "/" + tableName + ".ser");
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)
            ) {
                metaData = resultSet.getMetaData();
                columnCount = metaData.getColumnCount();

                System.out.print("已导出：");
                int count = 1;
                // 开始写入文件
                ArrayList<JSONObject> jsonObjList = new ArrayList<>();
                while (resultSet.next()) {
                    JSONObject jsonObjectRow = new JSONObject();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        jsonObjectRow.put(columnName, converterSqlObj2Str(resultSet.getObject(i)));
                    }
                    jsonObjList.add(jsonObjectRow);
                    logExportCount(count);
                    count++;
                }
                System.out.println("\n查询结束");
                System.out.println("开始写入...");
                objectOutputStream.writeObject(jsonObjList);
                objectOutputStream.close();
                fileOutputStream.close();
                System.out.println("写入结束");
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static String converterSqlObj2Str(Object obj) {
        if (obj == null) {
            return null;
        }
        if ("null".equals(obj.toString())) {
            return null;
        }
        if (obj instanceof Clob) {
            return clobToString((Clob) obj);
        }
        return obj.toString();
    }

    public static void testConnection() {

        try {
            // 创建连接
            Connection connection = OracleJDBC.getConnection();
            // 如果连接成功，打印成功信息
            if (connection != null) {
                System.out.println("Oracle JDBC连接成功！");
                // 关闭连接
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("Oracle JDBC连接失败！");
            e.printStackTrace();
        }
    }

    /**
     * Clob转String
     */
    public static String clobToString(Clob clob) {
        StringBuilder sb = new StringBuilder();
        try {

            Reader reader = clob.getCharacterStream();
            BufferedReader br = new BufferedReader(reader);

            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

            br.close();
            reader.close();
            clob.free();
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }

        return sb.toString();
    }


    /**
     * 打印导出进度
     *
     * @param count 已导出的条数
     */
    private static void logExportCount(int count) {
        if (toCleanCharLen > 0) {
            for (int i = 0; i < toCleanCharLen; i++) {
                System.out.print("\b");
            }
        }
        System.out.print(count);
        toCleanCharLen = String.valueOf(count).length();
    }


    /**
     * 通过json文件导入指定table数据
     */
    public static void importJsonData(File jsonFile) {
        JSONObject jsonObject = JSON.parseJsonFile(jsonFile);
        Set<String> keys = jsonObject.keySet();
        for (String key : keys) {
            JSONArray jsonArray = jsonObject.getJSONArray(key);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject data = jsonArray.getJSONObject(i);
                StringBuilder sql = new StringBuilder("insert into " + key + "(");
                StringBuilder values = new StringBuilder("values(");
                Set<String> dataKeys = data.keySet();
                for (String dataKey : dataKeys) {
                    sql.append(dataKey).append(",");
                    values.append("'").append(data.get(dataKey)).append("'").append(",");
                }
                sql.deleteCharAt(sql.length() - 1);
                values.deleteCharAt(values.length() - 1);
                sql.append(") ").append(values).append(")");
                System.out.println(sql);
            }
        }
        System.out.println(jsonObject);
    }

    public static void importSerDataSingle(String jsonFile) throws Exception {
        importSerDataSingle(jsonFile, 50);
    }
    public static void importSerDataSingle(String jsonFile,int batchMax) throws Exception {
        long totalStart = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        ArrayList<JSONObject> jsonObjects = readSerFile(jsonFile);
        long end = System.currentTimeMillis();
        System.out.printf("读取数据完成,耗时%s毫秒共%s条,开始写入数据库...\n", end - start, jsonObjects.size());
        if (jsonObjects.isEmpty()) {
            return;
        }
        JSONObject firstJSONObj = jsonObjects.get(0);
        Set<String> keySet = firstJSONObj.keySet();
        String keys = String.join(",", keySet);
        String v = IntStream.range(0,keySet.size()).mapToObj(i -> "?").collect(Collectors.joining(","));
        String sql = "insert into " + jsonFile + "(" + keys + ") values(" + v + ")";
        Connection connection = OracleJDBC.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        int batchCount = 0;
        Reader reader = null;
        for (JSONObject jsonObject : jsonObjects) {
            Set<String> dataKeys = jsonObject.keySet();
            int index = 1;
            for (String dataKey : dataKeys) {
                Object value = jsonObject.get(dataKey);
                if (value == null) {
                    preparedStatement.setNull(index++, 0);
                }else if (value instanceof Float) {
                    preparedStatement.setFloat(index++, (Float) value);
                }else if(value instanceof Integer){
                    preparedStatement.setInt(index++, (Integer) value);
                } else if (value instanceof String) {
                    if (((String) value).length() > 3000) {
                        String bigString = (String) value;
                        reader = new StringReader(bigString);
                        preparedStatement.setClob(index++, reader, bigString.length());
                    }else {
                        preparedStatement.setString(index++, (String) value);
                    }
                }
            }
            preparedStatement.addBatch();
            ++batchCount;
            if (batchCount == batchMax) {
                start = System.currentTimeMillis();
                preparedStatement.executeBatch();
                end = System.currentTimeMillis();
                System.out.println("成功插入" + batchMax + "条耗时" + (end - start) + "毫秒");
                preparedStatement.clearBatch();
                batchCount = 0;
            }
        }
        // 执行剩余的批量插入操作
        if (batchCount > 0) {
            start = System.currentTimeMillis();
            int[] res = preparedStatement.executeBatch();
            end = System.currentTimeMillis();
            System.out.println("成功插入" + res.length + "条耗时" + (end - start) + "毫秒");
        }
        System.out.println("数据导入完成,共耗时:" + (System.currentTimeMillis() - totalStart) + "毫秒");

        if (reader != null) {
            reader.close();
        }
        preparedStatement.close();
    }

    @SuppressWarnings("unchecked")
    public static ArrayList<JSONObject> readSerFile(String jsonFile) {
        Path currentPath = Paths.get("");
        try (
                InputStream inputStream = Files.newInputStream(Paths.get(currentPath.toAbsolutePath() + "/" + jsonFile + ".ser"));
                ObjectInputStream objectInput = new ObjectInputStream(inputStream)
        ) {
            // 从文件中读取序列化的数据结构
            return (ArrayList<JSONObject>) objectInput.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
