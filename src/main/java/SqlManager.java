import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.function.Function;

public class SqlManager {
    private String url;
    private String username;
    private String password;

    public SqlManager(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public static String[] getQueryString(String scriptPath) throws IOException {
        String[] queryString;
        try (FileInputStream fileInputStream = new FileInputStream(scriptPath)) {
            try (InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8)) {
                try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                    StringBuilder allFileContent = new StringBuilder();
                    final int dataLength = 2048;
                    char[] dataFromFile = new char[dataLength];
                    int readCharsCount;
                    while ((readCharsCount = bufferedReader.read(dataFromFile, 0, dataLength)) != -1) {
                        String partialRead = new String(dataFromFile, 0, readCharsCount);
                        allFileContent.append(partialRead);
                    }
                    queryString = allFileContent.toString().replaceAll("--\s.*\n|#.*\n|/[*].*[*]/", "").split(";");
                }
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
        return queryString;
    }

    @SafeVarargs
    public final <T> int executeUpdate(String sqlQuery, T... type) {
        int affectedRows = 0;
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            if (type.length == 0) {
                try (Statement statement = connection.createStatement()) {
                    affectedRows = statement.executeUpdate(sqlQuery);
                }

            } else {
                try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                    for (int i = 0; i < type.length; i++) {
                        preparedStatement.setObject(i + 1, type[i]);
                    }
                    affectedRows = preparedStatement.executeUpdate();
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return affectedRows;
    }

    @SafeVarargs
    public final <T, E> T executeQuery(String sqlQuery, Function<ResultSet, T> modelProcessor, E... type) {
        T result = null;
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            ResultSet resultSet;
            if (type.length == 0) {
                try (Statement statement = connection.createStatement()) {
                    resultSet = statement.executeQuery(sqlQuery);
                    result = modelProcessor.apply(resultSet);
                }
            } else {
                try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                    for (int i = 0; i < type.length; i++) {
                        preparedStatement.setObject(i + 1, type[i]);
                    }
                    resultSet = preparedStatement.executeQuery();
                    result = modelProcessor.apply(resultSet);
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return result;
    }
}
