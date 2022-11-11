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
