import java.sql.*;
import java.util.List;
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

    public int executeUpdate(String sqlQuery) {
        int affectedRows = 0;
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            try (Statement statement = connection.createStatement()) {
                affectedRows = statement.executeUpdate(sqlQuery);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return affectedRows;
    }

    public <T> int executeUpdate(String sqlQuery,
                                 List<T> type) {
        int affectedRows = 0;
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                for (int i = 0; i < type.size(); i++) {
                    preparedStatement.setObject(i + 1, type.get(i));
                }
                affectedRows = preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return affectedRows;
    }



    public <T> List<T> executeQuery(String sqlQuery,
                                    Function<ResultSet, List<T>> modelProcessor) {
        List<T> result = null;
        ResultSet resultSet;
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            try (Statement statement = connection.createStatement()) {
                resultSet = statement.executeQuery(sqlQuery);
                result = modelProcessor.apply(resultSet);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return result;
    }

    public <T> List<T> executeQuery(String sqlQuery,
                                    List<Object> type,
                                    Function<ResultSet, List<T>> modelProcessor) {
        List<T> result = null;
        ResultSet resultSet;
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                for (int i = 0; i < type.size(); i++) {
                    preparedStatement.setObject(i + 1, type.get(i));
                }
                resultSet = preparedStatement.executeQuery();
                result = modelProcessor.apply(resultSet);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return result;
    }
//    public List<Object> execute(String sqlQuery){
//        try (Connection connection = DriverManager.getConnection(url, username, password)) {
//            try (Statement statement = connection.createStatement()) {
//                if(statement.execute(sqlQuery)){
//                    return statement.getResultSet();
//                }
//                if(statement.getMoreResults())
//            }
//        } catch (SQLException e) {
//            System.err.println(e.getMessage());
//        }
//    }
}
