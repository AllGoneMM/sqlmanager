import java.lang.reflect.Type;
import java.sql.*;
import java.util.List;
import java.lang.Class;

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

    public int executeUpdate(String sqlQuery, List<Object> type) {
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

//    public void execute(String sqlQuery){
//        try (Connection connection = DriverManager.getConnection(url, username, password)) {
//            try (Statement statement = connection.createStatement()) {
//                statement.executeUpdate(sqlQuery);
//            }
//        } catch (SQLException e) {
//            System.err.println(e.getMessage());
//        }
//    }
}
