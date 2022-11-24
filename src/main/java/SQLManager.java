import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.function.Consumer;

public class SQLManager {
    //region FIELDS
    private static final String SKIPPED_QUERY_WARNING = "WARNING: EMPTY QUERY SKIPPED";
    private String url;
    private String username;
    private String password;

    /**
     * Construye un objeto SQLManager a partir de los datos de la base de datos
     *
     * @param url
     * @param username
     * @param password
     */
    public SQLManager(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    /**
     * Método auxiliar que toma como parámetro un array de consultas SQL (extraído a través del método
     * {@link SQLManager#getQueryString}) y en caso de que haya consultas vacías las elimina.
     *
     * @param queryString Un array de {@link String} compuesto de consultas SQL
     */
    private static void rmEmptyQuery(String[] queryString) {
        int emptyStrings = 0;
        boolean removeEmptyQuery = false;
        for (String s : queryString) {
            if (s.isEmpty()) {
                emptyStrings++;
                if (!removeEmptyQuery) {
                    removeEmptyQuery = true;
                }
            }
        }
        if (removeEmptyQuery) {
            String[] auxArray = new String[queryString.length - emptyStrings];
            int arrayCursor = 0;
            for (int i = 0; i < auxArray.length; i++) {
                for (int j = arrayCursor; j < queryString.length; j++) {
                    if (!queryString[i].isEmpty() || queryString[i] != null) {
                        auxArray[i] = queryString[j];
                        arrayCursor = j;
                        break;
                    }
                }
            }
            queryString = auxArray;
        }
    }

    /**
     * Devuelve un array de {@link String} compuesto por sentencias SQL, este array se obtiene leyendo un script SQL que
     * debemos crear previamente.<br>
     * <dl>
     *   <dt><strong>REQUISITOS DEL FICHERO .sql:</strong></dt>
     *   <dd>En la primera línea del fichero debemos usar la palabra clave <i>DELIMITER</i> junto con el delimitador
     *   personalizado que usaremos en el script para separar las sentencias SQL. Evitar cualquier espacio o salto de línea
     *   innecesario. <br>Los comentarios están admitidos.</dd>
     * </dl>
     *
     * @param scriptPath Ruta del fichero .sql
     * @param delimiter  Delimitador que se usa en el fichero para separar las sentencias SQL
     * @return Un array de {@link String} con las sentencias SQL separadas
     * @throws IOException
     */
    public static String[] getQueryString(String scriptPath, String delimiter) throws IOException {
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
                    queryString = allFileContent.toString().replaceAll("--\s.*\n|#.*\n|/[*].*[*]/|DELIMITER.*\n|", "").split(delimiter);
                    rmEmptyQuery(queryString);
                }
            }
        }
        return queryString;
    }
    //endregion

    //region GETTERS/SETTERS
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }
    //endregion

    //region CONSTRUCTORS

    public void setUsername(String username) {
        this.username = username;
    }
    //endregion

    //region AUX METHODS

    public String getPassword() {
        return password;
    }
    //endregion

    //region STATIC METHODS

    public void setPassword(String password) {
        this.password = password;
    }
    //endregion

    //region PUBLIC METHODS

    /**
     * @return Devuelve una instancia de la clase {@link Connection}
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(this.url, this.username, this.password);
    }

    /**
     * Ejecuta una sentencia SQL DML dinámica o estática
     *
     * @param queryString Sentencia SQL
     * @param type        Los valores a sustituir en caso de tratarse de una sentencia SQL dinámica con <strong>?</strong>
     * @param <T>
     * @return <strong>int</strong> Número de filas afectadas
     */
    @SafeVarargs
    public final <T> int executeUpdate(String queryString, T... type) {
        int affectedRows = 0;
        if (!queryString.isEmpty()) {
            try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
                if (type.length == 0) {
                    try (Statement statement = connection.createStatement()) {
                        affectedRows = statement.executeUpdate(queryString);
                    }

                } else {
                    try (PreparedStatement preparedStatement = connection.prepareStatement(queryString)) {
                        for (int i = 0; i < type.length; i++) {
                            preparedStatement.setObject(i + 1, type[i]);
                        }
                        affectedRows = preparedStatement.executeUpdate();
                    }
                }
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }
        } else System.err.println(SKIPPED_QUERY_WARNING);
        return affectedRows;
    }

    /**
     * Ejecuta un conjunto de sentencias SQL DML, todas deben ser dinámicas o estáticas
     *
     * @param queryString Sentencias SQL
     * @param type        Los valores a sustituir en caso de tratarse de una sentencia SQL dinámica con <strong>?</strong>
     * @param <T>
     * @return <strong>int[]</strong> Número de filas afectadas
     */
    @SafeVarargs
    public final <T> int[] executeUpdate(String[] queryString, T... type) {
        int[] affectedRows = new int[queryString.length];
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            if (type.length == 0) {
                try (Statement statement = connection.createStatement()) {
                    for (int i = 0; i < queryString.length; i++) {
                        if (!queryString[i].isEmpty()) {
                            affectedRows[i] = statement.executeUpdate(queryString[i]);
                        } else System.err.println(SKIPPED_QUERY_WARNING);

                    }
                }
            } else {
                int usedQM = 0;
                for (int i = 0; i < queryString.length; i++) {
                    if (!queryString[i].isEmpty()) {
                        try (PreparedStatement preparedStatement = connection.prepareStatement(queryString[i])) {
                            for (int j = 0; j < preparedStatement.getParameterMetaData().getParameterCount(); j++) {
                                preparedStatement.setObject(j + 1, type[usedQM]);
                                usedQM += 1;
                            }
                            affectedRows[i] = preparedStatement.executeUpdate();
                        }
                    } else System.err.println(SKIPPED_QUERY_WARNING);

                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return affectedRows;
    }

    /**
     * Ejecuta una consulta SQL DQL, dinámica o estática, solo funciona si el resultado de la consulta es un {@link ResultSet},
     * si el resultado de la consulta es otro tipo de dato o múltiples {@link ResultSet}, no funcionará correctamente.
     *
     * @param queryString    Sentencia SQL
     * @param modelProcessor <strong>IMPORTANTE</strong> Será el método que procesará la información a partir del {@link ResultSet}
     *                       devuelto por la consulta
     * @param type           Los valores a sustituir en caso de tratarse de una sentencia SQL dinámica con <strong>?</strong>
     * @param <T>
     */
    @SafeVarargs
    public final <T> void executeQuery(String queryString, Consumer<ResultSet> modelProcessor, T... type) {
        if (!queryString.isEmpty()) {
            try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
                ResultSet resultSet;
                if (type.length == 0) {
                    try (Statement statement = connection.createStatement()) {
                        resultSet = statement.executeQuery(queryString);
                        modelProcessor.accept(resultSet);
                    }
                } else {
                    try (PreparedStatement preparedStatement = connection.prepareStatement(queryString)) {
                        for (int i = 0; i < type.length; i++) {
                            preparedStatement.setObject(i + 1, type[i]);
                        }
                        resultSet = preparedStatement.executeQuery();
                        modelProcessor.accept(resultSet);
                    }
                }
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }
        } else System.err.println(SKIPPED_QUERY_WARNING);
    }

    /**
     * Ejecuta múltiples consultas SQL DQL <strong>estáticas</strong> solo funciona si el resultado de las consultas es un {@link ResultSet},
     * si el resultado de las consultas es otro tipo de dato o múltiples {@link ResultSet}, no funcionará correctamente.
     *
     * @param queryString    Sentencia SQL
     * @param modelProcessor <strong>IMPORTANTE</strong> Será el método que procesará la información a partir del {@link ResultSet}
     *                       devuelto por la consulta
     */
    public final void executeQuery(String[] queryString, Consumer<ResultSet> modelProcessor) {
        try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password)) {
            ResultSet resultSet;
            try (Statement statement = connection.createStatement()) {
                for (String s : queryString) {
                    if (!s.isEmpty()) {
                        resultSet = statement.executeQuery(s);
                        modelProcessor.accept(resultSet);
                    } else System.err.println(SKIPPED_QUERY_WARNING);
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }
    //endregion
}
