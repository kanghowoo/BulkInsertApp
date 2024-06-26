import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class BulkInsertApp {

    public static final int USER_ID = 1;
    public static final String APPLICATION_YML = "/application.yml";

    @Parameter(names = {"--count"}, description = "Number of records to insert", required = true)
    private int count;

    public static void main(String[] args) {
        BulkInsertApp app = new BulkInsertApp();
        JCommander.newBuilder().addObject(app).build().parse(args);
        app.run();
    }

    private void run() {
        String insertSQL = "INSERT INTO boards"  + " (title, content, user_id) VALUES (?, ?, ?)";

        Map<String, Object> config = yamlConfigReader();
        Map<String, String> dataSource = (Map<String, String>) config.get("datasource");
        final String url = dataSource.get("url");
        final String user = dataSource.get("user");
        final String password = String.valueOf(dataSource.get("password"));

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            connection.setAutoCommit(false);
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
                for (int i = 1; i <= count; i++) {
                    preparedStatement.setString(1, "test_title" + i);
                    preparedStatement.setString(2, "test_content" + i);
                    preparedStatement.setInt(3, USER_ID);
                    preparedStatement.addBatch();

                    if (i % 1000 == 0 || i == count) {
                        preparedStatement.executeBatch();
                        connection.commit();
                    }
                }
            } catch (SQLException e) {
                connection.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Object> yamlConfigReader() {
        Yaml yaml = new Yaml();
        Map<String, Object> config = null;
        try (InputStream inputStream = BulkInsertApp.class.getResourceAsStream(APPLICATION_YML)) {
            config = yaml.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return config;
    }
}
