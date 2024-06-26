import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class BulkInsertApp {

    public static final int USER_ID = 1;
    public static final String APPLICATION_YML = "/application.yml";
    public static final int BATCH_SIZE = 1000;

    @Parameter(names = {"--count"}, description = "Number of records to insert", required = true)
    private int count;

    public static void main(String[] args) {
        BulkInsertApp app = new BulkInsertApp();
        JCommander.newBuilder().addObject(app).build().parse(args);
        app.run();
    }

    private void run() {
        if (count < 1) {
            System.out.println("count should be bigger than 1");
            System.exit(1);
        }

        Map<String, Object> config = yamlConfigReader();
        Map<String, String> dataSource = (Map<String, String>) config.get("datasource");
        final String url = dataSource.get("url");
        final String user = dataSource.get("user");
        final String password = String.valueOf(dataSource.get("password"));

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            connection.setAutoCommit(false);

            try (Statement statement = connection.createStatement()) {
                for (int i = 1; i <= (count / BATCH_SIZE) + 1; i++) {
                    StringBuilder sql = new StringBuilder("INSERT INTO boards (title, content, user_id) VALUES ");

                    for (int j = 1; j <= BATCH_SIZE; j++) {
                        sql.append("('")
                           .append("test_title").append(i).append("', '")
                           .append("test_content").append(i).append("', '")
                           .append(USER_ID)
                           .append("')");

                        if (j < BATCH_SIZE) {
                            sql.append(", ");
                        }
                    }

                    statement.executeUpdate(sql.toString());
                }

                connection.commit();
            } catch (SQLException e) {
                System.out.println("sql statement error : " + e.getErrorCode());
                connection.rollback();
                System.exit(1);
            }
        } catch (SQLException e) {
            System.out.println("database connection error : " +e.getErrorCode());
            System.exit(1);
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
