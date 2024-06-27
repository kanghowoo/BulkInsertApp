import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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

    public static final long USER_ID = 1;
    public static final String APPLICATION_YML = "/application.yml";
    public static final int BATCH_SIZE = 1000;

    @Parameter(names = {"--count"}, description = "Number of records to insert", required = true)
    private int count;

    @Parameter(names = {"--dry-run"},
            description = "Check app will work well or not before run app. "
                          + "default value is true, it means app just return query statement",
            arity = 1)
    private boolean dryrun = true;

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

        try (Connection connection = DriverManager.getConnection(url, user, password);
             BufferedWriter writer = new BufferedWriter(new FileWriter("BulkInsertApp_query.txt"))
        ) {
            connection.setAutoCommit(false);

            try (Statement statement = connection.createStatement()) {
                for (long i = 1; i <= count; i += BATCH_SIZE) {
                    StringBuilder sql = getBatchQuery(i);

                    if (dryrun) {
                        writer.write(sql.toString());
                        continue;
                    }

                    statement.executeUpdate(sql.toString());
                }

                if (!dryrun) connection.commit();
            } catch (SQLException e) {
                System.out.println("sql statement error : " + e.getErrorCode());
                connection.rollback();
                System.exit(1);
            }
        } catch (SQLException e) {
            System.out.println("database connection error : " +e.getErrorCode());
            System.exit(1);
        } catch (IOException e) {
            System.out.println("dry run - file out error." + e.getMessage());
            System.exit(1);
        }
    }

    private static StringBuilder getBatchQuery(long i) {
        StringBuilder sql = new StringBuilder("INSERT INTO boards (title, content, user_id) VALUES ");
        final String title = "test_title";
        final String content = "test_content";

        for (int j = 1; j <= BATCH_SIZE; j++) {
            final long number = (i - 1) + j;
            sql.append(String.format("('%s', '%s', %d)", title + number, content + number, USER_ID));

            if (j < BATCH_SIZE) {
                sql.append(", ");
            }
        }
        return sql;
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
