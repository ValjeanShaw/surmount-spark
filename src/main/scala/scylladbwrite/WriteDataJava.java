package scylladbwrite;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author xiaoran
 * @date 2019/12/02
 */
public class WriteDataJava {
    public static void main(String[] args) {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoint("172.23.7.13,172.23.7.14,172.23.7.16")
                    .build();
            Session session = cluster.connect();

            ResultSet rs = session.execute("select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));
        } finally {
            if (cluster != null) cluster.close();
        }
    }
}
