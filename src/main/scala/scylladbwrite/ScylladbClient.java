package scylladbwrite;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author xiaoran
 * @date 2019/11/22
 */
public class ScylladbClient {
    private static Cluster cluster;
    private static Session session;
    private String url;
    private int port;
    private ConsistencyLevel consistencyLevel;
    private PreparedStatement preparedStatement;

    public ScylladbClient(String url, int port) {
        this.url=url;
        this.port=port;
    }

    public ScylladbClient(String url,int port,ConsistencyLevel consistencyLevel){
        this.url=url;
        this.port=port;
        this.consistencyLevel=consistencyLevel;
    }

    /**
     * 获取会话连接
     * @return
     */
    public Session initSession(){
        if (session == null) {
            synchronized (ScylladbClient.class) {
                if (session == null) {
                    PoolingOptions poolingOptions = new PoolingOptions();
                    poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, 32);
                    poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 2000)
                            .setMaxConnectionsPerHost(HostDistance.REMOTE, 15);
                    QueryOptions queryOptions=new QueryOptions();
                    if(consistencyLevel!=null){
                        queryOptions.setConsistencyLevel(consistencyLevel);
                    }
                    cluster = Cluster.builder()
                            .withQueryOptions(queryOptions)
                            .addContactPoints(url.split(","))
                            .withPort(port)
                            .withPoolingOptions(poolingOptions).build();
                    session = cluster.connect();
                }
            }
        }
        return session;
    }

    /**
     * 关闭连接
     */
    public void destory(){
        if (!session.isClosed()) {
            session.close();
            cluster.close();
        }
    }

    public PreparedStatement prepare(String prepare){
        preparedStatement = session.prepare(prepare);
        return preparedStatement;
    }

    public void insert(BoundStatement boundStatement){
        session.execute(boundStatement);
    }

    public void insertAsync(BoundStatement boundStatement){
        session.executeAsync(boundStatement);
    }


    public static void main(String[] args) {
        ScylladbClient scylladbClient = new ScylladbClient("172.23.7.13,172.23.7.14,172.23.7.16",9042);
        try {
//            BoundStatement boundStatement = new BoundStatement();


            ResultSet rs = scylladbClient.initSession().execute("select * from hbase.test limit 5");
            Row row = rs.one();
            System.out.println(row.getString("userid"));
            System.out.println(row.getString("title"));
            System.out.println(row.getString("content"));
        } finally {
            scylladbClient.destory();
        }
    }
}
