package util;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import config.Config;
import io.netty.channel.ChannelPromiseNotifier;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataUtl {

    /**
     * ES
     */

    private static Client client = null;

    public static Client getESClient() throws UnknownHostException {
        if (client == null) {
            TransportAddress address = new TransportAddress(InetAddress.getByName(Config.ES.URL), 9300);
            Settings settings = Settings.builder()
                    .build();

            client = new PreBuiltTransportClient(settings).addTransportAddress(address);
        }

        return client;
    }

    public static boolean createIndex(String index) throws UnknownHostException {
        CreateIndexRequestBuilder createIndexRequestBuilder = getESClient().admin().indices().prepareCreate(index);
        return createIndexRequestBuilder.execute().actionGet().isAcknowledged();
    }

    public static boolean deleteIndex(String index) throws UnknownHostException {
        DeleteIndexRequestBuilder deleteIndexRequestBuilder = getESClient().admin().indices().prepareDelete(index);
        return deleteIndexRequestBuilder.execute().actionGet().isAcknowledged();
    }

    public static SearchHits queryES(String index, QueryBuilder builder) throws UnknownHostException {
        return getESClient().prepareSearch(index).setQuery(builder).get().getHits();
    }

    public static void indexES(String index, String type, String id, XContentBuilder builder) throws UnknownHostException {
        getESClient().prepareIndex(index, type, id).setSource(builder).get();
        getESClient().admin().indices().prepareRefresh(index).get();
    }

    public static void flushES(String index) throws UnknownHostException {
        FlushRequest flushRequest = getESClient().admin().indices().prepareFlush(index).request();
        client.admin().indices().flush(flushRequest).actionGet();
    }

    /**
     * DB
     */

    private static Connection connection = null;

    public static Connection getDBConnection() throws SQLException {
        if (connection == null) {
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setUser(Config.DB.USERNAME);
            dataSource.setPassword(Config.DB.PASSWORD);

            connection = dataSource.getConnection();
        }

        return connection;
    }

    public static Statement getDBStatement() throws SQLException {
        if (statement == null) {
            statement = getDBConnection().createStatement();
        }

        return statement;
    }

    private static Statement statement = null;

    public static ResultSet queryDB(String dbName, String query) throws SQLException {
        Statement stm = getDBStatement();

        stm.executeQuery("USE " + dbName);
        return stm.executeQuery(query);
    }

    public static int insertAndGetID(String dbName, String insertQuery) throws SQLException {
        Statement stm = getDBStatement();

        stm.executeQuery("USE " + dbName);
        return stm.executeUpdate(insertQuery, Statement.RETURN_GENERATED_KEYS);
    }

    public static void batchInsert(String dbName, List<String> insertQueries) throws SQLException {
        Statement stm = getDBStatement();

        stm.executeQuery("USE " + dbName);

        for (String query : insertQueries) {
            stm.addBatch(query);
        }

        stm.executeBatch();
    }

    public static int getAutoIncrementID(PreparedStatement pstm) throws SQLException {
        ResultSet rs = pstm.getGeneratedKeys();
        rs.next();
        return rs.getInt(1);
    }

    public static List<Integer> getAutoIncrementIDs(PreparedStatement pstm) throws SQLException {
        List<Integer> IDs = new ArrayList<>();
        ResultSet rs = pstm.getGeneratedKeys();

        while (rs.next()) {
            IDs.add(rs.getInt(1));
        }

        return IDs;
    }

    public static int getMaxIDOfTable(String dbName, String tableName) throws SQLException {
        ResultSet rs = DataUtl.queryDB(dbName, "SELECT id FROM " + tableName + " ORDER BY id DESC LIMIT 1");
        rs.next();

        int maxIDOfMergedDB = rs.getInt(1);
        System.out.println("Max ID of " + dbName + "." + tableName + ": " + maxIDOfMergedDB);

        return maxIDOfMergedDB;
    }
}
