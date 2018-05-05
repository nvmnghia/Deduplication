package util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import config.Config;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.UnsupportedEncodingException;
import java.net.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataUtl {

    private static JsonParser parser = new JsonParser();
    private static Random random = new Random();

    public static JsonArray queryES(String field, String value) throws URISyntaxException, UnsupportedEncodingException {
        field = URLEncoder.encode(field, "UTF-8");
        value = URLEncoder.encode(value, "UTF-8");

        HttpPost query = new HttpPost();
        query.setURI(new URI( Config.ES_SEARCH_URI + field + ":" + value));
        System.out.println(Config.ES_SEARCH_URI + field + ":" + value);

        try (
                CloseableHttpClient client = HttpClientBuilder.create().build();
                CloseableHttpResponse response = client.execute(query);
        ) {

            String result = EntityUtils.toString(response.getEntity(), "UTF-8");
            JsonObject rootObject = parser.parse(result).getAsJsonObject();

            // Make sure the poor server won't be banged to death
            Thread.sleep(random.nextInt(55));

            return rootObject.getAsJsonObject("hits").getAsJsonArray("hits");

        } catch (Exception e) {
            return null;
        }
    }

    private static Client client = null;

    public static Client getESClient() throws UnknownHostException {
        if (client == null) {
            TransportAddress address = new TransportAddress(InetAddress.getByName("localhost"), 9300);
            Settings settings = Settings.builder()
                    .put("cluster.name", Config.ES_CLUSTER_NAME)
                    .build();

            client = new PreBuiltTransportClient(settings).addTransportAddress(address);
        }

        return client;
    }

    public static SearchHits queryES(String index, QueryBuilder builder) throws UnknownHostException {
        return getESClient().prepareSearch(index).setQuery(builder).get().getHits();
    }

    public static void indexES(String index, String type, String id, XContentBuilder builder) throws UnknownHostException {
        getESClient().prepareIndex(index, type, id).setSource(builder).get();
        getESClient().admin().indices().prepareRefresh(index).get();

        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Connection connection = null;

    public static Connection getDBConnection() throws SQLException {
        if (connection == null) {
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setUser("root");
            dataSource.setPassword("nvmnghia");

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

    public static int insertAndGetID(String dbName, String query) throws SQLException {
        Statement stm = getDBStatement();

        stm.executeQuery("USE " + dbName);
        return stm.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
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
}
