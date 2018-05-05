package deduplicator;

import com.google.gson.Gson;
import data.Article;
import data.ArticleSource;
import data.Organization;
import importer.ImportDB;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.sql.SQLException;

public class RunnerISI {
    // Only for ISI articles
    public static void main(String[] args) throws IOException, SQLException {
        String organization = "Cơ quan hành pháp quốc gia";

        Gson gson = new Gson();

        CloseableHttpClient httpClient = HttpClients.createDefault();

        HttpPost httppost = new HttpPost("http://localhost:8000/api/organizes/createByName");
        httppost.setEntity(new StringEntity("{\n" +
                "\t\"nameOrganize\": \"" + organization + "\"\n" +
                "}"));

        HttpResponse response = httpClient.execute(httppost);
        String entity = EntityUtils.toString(response.getEntity(), "UTF-8");
        Organization org = gson.fromJson(entity, Organization.class);
        System.out.println(entity);
    }
}
