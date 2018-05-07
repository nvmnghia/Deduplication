package data;

import config.Config;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import util.DataUtl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ArticleSource {
    public static List<Article> getArticles(String index, String field, String value) throws UnknownHostException, SQLException {
        switch (Config.getDataSource()) {
            case ES_TRANSPORT_6_2_3:
                return getArticlesES_Transport(index, field, value);

            case ES_REST:
                return getArticlesES_REST(field, value);

            case DB:
                return getArticlesDB(field, value);

            default:
                return new ArrayList<>();
        }
    }

    public static List<Article> getArticles(String index, String type, String field, String value) throws UnknownHostException, SQLException {
        List<Article> articles = getArticles(index, field, value);

        boolean is_isi = type.equals("isi");
        for (int i = 0; i < articles.size(); ++i) {
            if (articles.get(i).isISI() != is_isi) {
                articles.remove(i--);
            }
        }

        return articles;
    }

    public static Article getArticleByID(String index, String type, int ID) throws SQLException, UnknownHostException {
        List<Article> articles = getArticles(index, Config.FIELD_ID, String.valueOf(ID));

        if (articles == null) {
            System.out.println("real null");

            return null;
        } else {
            boolean is_isi = type.equals("isi");

            for (Article article : articles) {
                if (article.isISI() == is_isi) {
                    return article;
                }
            }

            try {
                Files.write(Paths.get("D:\\VCI\\Deduplication\\src\\almost.txt"), (type + "    " + ID + "\n\n\n\n").getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    private static List<Article> getArticlesES_Transport(String index, String field, String value) throws UnknownHostException {
        List<Article> articles = new ArrayList<>();

        QueryBuilder filterByField = QueryBuilders.matchQuery(field, value);
        SearchHits hits = DataUtl.queryES(index, filterByField);

        if (hits == null || hits.getTotalHits() == 0) {
            return articles;
        }

        for (SearchHit hit : hits) {
            Article article = new Article();

            article.setID((Integer) hit.getSourceAsMap().get("original_id"));
            article.setTitle((String) hit.getSourceAsMap().get("title"));
            article.setYear((String) hit.getSourceAsMap().get("year"));
            article.setRawAuthorStr((String) hit.getSourceAsMap().get("author"));
            article.setISSN((String) hit.getSourceAsMap().get("issn"));
            article.setISI(hit.getSourceAsMap().get("is_isi").equals(Boolean.TRUE));
            article.setScopus(hit.getSourceAsMap().get("is_scopus").equals(Boolean.TRUE));
            article.setJournal((String) hit.getSourceAsMap().get("journal"));
            article.setAuthors_json((String) hit.getSourceAsMap().get("authors_json"));
            article.setDOI((String) hit.getSourceAsMap().get("doi"));
            article.setVolume((String) hit.getSourceAsMap().get("volume"));
            article.setNumber((String) hit.getSourceAsMap().get("number"));
            article.setAbstracts((String) hit.getSourceAsMap().get("abstract"));
            article.setReference((String) hit.getSourceAsMap().get("reference"));
            article.setKeywords((String) hit.getSourceAsMap().get("keyword"));

            if (! index.equals(Config.ES_INDEX)) {
                article.setJournalID((Integer) hit.getSourceAsMap().get("journal_id"));
                article.setMergedID(article.getID());
            }

            articles.add(article);
        }

        return articles;
    }

    private static List<Article> getArticlesES_REST(String field, String value) {
        return null;
    }

    private static List<Article> getArticlesDB(String field, String value) throws SQLException {
        List<Article> articles = new ArrayList<>();

        String query;
        if (field.equalsIgnoreCase("id")) {
            query = "SELECT ar.id, ar.title, ar.doi, ar.year, ar.is_isi, ar.is_scopus, GROUP_CONCAT(au.name SEPARATOR ', '), j.issn, j.name FROM articles ar " +
                    "JOIN articles_authors aa ON aa.article_id = ar.id " +
                    "JOIN authors au ON aa.author_id = au.id " +
                    "JOIN journals j ON j.id = ar.journal_id " +
                    "WHERE ar.reference IS NOT NULL AND length(ar.reference) >= 1 AND ar.id = " + value + " " +
                    "GROUP BY ar.id";
        } else {
            query = "SELECT ar.id, ar.title, ar.doi, ar.year, ar.is_isi, ar.is_scopus, GROUP_CONCAT(au.name SEPARATOR ', '), j.issn, j.name FROM articles ar " +
                    "JOIN articles_authors aa ON aa.article_id = ar.id " +
                    "JOIN authors au ON aa.author_id = au.id " +
                    "JOIN journals j ON j.id = ar.journal_id " +
                    "WHERE ar.reference IS NOT NULL AND length(ar.reference) >= 1 AND MATCH (title) AGAINST ('" + value + "' IN NATURAL LANGUAGE MODE) " +
                    "GROUP BY ar.id";
        }

        ResultSet resultSet = DataUtl.queryDB(Config.DB_NAME, query);

        while (resultSet.next()) {
            Article article = new Article();

            article.setID(resultSet.getInt(1));
            article.setTitle(resultSet.getString(2));
            article.setDOI(resultSet.getString(3));
            article.setYear(resultSet.getString(4));
            article.setISI(resultSet.getString(5) != null && resultSet.getString(5).length() != 0 && resultSet.getInt(5) == 1);
            article.setScopus(resultSet.getString(6) != null && resultSet.getString(6).length() != 0 && resultSet.getInt(6) == 1);
            article.setRawAuthorStr(resultSet.getString(7));
            article.setISSN(resultSet.getString(8));
            article.setJournal(resultSet.getString(9));

            articles.add(article);
        }

        return articles;
    }
}
