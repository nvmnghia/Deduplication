package data;

import config.Config;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import util.DataUtl;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ArticleSource {
    /**
     * Do the same things as the above function, but filter out the type
     * @param index
     * @param type
     * @param field
     * @param value
     * @return
     * @throws UnknownHostException
     * @throws SQLException
     */
    public static List<Article> getArticles(String index, int type, String field, String value) throws UnknownHostException {
        List<Article> articles = getArticles(index, field, value);

        if (type == Article.SCOPUS) {
            for (int i = 0; i < articles.size(); ++i) {
                if (! articles.get(i).isScopus()) {
                    articles.remove(i--);
                }
            }
        } else {
            for (int i = 0; i < articles.size(); ++i) {
                if (! articles.get(i).isISI()) {
                    articles.remove(i--);
                }
            }
        }

        return articles;
    }

    public static Article getArticleByID(String index, int type, int ID) throws UnknownHostException {
        List<Article> articles = getArticles(index, "original_id", String.valueOf(ID));

        if (articles == null) {
            return null;
        } else {
            if (type == Article.SCOPUS) {
                for (Article article : articles) {
                    if (article.isScopus() && article.getID() == ID) {
                        return article;
                    }
                }
            } else {
                for (Article article : articles) {
                    if (article.isISI() && article.getID() == ID) {
                        return article;
                    }
                }
            }

            return null;
        }
    }

    private static List<Article> getArticles(String index, String field, String value) throws UnknownHostException {
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
            article.setAuthorsJSON((String) hit.getSourceAsMap().get("authors_json"));
            article.setDOI((String) hit.getSourceAsMap().get("doi"));
            article.setVolume((String) hit.getSourceAsMap().get("volume"));
            article.setNumber((String) hit.getSourceAsMap().get("number"));
            article.setAbstracts((String) hit.getSourceAsMap().get("abstract"));
            article.setReference((String) hit.getSourceAsMap().get("reference"));
            article.setKeywords((String) hit.getSourceAsMap().get("keyword"));

            if (! index.equals(Config.ES.INDEX)) {
                article.setJournalID((Integer) hit.getSourceAsMap().get("journal_id"));
                article.setMergedID(article.getID());
            }

            articles.add(article);
        }

        return articles;
    }
}
