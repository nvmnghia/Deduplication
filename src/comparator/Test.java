package comparator;

import config.Config;
import data.Article;
import data.ArticleSource;
import importer.ImportDB;
import util.DataUtl;

import java.net.UnknownHostException;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) throws UnknownHostException, SQLException {
        Article scopus = ArticleSource.getArticleByID(Config.ES.INDEX, Article.SCOPUS, 4);
        System.out.println(scopus.getTitle());
    }
}
