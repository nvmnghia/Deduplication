package deduplicator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import config.Config;
import data.Article;
import data.ArticleSource;
import data.Match;
import importer.ImportDB;
import util.Sluginator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;

import static data.ArticleSource.getArticleByID;

public class Runner {
    public static void main(String[] args) throws IOException, SQLException {
//        importAllScopus();

        List<Match> listMatches = new ArrayList<>();

        for (int i = 500; i < 41000; ++i) {
            try {
                addToListMatches(listMatches, Deduplicator.deduplicate("isi", i));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Deduplicator.finishHim();

        Sluginator.slugifyAll();

        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        File output = new File("D:\\VCI\\Deduplication\\src\\output.txt");

        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        writer.write(gson.toJson(listMatches));
        writer.close();
    }

    public static void importAllScopus() throws SQLException, IOException {
        for (int i = 11136; i < 44000; ++i) {
            Article scopus = ArticleSource.getArticleByID(Config.ES_INDEX, "scopus", i);
            if (scopus != null) {
                ImportDB.createArticle(scopus);
            }
        }
    }

    public static void addToListMatches(List<Match> currentMatches, List<Match> newMatches) {
        for (int i = 0; i < newMatches.size(); ++i) {
            Match newMatch = newMatches.get(i);

            for (Match currentMatch : currentMatches) {
                if (currentMatch.getISI() == newMatch.getISI() && currentMatch.getScopus() == newMatch.getScopus()) {
                    newMatches.remove(i--);
                    break;
                }
            }
        }

        currentMatches.addAll(newMatches);
    }

    public static void removeDuplicateMatch(List<Match> listMatches) {
        HashSet<String> setMatchStr = new HashSet<>();

        for (int i = 0; i < listMatches.size(); ++i) {
            Match match = listMatches.get(i);
            String matchStr = match.getISI() + "-" + match.getScopus();

            if (setMatchStr.contains(matchStr)) {
                listMatches.remove(i--);
            } else {
                setMatchStr.add(matchStr);
            }
        }
    }

    public static class Worker implements Callable<List<Match>> {

        private String type;
        private int start, end;

        public Worker(String type, int start, int end) {
            this.type = type;
            this.start = start;
            this.end = end;
        }

        @Override
        public List<Match> call() throws Exception {
            List<Match> listMatches = new ArrayList<>();

            for (int i = start; i < end; ++i) {
                try {
                    listMatches.addAll(Deduplicator.deduplicate(type, i));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            return listMatches;
        }
    }
}
