package data;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;

public class Article {
    private static Gson gson = new Gson();

    private int id;
    private String rawAuthorStr, doi, issn, journal, journal_abbr, language, publisher, title, type, volume, authors_json, uri, reference, keywords, abstracts, number;
    private List<Author> listAuthors;
    private boolean is_isi, is_scopus, is_vci;
    private int year;

    public Article() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getRawAuthorStr() {
        return rawAuthorStr;
    }

    public void setRawAuthorStr(String rawAuthorStr) {
        this.rawAuthorStr = rawAuthorStr;
    }

    public String getDOI() {
        return doi;
    }

    public void setDOI(String doi) {
        this.doi = doi;
    }

    public String getISSN() {
        return issn;
    }

    public void setISSN(String issn) {
        this.issn = issn;
    }

    public String getJournal() {
        return journal;
    }

    public void setJournal(String journal) {
        this.journal = journal;
    }

    public String getJournal_abbr() {
        return journal_abbr;
    }

    public void setJournal_abbr(String journal_abbr) {
        this.journal_abbr = journal_abbr;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = convertArticleType(type);
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setYear(String year) {
        try {
            this.year = Integer.valueOf(year);
        } catch (Exception e) {
            this.year = -1;
        }
    }

    public String getDoi() {
        return doi;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }

    public String getIssn() {
        return issn;
    }

    public void setIssn(String issn) {
        this.issn = issn;
    }

    public String getURI() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public String getAbstract() {
        return abstracts;
    }

    public void setAbstracts(String abstracts) {
        this.abstracts = abstracts;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getAuthors_json() {
        return authors_json;
    }

    public void setAuthors_json(String authors_json) {
        this.authors_json = authors_json;

        if (authors_json != null) {
            this.listAuthors = gson.fromJson(authors_json, new TypeToken<List<Author>>(){}.getType());
        }
    }

    public boolean isDuplicate() {
        return is_isi && is_scopus;
    }

    public void setDuplicate() {
        is_isi = true;
        is_scopus = true;
    }

    public List<Author> getListAuthors() {
        if (listAuthors == null) {
            if (authors_json != null) {
                this.listAuthors = gson.fromJson(authors_json, new TypeToken<List<Author>>(){}.getType());
            } else {
                String[] listAuthorFullNames = null;

                if (is_isi) {
                    listAuthorFullNames = rawAuthorStr.split(" and ");
                }
                if (is_scopus) {
                    listAuthorFullNames = rawAuthorStr.split(", ");
                }

                List<Author> authors = new ArrayList<>(listAuthorFullNames.length);

                for (String authorFullName : listAuthorFullNames) {
                    authors.add(new Author(authorFullName));
                }

                listAuthors = authors;
            }
        }

        return listAuthors;
    }

    public boolean isISI() {
        return is_isi;
    }

    public void setISI(boolean is_isi) {
        this.is_isi = is_isi;
    }

    public boolean isScopus() {
        return is_scopus;
    }

    public void setScopus(boolean is_scopus) {
        this.is_scopus = is_scopus;
    }

    public boolean isVCI() {
        return is_vci;
    }

    public void setVCI(boolean is_vci) {
        this.is_vci = is_vci;
    }

    public static String convertArticleType(String articleType) {
        if (articleType == null) {
            return "journal";
        }

        articleType = articleType.toLowerCase();
        if (articleType.contains("proceeding") || articleType.contains("conference")) {
            return "conference";
        }

        if (articleType.contains("article")) {
            return "journal";
        }

        return "other";
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(id).append(": ").append(title).append('\n');
        builder.append(rawAuthorStr).append('\n');
        builder.append(doi).append(' ').append(year).append(' ').append(journal).append(is_isi ? " ISI" : " Scopus");

        return builder.toString();
    }
}
