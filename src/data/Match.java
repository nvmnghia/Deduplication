package data;

public class Match {
    private int ISI, Scopus;

    public Match(int isi_id, int scopus_id) {
        this.ISI = isi_id;
        this.Scopus = scopus_id;
    }

    public int getISI() {
        return ISI;
    }

    public void setISI(int ISI) {
        this.ISI = ISI;
    }

    public int getScopus() {
        return Scopus;
    }

    public void setScopus(int scopus) {
        this.Scopus = scopus;
    }
}
