package data;

import java.util.ArrayList;

public class Author {
    private String first_name, last_name, fullName;
    private String[] organize;
    private ArrayList<String> listAbbrName;

    public Author() {
    }

    public Author(String fullName) {
        this.fullName = fullName;
    }

    public String[] getOrganizations() {
        return organize;
    }

    public void setOrganizations(String[] organize) {
        this.organize = organize;
    }

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;

        if (this.last_name != null) {
            fullName = first_name + (first_name.charAt(first_name.length() - 1) == ' ' ? "" : " ") + last_name;
        }
    }

    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;

        if (this.first_name != null) {
            fullName = first_name + (first_name.charAt(first_name.length() - 1) == ' ' ? "" : " ") + last_name;
        }
    }

    public String getFullName() {
        if (fullName == null) {
            fullName = (first_name.trim() + " " + last_name.trim()).trim();
        }

        return fullName;
    }

    public ArrayList<String> getListAbbrName() {
        if (listAbbrName == null) {
            if (fullName == null) {
                fullName = (first_name.trim() + " " + last_name.trim()).trim();
            }

            this.listAbbrName = new ArrayList<>();
            this.listAbbrName.add(fullName);
            this.listAbbrName.addAll(Name.generateAbbrNames(fullName));

            if (first_name != null && last_name != null) {
                this.listAbbrName.add(first_name + ", " + last_name);
            }
        }

        return listAbbrName;
    }
}
