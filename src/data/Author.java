package data;

import com.google.gson.annotations.SerializedName;
import importer.ImportDB;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Author {
    private String first_name, last_name, fullName;
    @SerializedName(value = "organize", alternate = {"organizes", "organization", "organizations"})
    private String[] organize;
    private String[] splittedOrganizations = null;
    private ArrayList<String> listAbbrName;

    public Author() {
    }

    public Author(String fullName) {
        this.fullName = fullName;
    }


    public String[] getOrganizations() {
        if (splittedOrganizations == null) {
            List<String> tempSplittedOrganizations = new ArrayList<>(Arrays.asList(organize));

            for (int i = 0; i < tempSplittedOrganizations.size(); ++i) {
                String organization = tempSplittedOrganizations.get(i);
                List<String> splitted = ImportDB.splitLumpedOrganizations(organization);

                if (splitted.size() > 1) {
                    tempSplittedOrganizations.remove(i--);
                    tempSplittedOrganizations.addAll(splitted);
                }
            }

            splittedOrganizations = new String[tempSplittedOrganizations.size()];
            splittedOrganizations = tempSplittedOrganizations.toArray(splittedOrganizations);

            if ((splittedOrganizations.length == 0 && organize.length != 0) || splittedOrganizations.length < organize.length) {
                System.out.println("FUCK, LESS???");

                for (String str : organize) {
                    System.out.println(str);
                }

                System.out.println("IS MORE THAN");

                for (String str : splittedOrganizations) {
                    System.out.println(str);
                }
            }
        }

        return splittedOrganizations;
    }

    /**
     * Not recommended. Use getOrganizations instead.
     * This function only serves the Article.getRawOrganizations function.
     * @return
     */
    public String[] getRawOrganizations() {
        return organize;
    }

    public void setOrganize(String[] organize) {
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
