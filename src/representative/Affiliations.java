package representative;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Vo Dinh Hieu
 */
public class Affiliations {
    private int numberAffiliation;
    private List<Affiliation> affiliations = new ArrayList<>();

    public Affiliations(String fileName) throws Exception {

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document document = documentBuilder.parse(new File(fileName));

        NodeList affs = document.getElementsByTagName("affiliation");
        numberAffiliation = affs.getLength();

        for (int i = 0; i < numberAffiliation; ++i) {
            affiliations.add(new Affiliation(affs.item(i)));
        }
    }

    public List<Affiliation> getAffiliations() {
        return affiliations;
    }
}