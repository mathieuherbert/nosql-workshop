package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.*;
import java.util.List;

public class ActivitesImporter {

    private final DBCollection installationsCollection;

    public ActivitesImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
    }

    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/activites.csv");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> updateEquipement(line));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void updateEquipement(final String line) {
        String[] columns = line
                .substring(1, line.length() - 1)
                .split("\",\"");

        // Programmation défensive : certaines lignes n'ont pas d'activités de définies
        if (columns.length >= 6) {
            String equipementId = columns[2].trim();

            // TODO codez la mise à jour de l'installation pour rattacher les activités à ses équipements


            System.out.println();
            BasicDBObject searchQuery = new BasicDBObject().append("equipements.numero", equipementId);
            BasicDBObject updateQuery = new BasicDBObject();
            updateQuery.append("$push",
                    new BasicDBObject().append("equipements.$.activites", columns[5]));

            installationsCollection.update(searchQuery, updateQuery);




            // System.out.println(installation);

        }
    }
}
