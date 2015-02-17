package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class EquipementsImporter {

    private final DBCollection installationsCollection;

    public EquipementsImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
    }

    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/equipements.csv");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> updateInstallation(line));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void updateInstallation(final String line) {
        String[] columns = line.split(",");

        String installationId = columns[2];



        DBObject newEquipement = new BasicDBObject();
        newEquipement.put("numero", columns[4]);
        newEquipement.put("nom", columns[5]);
        newEquipement.put("type", columns[7]);
        newEquipement.put("famille", columns[9]);

        BasicDBObject searchQuery = new BasicDBObject().append("_id", installationId);
        BasicDBObject updateQuery = new BasicDBObject();
        updateQuery.append("$push",
                new BasicDBObject().append("equipements", newEquipement));
        installationsCollection.update(searchQuery, updateQuery);

        // TODO codez la mise à jour de l'installation pour ajouter ses équipements
    }
}
