package nosql.workshop.batch.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

/**
 * Importe les 'installations' dans MongoDB.
 */
public class InstallationsImporter {

    private final DBCollection installationsCollection;

    public InstallationsImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
    }

    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/installations.csv");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> installationsCollection.save(toDbObject(line)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DBObject toDbObject(final String line) {
        String[] columns = line
                .substring(1, line.length() - 1)
                .split("\",\"");
        BasicDBObject basicDBObject = new BasicDBObject();
        // TODO créez le document à partir de la ligne CSV

        basicDBObject.put("_id",columns[1] );
        basicDBObject.put("nom",columns[0] );

        BasicDBObject adressDBObject = new BasicDBObject();
        adressDBObject.put("commune", columns[2]);
        adressDBObject.put("codePostal", columns[4]);
        adressDBObject.put("lieuDit", columns[5]);
        adressDBObject.put("numero", columns[6]);
        adressDBObject.put("voie", columns[7]);

        basicDBObject.put("adresse",adressDBObject );

        BasicDBObject locationDBObject = new BasicDBObject();
        adressDBObject.put("type", "Point");
        double[] locations = new double[2];
        locations[0] = Double.parseDouble(columns[9]);
        locations[1] = Double.parseDouble(columns[10]);
        adressDBObject.put("coordinates", locations);
        basicDBObject.put("location",locationDBObject);


        if(columns[16].equals("Non")){
            basicDBObject.put("multiCommune",false);
        }else {
            basicDBObject.put("multiCommune",true);
        }

        basicDBObject.put("nbPlacesParking",columns[17].isEmpty()?0:Integer.parseInt(columns[17]));
        basicDBObject.put("nbPlacesParkingHandicapes",columns[18].isEmpty()?0:Integer.parseInt(columns[18]));
        if(columns.length >= 29) {
            try {
                LocalDate localDate = LocalDate.parse(columns[28].split(" ")[0]);
                basicDBObject.put("dateMiseAJourFiche", Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));
            } catch (DateTimeParseException ex) {
                System.out.println(columns[28]);
            }
        }


        return basicDBObject;
    }
}
