package nosql.workshop.services;

import com.google.inject.Inject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import org.bson.types.ObjectId;
import org.jongo.Aggregate;
import org.jongo.FindOne;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Service permettant de manipuler les installations sportives.
 */
public class InstallationService {

    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    /**
     * Retourne une installation étant donné son numéro.
     *
     * @param numero le numéro de l'installation.
     * @return l'installation correspondante, ou <code>null</code> si non trouvée.
     */
    public Installation get(String numero) {
        // TODO codez le service

        Installation installation = installations.findOne("{\"_id\":\"" + numero + "\"}").as(Installation.class);

        return installation;
    }

    /**
     * Retourne la liste des installations.
     *
     * @param page     la page à retourner.
     * @param pageSize le nombre d'installations par page.
     * @return la liste des installations.
     */
    public List<Installation> list(int page, int pageSize) {
        // TODO codez le service
        System.out.println("ici!!!!!!!!!!!!!");
        List<Installation> installationList = new ArrayList<>();
        Iterator<Installation> it  = installations.find().skip((page - 1) * pageSize).limit(pageSize).as(Installation.class).iterator();
        while (it.hasNext()){
            installationList.add(it.next());
        }
       return installationList;
    }

    /**
     * Retourne une installation aléatoirement.
     *
     * @return une installation.
     */
    public Installation random() {
        long count = count();
        int random = new Random().nextInt((int) count);
        // TODO codez le service
        return  installations.find().skip(random).limit(1).as(Installation.class).next();


    }

    /**
     * Retourne le nombre total d'installations.
     *
     * @return le nombre total d'installations
     */
    public long count() {
        return installations.count();
    }

    /**
     * Retourne l'installation avec le plus d'équipements.
     *
     * @return l'installation avec le plus d'équipements.
     */
    public Installation installationWithMaxEquipments() {
        System.out.println("withMaxEquipments");
        return  installations.aggregate("{$project: {nbEquipements : { $size: \"$equipements\"},nom: 1,equipements: 1}}")
        .and("{$sort:{\"nbEquipements\" : -1}}")
        .and("{$limit : 1}")
        .as(Installation.class).iterator().next();
    }

    /**
     * Compte le nombre d'installations par activité.
     *
     * @return le nombre d'installations par activité.
     */
    public List<CountByActivity> countByActivity() {
        System.out.println("countbyactivity");
        Iterator<CountByActivity> it  = installations.aggregate(" {$unwind : \"$equipements\" }")
                .and(" {$unwind :\"$equipements.activites\" }").and(" {$group: {_id:\"$equipements.activites\", total: {$sum:1} }}")
        .and("\t{ $project : { _id : 0, activite : \"$_id\" , total : 1 } }\n"
                ).and(" {$sort : {total : -1}}").as(CountByActivity.class).iterator();

        List<CountByActivity> countByActivities = new ArrayList<>();

        while (it.hasNext()){
            countByActivities.add(it.next());
        }
        System.out.println("countByActivities");
        System.out.println(countByActivities.size());
        System.out.println(countByActivities.get(0).getActivite());
        System.out.println(countByActivities.get(0).getTotal());
        //throw new UnsupportedOperationException();
        return countByActivities;
    }

    public double averageEquipmentsPerInstallation() {
        System.out.println("averageEquipmentsPerInstallation");

        double ret = (double)(installations.aggregate(" {$unwind : \"$equipements\"} ").as(Object.class).size())/(double )count() ;
        System.out.println(ret);
        return ret;
    }

    /**
     * Recherche des installations sportives.
     *
     * @param searchQuery la requête de recherche.
     * @return les résultats correspondant à la requête.
     */
    public List<Installation> search(String searchQuery) {
        System.out.println("Create Index Weight");
        BasicDBObject index = new BasicDBObject();
        index.put("nom", "text");
        index.put("adresse", "text");
        BasicDBObject weights = new BasicDBObject("nom", 3).append("adresse", 10);
        BasicDBObject options = new BasicDBObject("weights", weights).append("default_language", "french");
        installations.getDBCollection().createIndex(index, options);


        Iterator<Installation> it = installations.find("{\n" +
                "        \"$text\": {\n" +
                "            \"$search\": \""+searchQuery+"\",\n" +
                "            \"$language\" : \"french\"\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"score\": {\"$meta\": \"textScore\"}\n" +
                "    }").as(Installation.class).iterator();
        List<Installation> installationList = new ArrayList<>();
        System.out.println("retour geo");
        while (it.hasNext()){
            installationList.add(it.next());
        }
        return installationList;
    }

    /**
     * Recherche des installations sportives par proximité géographique.
     *
     * @param lat      latitude du point de départ.
     * @param lng      longitude du point de départ.
     * @param distance rayon de recherche.
     * @return les installations dans la zone géographique demandée.
     */
    public List<Installation> geosearch(double lat, double lng, double distance) {

        DBObject index2d = BasicDBObjectBuilder.start("location", "2dsphere").get();
         installations.getDBCollection().createIndex(index2d);

        MongoCursor<Installation> it = installations.find("{ \"location\" : \n" +
                "    { $near :\n" +
                "        { $geometry :\n" +
                "            { type : \"Point\" ,\n" +
                "              coordinates : [ " + lng + ", " + lat + " ]\n" +
                "            },\n" +
                "            $maxDistance : " + distance + "\n" +
                "        }\n" +
                "    }\n" +
                "}").as(Installation.class);


        List<Installation> installationList = new ArrayList<>();
        System.out.println("retour geo");
        while (it.hasNext()){
            installationList.add(it.next());
        }
        return installationList;

    }
}
