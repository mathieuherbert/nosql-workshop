package nosql.workshop.services;

import com.google.inject.Inject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.CountByActivity;
import org.bson.types.ObjectId;
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

        // TODO codez le service
        throw new UnsupportedOperationException();
    }

    /**
     * Compte le nombre d'installations par activité.
     *
     * @return le nombre d'installations par activité.
     */
    public List<CountByActivity> countByActivity() {
        // TODO codez le service
        throw new UnsupportedOperationException();
    }

    public double averageEquipmentsPerInstallation() {
        // TODO codez le service
        throw new UnsupportedOperationException();
    }

    /**
     * Recherche des installations sportives.
     *
     * @param searchQuery la requête de recherche.
     * @return les résultats correspondant à la requête.
     */
    public List<Installation> search(String searchQuery) {
        Iterator<Installation> it = installations.find("{nom:\""+searchQuery+"\"}").as(Installation.class).iterator();
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

        installations.ensureIndex("location.coordinates","2dsphere");

        MongoCursor<Installation> it =  installations.find("{location.coordinates : {$near: ["+lat+", "+lng+"], $maxDistance: " + distance + "}}").as(Installation.class);

        // TODO codez le service
        List<Installation> installationList = new ArrayList<>();
        System.out.println("retour geo");
        while (it.hasNext()){
            installationList.add(it.next());
        }
        return installationList;

    }
}
