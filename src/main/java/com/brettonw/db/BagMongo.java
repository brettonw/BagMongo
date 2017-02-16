package com.brettonw.db;

import com.brettonw.bag.*;
import com.brettonw.bag.formats.MimeType;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.HashMap;
import java.util.Map;

import static com.brettonw.db.Keys.COLLECTION_NAME;
import static com.brettonw.db.Keys.CONNECTION_STRING;
import static com.brettonw.db.Keys.DATABASE_NAME;

public class BagMongo implements BagDbInterface, AutoCloseable {
    private static final Logger log = LogManager.getLogger (BagMongo.class);

    private static final String ID_KEY = "_id";
    private static final String LOCALHOST_DEFAULT = "mongodb://localhost:27017";

    private static final Map<MongoClientURI, MongoClient> MONGO_CLIENTS = new HashMap<> ();

    private String name;
    private MongoCollection<Document> collection;

    private BagMongo (String name, MongoCollection<Document> collection) {
        this.name = name;
        this.collection = collection;
        log.info ("Connected to '" + name + "'");
    }

    public static BagMongo connect (MongoClientURI clientUri, String databaseName, String collectionName) {
        // try to get the client
        MongoClient mongoClient = MONGO_CLIENTS.get (clientUri);
        if (mongoClient == null) {
            mongoClient = new MongoClient (clientUri);
            try {
                mongoClient.getAddress ();
            } catch (Exception exception) {
                log.error ("Failed to connect to '" + clientUri + "'", exception);
                return null;
            }
            MONGO_CLIENTS.put (clientUri, mongoClient);
        }

        // try to get the collection
        MongoDatabase database = mongoClient.getDatabase (databaseName);
        MongoCollection<Document> collection = database.getCollection (collectionName);
        // XXX I am unable to determine a failure case here, so I am choosing to ignore the possibility
        return new BagMongo (databaseName + "/" + collectionName, collection);
    }

    public static BagMongo connect (String connectionString, String databaseName, String collectionName) {
        MongoClientURI mongoClientUri = null;
        try {
            mongoClientUri = new MongoClientURI (connectionString);
        } catch (Exception exception) {
            log.error ("Failed to connect to '" + connectionString + "'", exception);
            return null;
        }

        return connect (mongoClientUri, databaseName, collectionName);
    }

    public static BagMongo connect (String databaseName, String collectionName) {
        return connect (LOCALHOST_DEFAULT, databaseName, collectionName);
    }

    public static BagMongo connect (String collectionName) {
        return connect (collectionName, collectionName);
    }

    public static BagMongo connect (BagObject configuration) {
        String collectionName = configuration.getString (COLLECTION_NAME);
        if (collectionName != null) {
            String connectionString = configuration.has (CONNECTION_STRING) ? configuration.getString (CONNECTION_STRING) : LOCALHOST_DEFAULT;
            String databaseName = configuration.has (DATABASE_NAME) ? configuration.getString (DATABASE_NAME) : collectionName;
            return connect (connectionString, databaseName, collectionName);
        } else {
            log.error ("Invalid configuration (missing '" + COLLECTION_NAME + "')");
            return null;
        }
    }

    public BagDbInterface put (BagObject bagObject) {
        Document document = Document.parse (bagObject.toString (MimeType.JSON));
        collection.insertOne (document);
        return this;
    }

    public BagDbInterface putMany (BagArray bagArray) {
        for (int i = 0, end = bagArray.getCount (); i < end; ++i) {
            put (bagArray.getBagObject (i));
        }
        return this;
    }

    private Bson buildQuery (String queryJson) {
        if (queryJson != null) {
            BagObject queryBagObject = BagObjectFrom.string (queryJson, MimeType.JSON);
            if (queryBagObject != null) {
                int count = queryBagObject.getCount ();
                String[] keys = queryBagObject.keys ();
                if (count > 1) {
                    Bson[] bsons = new Bson[count];
                    for (int i = 0; i < count; ++i) {
                        bsons[i] = Filters.eq (keys[i], queryBagObject.getString (keys[i]));
                    }
                    return Filters.and (bsons);
                } else if (count == 1) {
                    return Filters.eq (keys[0], queryBagObject.getString (keys[0]));
                }
            }
        }
        return new Document ();
    }

    private static BagObject extract (Document document) {
        if (document != null) {
            String json = document.toJson ();
            BagObject bagObject = BagObjectFrom.string (json, MimeType.JSON);

            // Mongo adds "_id" if the posting object doesn't include it. we decide to allow
            // this, but to otherwise mask it from the user as it would lock us into the
            // Mongo API
            bagObject = bagObject.select (new SelectKey (SelectType.EXCLUDE, ID_KEY));
            return bagObject;
        }
        return null;
    }

    public BagObject get (String queryJson) {
        Bson filter = buildQuery (queryJson);
        FindIterable<Document> queryResult = collection.find (filter);
        Document got = queryResult.first ();
        BagObject bagObject = extract (got);
        return bagObject;
    }

    public BagArray getMany (String queryJson) {
        final BagArray bagArray = new BagArray ();
        Bson filter = buildQuery (queryJson);
        collection.find (filter).forEach (
                (Block<Document>) document -> bagArray.add (extract (document))
        );
        return bagArray;
    }

    public BagArray getAll () {
        final BagArray bagArray = new BagArray ();
        collection.find (new Document ()).forEach (
                (Block<Document>) document -> bagArray.add (extract (document))
        );
        return bagArray;
    }

    public BagDbInterface delete (String queryJson) {
        Bson filter = buildQuery (queryJson);
        collection.deleteOne (filter);
        return this;
    }

    public BagDbInterface deleteMany (String queryJson) {
        Bson filter = buildQuery (queryJson);
        collection.deleteMany (filter);
        return this;
    }

    public BagDbInterface deleteAll () {
        collection.deleteMany (new Document ());
        return this;
    }

    public void drop () {
        collection.drop ();
        log.info ("Dropped '" + name + "'" );
    }

    @Override
    public void close () throws Exception {
        // XXX what should happen here?
        log.info ("Closed '" + name + "'");
    }

    public long getCount () {
        return collection.count ();
    }

    public String getName () {
        return name;
    }
}
