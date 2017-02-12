package com.brettonw.db;

import com.brettonw.bag.BagArray;
import com.brettonw.bag.BagObject;
import com.brettonw.bag.BagObjectFrom;
import com.brettonw.bag.formats.MimeType;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.HashMap;
import java.util.Map;

public class BagMongo {
    private static Map<MongoClientURI, MongoClient> mongoClients = new HashMap<> ();

    private MongoCollection<Document> collection;

    public BagMongo (MongoClientURI clientUri, String collectionName) {
        MongoClient mongoClient = mongoClients.get (clientUri);
        if (mongoClient == null) {
            mongoClient = new MongoClient(clientUri);
            mongoClients.put (clientUri, mongoClient);
        }

        MongoDatabase database = mongoClient.getDatabase("BagMongo");
        collection = database.getCollection("test");
    }

    public BagMongo (String connectionString, String collectionName) {
        this (new MongoClientURI (connectionString), collectionName);
    }

    public BagMongo (String collectionName) {
        this ("mongodb://localhost:27017", collectionName);
    }

    public BagMongo put (BagObject bagObject) {
        Document document = Document.parse (bagObject.toString (MimeType.JSON));
        collection.insertOne (document);
        return this;
    }

    public BagMongo putMany (BagArray bagArray) {
        for (int i = 0, end = bagArray.getCount (); i < end; ++i) {
            put (bagArray.getBagObject (i));
        }
        return this;
    }

    private Bson buildQuery (BagObject query) {
        if (query != null) {
            int count = query.getCount ();
            String[] keys = query.keys ();
            if (count > 1) {
                Bson[] bsons = new Bson[count];
                for (int i = 0; i < count; ++i) {
                    bsons[i] = Filters.eq (keys[i], query.getString (keys[i]));
                }
                return Filters.and (bsons);
            } else if (count == 1) {
                return Filters.eq (keys[0], query.getString (keys[0]));
            }
        }
        return new Document ();
    }

    private MongoIterable<Document> find (BagObject query) {
        Bson queryBson = buildQuery (query);
        return collection.find (queryBson);
    }

    public BagObject get (BagObject query) {
        Document document = find (query).first ();
        return BagObjectFrom.string (document.toJson ());
    }

    public BagObject get (String query) {
        return get (BagObjectFrom.string (query));
    }

    public BagArray getMany (BagObject query) {
        BagArray bagArray = new BagArray ();
        find (query).forEach ((Block<Document>) document -> bagArray.add (BagObjectFrom.string (document.toJson ())));
        return bagArray;
    }

    public BagArray getMany (String query) {
        return getMany (BagObjectFrom.string (query));
    }

}
