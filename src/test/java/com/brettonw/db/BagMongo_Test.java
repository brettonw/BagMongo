package com.brettonw.db;

import com.brettonw.bag.BagArray;
import com.brettonw.bag.BagObject;
import com.brettonw.bag.formats.MimeType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BagMongo_Test {
    private static final String TEST_COLLECTION_NAME = "Test";
    private BagArray testBagArray;
    private String queryJson;
    private String queryManyJson;

    public BagMongo_Test () {
        testBagArray = new BagArray ()
                .add (new BagObject ()
                        .put ("id", 1)
                        .put ("key", "value 1" )
                        .put ("payload", "full" )
                )
                .add (new BagObject ()
                        .put ("id", 2)
                        .put ("key", "value 2" )
                        .put ("payload", "medium" )
                )
                .add (new BagObject ()
                        .put ("id", 3)
                        .put ("key", "value 3" )
                        .put ("payload", "medium" )
                )
                .add (new BagObject ()
                        .put ("id", 4)
                        .put ("key", "value 4" )
                        .put ("payload", "empty" )
                );

        queryJson = new BagObject ().put ("id", 2).toString (MimeType.JSON);
        queryManyJson = new BagObject ().put ("payload", "medium" ).toString (MimeType.JSON);

        // ensure the database is fresh
        close (open ());
    }

    private BagDbInterface open () {
        return new BagMongo (TEST_COLLECTION_NAME);
    }

    private void close (BagDbInterface bagDb) {
        bagDb.deleteAll ();
        assertEquals (0, bagDb.getCount ());
        bagDb.drop ();
    }

    // happy path...
    @Test
    public void test1 () {
        BagDbInterface bagDb = open ()
                .put (testBagArray.getBagObject (0))
                .put (testBagArray.getBagObject (1))
                .put (testBagArray.getBagObject (2));
        assertEquals (3, bagDb.getCount ());

        BagObject result = bagDb.get (queryJson);
        assertEquals (testBagArray.getBagObject (1), result);

        close (bagDb);
    }

    @Test
    public void test2 () {
        BagDbInterface bagDb = open ().putMany (testBagArray);
        assertEquals (testBagArray.getCount (), bagDb.getCount ());

        BagObject result = bagDb.get (queryJson);
        assertEquals (testBagArray.getBagObject (1), result);

        close (bagDb);
    }

    @Test
    public void test3 () {
        BagDbInterface bagDb = open ().putMany (testBagArray);
        assertEquals (testBagArray.getCount (), bagDb.getCount ());

        bagDb.delete (queryJson);
        BagObject result = bagDb.get (queryJson);
        assertEquals (null, result);
        assertEquals (testBagArray.getCount () - 1, bagDb.getCount ());

        close (bagDb);
    }

    @Test
    public void test4 () {
        BagDbInterface bagDb = open ().putMany (testBagArray);
        assertEquals (testBagArray.getCount (), bagDb.getCount ());

        BagArray bagArray = bagDb.getAll ();
        assertNotEquals (null, bagArray);
        assertEquals (testBagArray.getCount (), bagArray.getCount ());
        assertEquals (testBagArray, bagArray);

        close (bagDb);
    }

    @Test
    public void test5 () {
        BagDbInterface bagDb = open ().putMany (testBagArray);
        assertEquals (testBagArray.getCount (), bagDb.getCount ());

        BagArray bagArray = bagDb.getMany (queryManyJson);
        assertNotEquals (null, bagArray);
        assertEquals (2, bagArray.getCount ());
        assertEquals (testBagArray.getBagObject (1), bagArray.getBagObject (0));
        assertEquals (testBagArray.getBagObject (2), bagArray.getBagObject (1));

        close (bagDb);
    }

    @Test
    public void test6 () {
        BagDbInterface bagDb = open ().putMany (testBagArray);
        assertEquals (testBagArray.getCount (), bagDb.getCount ());

        bagDb.deleteMany (queryManyJson);
        assertEquals (2, bagDb.getCount ());

        BagArray bagArray = bagDb.getMany (queryManyJson);
        assertNotEquals (null, bagArray);
        assertEquals (0, bagArray.getCount ());

        close (bagDb);
    }

    // sad path...

}
