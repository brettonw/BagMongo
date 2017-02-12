package com.brettonw.db;

import com.brettonw.bag.BagObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BagMongo_Test {
    @Test
    public void test1 () {
        BagMongo bagMongo = new BagMongo ("test");
        bagMongo.put (new BagObject ().put ("id", 1).put ("key", "value 1").put ("payload", "full"));
        BagObject testCase = new BagObject ().put ("id", 2).put ("key", "value 2").put ("payload", "medium");
        bagMongo.put (testCase);
        bagMongo.put (new BagObject ().put ("id", 3).put ("key", "value 3").put ("payload", "empty"));
        BagObject result = bagMongo.get (new BagObject ().put ("id", 1));
        assertEquals (testCase, result);
    }
}
