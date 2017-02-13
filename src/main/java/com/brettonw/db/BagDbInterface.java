package com.brettonw.db;

import com.brettonw.bag.BagArray;
import com.brettonw.bag.BagObject;

public interface BagDbInterface {
    BagDbInterface put (BagObject bagObject);
    BagDbInterface putMany (BagArray bagArray);

    BagObject get (String queryJson);
    BagArray getMany (String queryJson);
    BagArray getAll ();

    BagDbInterface delete (String queryJson);
    BagDbInterface deleteMany (String queryJson);
    BagDbInterface deleteAll ();

    void drop ();

    long getCount ();
    public String getName ();
}
