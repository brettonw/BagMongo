package com.brettonw.db;

import com.brettonw.bag.BagArray;
import com.brettonw.bag.BagObject;

public interface BagDbInterface extends AutoCloseable {
    /**
     *
     * @param bagObject
     * @return
     */
    BagDbInterface put (BagObject bagObject);

    /**
     *
     * @param bagArray
     * @return
     */
    BagDbInterface putMany (BagArray bagArray);

    /**
     *
     * @param queryJson
     * @return
     */
    BagObject get (String queryJson);

    /**
     *
     * @param queryJson
     * @return
     */
    BagArray getMany (String queryJson);

    /**
     *
     * @return
     */
    BagArray getAll ();

    /**
     *
     * @param queryJson
     * @return
     */
    BagDbInterface delete (String queryJson);

    /**
     *
     * @param queryJson
     * @return
     */
    BagDbInterface deleteMany (String queryJson);

    /**
     *
     * @return
     */
    BagDbInterface deleteAll ();

    /**
     *
     */
    void drop ();

    /**
     *
     * @return
     */
    long getCount ();

    /**
     *
     * @return
     */
    String getName ();
}
