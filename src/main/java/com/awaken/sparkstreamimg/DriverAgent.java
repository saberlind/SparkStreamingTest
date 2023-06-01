package com.awaken.sparkstreamimg;

import com.mysql.jdbc.Driver;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * @author Awaken
 * @create 2023/6/1 21:51
 * @Topic
 */
public class DriverAgent extends Driver implements Serializable {

    public DriverAgent() throws SQLException {
        super();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}
