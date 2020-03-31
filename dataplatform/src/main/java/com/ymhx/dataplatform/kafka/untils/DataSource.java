package com.ymhx.dataplatform.kafka.untils;

import java.sql.Connection;

public interface DataSource {
    public Connection getConnection();
}
