/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;

/**
 * A lower-case implementation of the {@link DefaultTableNamingStrategy} where the computed table name
 * will also be returned in all lower-case.
 *
 * @author Chris Cranford
 */
public class LowercaseTableNamingStrategy extends DefaultTableNamingStrategy {
    @Override
    public String resolveTableName(JdbcSinkConnectorConfig config, SinkRecord record) {
        return super.resolveTableName(config, record).toLowerCase();
    }
}
