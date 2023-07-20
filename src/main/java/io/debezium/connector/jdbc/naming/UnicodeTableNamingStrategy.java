/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.util.Strings;

/**
 * The unicode extend of the {@link DefaultTableNamingStrategy} where the table name is driven
 * directly from the topic name, replacing any {@code dot} characters with {@code underscore}
 * and source field in topic, and revert any {@code _uxxxx} unicode characters back.
 *
 * @author Harvey Yue
 */

public class UnicodeTableNamingStrategy extends DefaultTableNamingStrategy {

    @Override
    public String resolveTableName(JdbcSinkConnectorConfig config, SinkRecord record) {
        String table = super.resolveTableName(config, record);
        if (table != null) {
            table = Strings.revertUnicode(table);
        }
        return table;
    }
}
