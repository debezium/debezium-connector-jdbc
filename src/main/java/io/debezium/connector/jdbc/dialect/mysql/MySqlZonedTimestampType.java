/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.time.ZonedDateTime;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.type.debezium.ZonedTimestampType;
import io.debezium.time.ZonedTimestamp;

/**
 * A custom MySQL implementation of {@link ZonedTimestampType}.
 *
 * @author Chris Cranford
 */
class MySqlZonedTimestampType extends ZonedTimestampType {

    public static final MySqlZonedTimestampType INSTANCE = new MySqlZonedTimestampType();

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null) {
            query.setParameter(index, null);
        }
        else if (value instanceof String) {
            // Writing the value to MySQL using ZonedDateTime like other dialects seems to lead to
            // inconsistent results; using OffsetDateTime which appears to work consistently. This
            // appears to potentially be either a JDBC driver or Hibernate bug?
            query.setParameter(index, ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER).toOffsetDateTime());
        }
        else {
            throwUnexpectedValue(value);
        }
    }

}
