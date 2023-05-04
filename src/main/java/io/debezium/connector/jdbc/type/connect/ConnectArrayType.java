/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.util.ArrayList;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} that supports {@code BYTES} connect schema types.
 *
 * @author Chris Cranford
 */
public class ConnectArrayType extends AbstractConnectSchemaType {

    public static final ConnectArrayType INSTANCE = new ConnectArrayType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "ARRAY" };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "ARRAY";
    }

    @Override
    public String getQueryBinding(Schema schema) {
        return "STRING_TO_ARRAY(?, ',')";
    }

    @Override
    public void bind(Query<?> query, int index, Schema schema, Object value) {
        if (value == null || value instanceof String) {
            query.setParameter(index, value);
        }
        else if (value instanceof ArrayList) {
            query.setParameter(index, String.join(",", (ArrayList) value));
        }
        else {
            throw new RuntimeException("Unknown type " + value.getClass());
        }

    }
}
