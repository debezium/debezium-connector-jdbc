/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

/**
 * The upper-case implementation of the {@link ColumnNamingStrategy} that simply returns the field's
 * name as the column name in the destination table but in all upper-case.
 *
 * @author Chris Cranford
 */
public class UppercaseColumnNamingStrategy implements ColumnNamingStrategy {
    @Override
    public String resolveColumnName(String fieldName) {
        return fieldName.toUpperCase();
    }
}
