/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import org.hibernate.query.NativeQuery;

public class NativeQueryBinder implements QueryBinder {

    private final NativeQuery<?> binder;

    public NativeQueryBinder(NativeQuery<?> binder) {
        this.binder = binder;
    }

    @Override
    public void bind(int index, Object value) {

        binder.setParameter(index, value);
    }
}
