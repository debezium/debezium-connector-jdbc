/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.paramount;

import org.hibernate.dialect.DatabaseVersion;

public class SnowflakeDialect extends org.hibernate.dialect.Dialect {

    @Override
    public DatabaseVersion getVersion() {
        return new DatabaseVersion() {
            @Override
            public int getDatabaseMajorVersion() {
                return 0;
            }

            @Override
            public int getDatabaseMinorVersion() {
                return 0;
            }
        };
    }
}
