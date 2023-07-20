/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.naming.ColumnNamingStrategy;
import io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy;
import io.debezium.connector.jdbc.naming.UnicodeColumnNamingStrategy;

/**
 * Tests for the {@link ColumnNamingStrategy} interface and implementations.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class ColumnNamingStrategyTest {
    @Test
    public void testDefaultColumnNamingStrategy() {
        final DefaultColumnNamingStrategy strategy = new DefaultColumnNamingStrategy();
        assertThat(strategy.resolveColumnName("field")).isEqualTo("field");
    }

    @Test
    public void testUnicodeColumnNamingStrategy() {
        final UnicodeColumnNamingStrategy strategy = new UnicodeColumnNamingStrategy();
        assertThat(strategy.resolveColumnName("CAR_u0024BRAND")).isEqualTo("CAR$BRAND");
    }
}
