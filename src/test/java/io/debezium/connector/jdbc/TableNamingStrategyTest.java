/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.naming.DefaultTableNamingStrategy;
import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Tests for the {@link TableNamingStrategy} interface and implementations.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class TableNamingStrategyTest {

    protected JdbcSinkConnectorConfig createJdbcSinkConnectorConfig(Map<String, String> props) {
        Map<String, String> newProps = new HashMap<>(props);
        newProps.put(JdbcSinkConnectorConfig.TABLE_NAMING_STRATEGY, DefaultTableNamingStrategy.class.getName());
        return new JdbcSinkConnectorConfig(newProps);
    }

    @Test
    public void testDefaultTableNamingStrategy() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of());
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        assertThat(strategy.resolveTableName(config, factory.createRecord("database.schema.table"))).isEqualTo("database_schema_table");
    }

    @Test
    public void testTableNamingStrategyWithTableNameFormat() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of("table.name.format", "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        assertThat(strategy.resolveTableName(config, factory.createRecord("database.schema.table"))).isEqualTo("kafka_database_schema_table");
    }

    @Test
    @FixFor("DBZ-6491")
    public void testTableNamingStrategyWithPrependedSchema() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of("table.name.format", "SYS.${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        assertThat(strategy.resolveTableName(config, factory.createRecord("database.schema.table"))).isEqualTo("SYS.database_schema_table");
    }

    @Test
    public void testDefaultTableNamingStrategyWithDebeziumSource() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of("table.name.format", "source_${source.db}_${source.schema}_${source.table}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        SinkRecord sinkRecord = factory.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1");
        assertThat(strategy.resolveTableName(config, sinkRecord)).isEqualTo("source_database1_schema1_table1");
    }

    @Test
    public void testDefaultTableNamingStrategyWithInvalidSourceField() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of("table.name.format", "source_${source.invalid}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        SinkRecord sinkRecord = factory.createRecord("database.schema.table", (byte) 1, "database1", "schema1", "table1");
        Assertions.assertThrows(DataException.class, () -> strategy.resolveTableName(config, sinkRecord));
    }

    @Test
    public void testDefaultTableNamingStrategyWithDebeziumSourceAndTombstone() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of("table.name.format", "source_${source.db}_${source.schema}_${source.table}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        SinkRecord sinkRecord = factory.tombstoneRecord("database.schema.table");
        assertThat(strategy.resolveTableName(config, sinkRecord)).isNull();
    }

    @Test
    public void testDefaultTableNamingStrategyWithTopicAndTombstone() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of("table.name.format", "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        SinkRecord sinkRecord = factory.tombstoneRecord("database.schema.table");
        assertThat(strategy.resolveTableName(config, sinkRecord)).isEqualTo("kafka_database_schema_table");
    }

    @Test
    public void testTableNamingStrategyWithDebeziumUnicode() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of("table.name.format", "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        SinkRecord sinkRecord = factory.tombstoneRecord("database.schema.table_u0024name");
        assertThat(strategy.resolveTableName(config, sinkRecord)).isEqualTo("kafka_database_schema_table_u0024name");
    }
}
