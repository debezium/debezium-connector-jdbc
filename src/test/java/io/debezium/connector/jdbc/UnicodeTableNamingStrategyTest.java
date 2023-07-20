/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.connector.jdbc.naming.UnicodeTableNamingStrategy;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;

@Tag("UnitTests")
public class UnicodeTableNamingStrategyTest extends TableNamingStrategyTest {

    @Override
    protected JdbcSinkConnectorConfig createJdbcSinkConnectorConfig(Map<String, String> props) {
        Map<String, String> newProps = new HashMap<>(props);
        newProps.put(JdbcSinkConnectorConfig.TABLE_NAMING_STRATEGY, UnicodeTableNamingStrategy.class.getName());
        return new JdbcSinkConnectorConfig(newProps);
    }

    @Test
    public void testTableNamingStrategyWithDebeziumUnicode() {
        final JdbcSinkConnectorConfig config = createJdbcSinkConnectorConfig(Map.of("table.name.format", "kafka_${topic}"));
        final SinkRecordFactory factory = new DebeziumSinkRecordFactory();
        final TableNamingStrategy strategy = config.getTableNamingStrategy();
        SinkRecord sinkRecord = factory.tombstoneRecord("database.schema.table_u0024name");
        assertThat(strategy.resolveTableName(config, sinkRecord)).isEqualTo("kafka_database_schema_table$name");
    }
}
