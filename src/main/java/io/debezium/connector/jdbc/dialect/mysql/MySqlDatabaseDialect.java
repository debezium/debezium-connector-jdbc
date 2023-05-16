/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.List;

import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.SinkRecordDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Strings;

/**
 * A {@link DatabaseDialect} implementation for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlDatabaseDialect extends GeneralDatabaseDialect {

    private static final List<String> NO_DEFAULT_VALUE_TYPES = Arrays.asList(
            "tinytext", "mediumtext", "longtext", "text", "tinyblob", "mediumblob", "lonblob");

    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME_WITH_SPACE = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();

    public static class MySqlDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof MySQLDialect;
        }

        @Override
        public Class<?> name() {
            return MySqlDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new MySqlDatabaseDialect(config, sessionFactory);
        }
    }

    private MySqlDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();

        registerType(BooleanType.INSTANCE);
        registerType(BitType.INSTANCE);
        registerType(BytesType.INSTANCE);
        registerType(EnumType.INSTANCE);
        registerType(SetType.INSTANCE);
        registerType(MediumIntType.INSTANCE);
        registerType(IntegerType.INSTANCE);
        registerType(TinyIntType.INSTANCE);
        registerType(YearType.INSTANCE);
        registerType(JsonType.INSTANCE);
        registerType(ZonedTimestampWithoutTimezoneType.INSTANCE);
        registerType(MapToJsonType.INSTANCE);
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        return 255;
    }

    @Override
    public String getFormattedTime(ZonedDateTime value) {
        return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(value));
    }

    @Override
    public String getFormattedDateTime(ZonedDateTime value) {
        return String.format("'%s'", ISO_LOCAL_DATE_TIME_WITH_SPACE.format(value));
    }

    @Override
    public String getFormattedTimestamp(ZonedDateTime value) {
        return String.format("'%s'", ISO_LOCAL_DATE_TIME_WITH_SPACE.format(value));
    }

    @Override
    public String getFormattedTimestampWithTimeZone(String value) {
        final ZonedDateTime zonedDateTime = ZonedDateTime.parse(value, ZonedTimestamp.FORMATTER);
        return String.format("'%s'", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(zonedDateTime));
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, SinkRecordDescriptor record) {
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("INSERT INTO ");
        builder.append(table.getId().getTableName());
        builder.append(" (");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") VALUES (");
        builder.appendLists(", ", record.getKeyFieldNames(), record.getNonKeyFieldNames(), (name) -> columnQueryBindingFromField(name, record));
        builder.append(") ");

        final List<String> updateColumnNames = record.getNonKeyFieldNames().isEmpty()
                ? record.getKeyFieldNames()
                : record.getNonKeyFieldNames();

        if (getDatabaseVersion().isSameOrAfter(8, 0, 20)) {
            // MySQL 8.0.20 deprecated the use of "VALUES()" in exchange for table aliases
            builder.append("AS new ON DUPLICATE KEY UPDATE ");
            builder.appendList(",", updateColumnNames, (name) -> {
                final String columnName = columnNameFromField(name, record);
                return columnName + "=new." + columnName;
            });
        }
        else {
            builder.append("ON DUPLICATE KEY UPDATE ");
            builder.appendList(",", updateColumnNames, (name) -> {
                final String columnName = columnNameFromField(name, record);
                return columnName + "=VALUES(" + columnName + ")";
            });
        }

        return builder.build();
    }

    @Override
    protected void addColumnDefaultValue(SinkRecordDescriptor.FieldDescriptor field, StringBuilder columnSpec) {
        final String fieldType = field.getTypeName();
        if (!Strings.isNullOrBlank(fieldType)) {
            if (NO_DEFAULT_VALUE_TYPES.contains(fieldType.toLowerCase())) {
                return;
            }
        }
        super.addColumnDefaultValue(field, columnSpec);
    }

    @Override
    protected String getSchemaName(SessionFactory sessionFactory) {
        final StatelessSession session = sessionFactory.openStatelessSession();
        try {
            return session.doReturningWork((connection) -> {
                try (PreparedStatement ps = connection.prepareStatement("SELECT database()")) {
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getString(1);
                        }
                        return null;
                    }
                }
            });
        }
        finally {
            session.close();
        }
    }

}