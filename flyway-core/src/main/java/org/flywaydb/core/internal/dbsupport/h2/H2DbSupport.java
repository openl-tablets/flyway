/*
 * Copyright 2010-2017 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.dbsupport.h2;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.FlywaySqlException;
import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.SqlStatementBuilder;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * H2 database specific support
 */
public class H2DbSupport extends DbSupport {
    private final boolean requiresV2Metadata;

    /**
     * Creates a new instance.
     *
     * @param connection The connection to use.
     */
    public H2DbSupport(Connection connection) {
        super(new JdbcTemplate(connection, Types.VARCHAR));
        String version;
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            version = metaData.getDatabaseMajorVersion() + "." + metaData.getDriverMinorVersion();
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to determine the major version of the database", e);
        }
        requiresV2Metadata = MigrationVersion.fromVersion(version).compareTo(MigrationVersion.fromVersion("2.0.0")) >= 0;
    }

    public String getDbName() {
        return "h2";
    }

    public String getCurrentUserFunction() {
        return "USER()";
    }

    protected String doGetCurrentSchemaName() throws SQLException {
        return jdbcTemplate.queryForString("CALL SCHEMA()");
    }

    @Override
    protected void doChangeCurrentSchemaTo(String schema) throws SQLException {
        jdbcTemplate.execute("SET SCHEMA " + quote(schema));
    }

    public boolean supportsDdlTransactions() {
        return false;
    }

    public String getBooleanTrue() {
        return "1";
    }

    public String getBooleanFalse() {
        return "0";
    }

    public SqlStatementBuilder createSqlStatementBuilder() {
        return new H2SqlStatementBuilder();
    }

    @Override
    public String doQuote(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Schema getSchema(String name) {
        return new H2Schema(jdbcTemplate, this, name, requiresV2Metadata);
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }
}