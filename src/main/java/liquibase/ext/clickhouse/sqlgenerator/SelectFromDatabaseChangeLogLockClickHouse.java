package liquibase.ext.clickhouse.sqlgenerator;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.ext.clickhouse.database.ClickHouseDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SelectFromDatabaseChangeLogLockGenerator;
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import liquibase.util.StringUtil;

public class SelectFromDatabaseChangeLogLockClickHouse extends SelectFromDatabaseChangeLogLockGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(SelectFromDatabaseChangeLogLockStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(SelectFromDatabaseChangeLogLockStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String liquibaseSchema;
        liquibaseSchema = database.getLiquibaseSchemaName();

        // use LEGACY quoting since we're dealing with system objects
        ObjectQuotingStrategy currentStrategy = database.getObjectQuotingStrategy();
        database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);
        try {
            String sql = "SELECT " + StringUtil.join(statement.getColumnsToSelect(), ",", (StringUtil.StringUtilFormatter<ColumnConfig>) col -> {
                if ((col.getComputed() != null) && col.getComputed()) {
                    return col.getName();
                } else {
                    return database.escapeColumnName(null, null, null, col.getName());
                }
            }) + " FROM " +
                    database.escapeTableName(database.getLiquibaseCatalogName(), liquibaseSchema, database.getDatabaseChangeLogLockTableName()) +
                    " FINAL " +
                    " WHERE " + database.escapeColumnName(database.getLiquibaseCatalogName(), liquibaseSchema, database.getDatabaseChangeLogLockTableName(), "ID") + "=1";

            return new Sql[] {
                    new UnparsedSql(sql)
            };
        } finally {
            database.setObjectQuotingStrategy(currentStrategy);
        }

    }
}
