package liquibase.ext.clickhouse.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.clickhouse.database.ClickHouseDatabase;
import liquibase.ext.clickhouse.params.ClusterConfig;
import liquibase.ext.clickhouse.params.ParamsLoader;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.UpdateGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.UpdateStatement;

import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

import static liquibase.util.SqlUtil.replacePredicatePlaceholders;

public class UpdateGeneratorClickHouse extends UpdateGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(UpdateStatement statement, Database database) {
        return database instanceof ClickHouseDatabase;
    }

    @Override
    public Sql[] generateSql(UpdateStatement statement,
                             Database database,
                             SqlGeneratorChain sqlGeneratorChain) {
        ClusterConfig properties = ParamsLoader.getLiquibaseClickhouseProperties();

        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(generateSource(database, properties));
        sb.append("UPDATE ");
        sb.append(generateColumnValues(statement.getNewColumnValues(), database));
        sb.append(generateWhereClause(statement, database));
        return SqlGeneratorUtil.generateSql(database, sb.toString());
    }

    private String generateSource(Database database, ClusterConfig properties) {
        return "`" + database.getDefaultSchemaName() + "`.`"
                + database.getDatabaseChangeLogTableName() + "` "
                + SqlGeneratorUtil.generateSqlOnClusterClause(properties);
    }

    private String generateColumnValues(Map<String, Object> columnValues, Database database) {
        return columnValues.entrySet().stream()
                .map(entry -> entry.getKey() + " = " + convertToString(entry.getValue(), database))
                .collect(Collectors.joining(", "));
    }

    private String generateWhereClause(UpdateStatement statement, Database database) {
        StringBuilder sb = new StringBuilder();
        if (statement.getWhereClause() != null) {
            sb.append(" WHERE ");
            sb.append(
                    replacePredicatePlaceholders(
                            database,
                            statement.getWhereClause(),
                            statement.getWhereColumnNames(),
                            statement.getWhereParameters()));
        } else {
            sb.append(" WHERE true ");
        }
        sb.append("SETTINGS mutations_sync = 1");
        return sb.toString();
    }

    private String convertToString(Object newValue, Database database) {
        if ((newValue == null) || "NULL".equalsIgnoreCase(newValue.toString())) {
            return "NULL";
        }
        if ((newValue instanceof String)
                && !looksLikeFunctionCall(((String) newValue), database)) {
            return DataTypeFactory.getInstance().fromObject(newValue, database).objectToSql(newValue, database);
        }
        if (newValue instanceof Date) {
            // converting java.util.Date to java.sql.Date
            Date date = (Date) newValue;
            if (date.getClass().equals(Date.class)) {
                date = new java.sql.Date(date.getTime());
            }
            return database.getDateLiteral(date);
        }
        if (newValue instanceof Boolean) {
            if (((Boolean) newValue)) {
                return DataTypeFactory.getInstance().getTrueBooleanValue(database);
            } else {
                return DataTypeFactory.getInstance().getFalseBooleanValue(database);
            }
        }
        if (newValue instanceof DatabaseFunction) {
            return database.generateDatabaseFunctionValue((DatabaseFunction) newValue);
        }
        return newValue.toString();
    }
}
