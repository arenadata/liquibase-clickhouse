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

        StringBuilder sb = new StringBuilder(
                String.format(
                        "ALTER TABLE `%s`.`%s` " + SqlGeneratorUtil.generateSqlOnClusterClause(properties),
                        database.getDefaultSchemaName(),
                        database.getDatabaseChangeLogTableName()
                )
        );

        sb.append("UPDATE ");
        for (String column : statement.getNewColumnValues().keySet()) {
            sb.append(" ")
                    .append(column)
                    .append(" = ")
                    .append(convertToString(statement.getNewColumnValues().get(column), database))
                    .append(",");
        }
        int lastComma = sb.lastIndexOf(",");
        if (lastComma >= 0) {
            sb.deleteCharAt(lastComma);
        }
        if (statement.getWhereClause() != null) {
            sb.append(" WHERE ")
                    .append(
                            replacePredicatePlaceholders(
                                    database,
                                    statement.getWhereClause(),
                                    statement.getWhereColumnNames(),
                                    statement.getWhereParameters()));
        } else {
            sb.append(" WHERE true ");
        }
        sb.append("SETTINGS mutations_sync = 1");

        return SqlGeneratorUtil.generateSql(database, sb.toString());
    }

    private String convertToString(Object newValue, Database database) {
        String sqlString;
        if ((newValue == null) || "NULL".equalsIgnoreCase(newValue.toString())) {
            sqlString = "NULL";
        } else if ((newValue instanceof String)
                && !looksLikeFunctionCall(((String) newValue), database)) {
            sqlString = DataTypeFactory.getInstance().fromObject(newValue, database).objectToSql(newValue, database);
        } else if (newValue instanceof Date) {
            // converting java.util.Date to java.sql.Date
            Date date = (Date) newValue;
            if (date.getClass().equals(Date.class)) {
                date = new java.sql.Date(date.getTime());
            }
            sqlString = database.getDateLiteral(date);
        } else if (newValue instanceof Boolean) {
            if (((Boolean) newValue)) {
                sqlString = DataTypeFactory.getInstance().getTrueBooleanValue(database);
            } else {
                sqlString = DataTypeFactory.getInstance().getFalseBooleanValue(database);
            }
        } else if (newValue instanceof DatabaseFunction) {
            sqlString = database.generateDatabaseFunctionValue((DatabaseFunction) newValue);
        } else {
            sqlString = newValue.toString();
        }
        return sqlString;
    }
}
