package gpc

import groovy.json.JsonSlurper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Immutable
import tech.tablesaw.api.ColumnType
import tech.tablesaw.api.Table

import java.sql.Connection
import java.sql.Types

@CompileStatic
class Tabular {

    @Immutable
    static class ColumnMeta {
        final String name
        final Integer dataType = Types.VARCHAR
        final Integer size = null
        final Boolean nullable = true
        final List<String> nulls = [""]
        final Integer number

        String ddl(Map<Integer, String> toName) {
            final sizePart = size != null ? "(${size})" : ""
            final nullPart = !nullable ? " not null" : ""
            "${name} ${toName[dataType]}${sizePart} ${nullPart}"
        }

        static String createStatement(String table_name, List<ColumnMeta> cols, Map<Integer, String> toName) {
            """
             create table ${table_name} (
                ${cols.collect { it.ddl(toName) }.join(",\n  ")}
            )
            """.trim()
        }

        static String insertStatement(String table_name, List<ColumnMeta> cols) {
            """
            insert into ${table_name} (
            ${cols.collect { it.name }.join(",\n  ")})
            values (${cols.collect { it.param() }.join(",\n  ")})
            """.trim()
        }

        String param() {
            "?.${name}"
        }

        static Map<Integer, String> typeNames(Connection connection) {
            // https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getTypeInfo()
            final dbTypes = connection.metaData.getTypeInfo()
            Map<Integer, String> toName = [:]
            while (dbTypes.next()) {
                // println([DATA_TYPE    : dbTypes.getInt('DATA_TYPE'),
                //          TYPE_NAME    : dbTypes.getString('TYPE_NAME'),
                //          CREATE_PARAMS: dbTypes.getString('CREATE_PARAMS')])
                final ty = dbTypes.getInt('DATA_TYPE')
                if (!toName.containsKey(ty)) {
                    toName[ty] = dbTypes.getString('TYPE_NAME')
                }
            }
            if (toName[Types.BOOLEAN] == null) {
                toName[Types.BOOLEAN] = toName[Types.INTEGER]
            }
            toName
        }
    }

    static void importCSV(Sql sql, String table_name, URL data, URL meta) {
        final schema = columnDescriptions(new JsonSlurper().parse(meta))
        final stmt = ColumnMeta.createStatement(table_name, schema, ColumnMeta.typeNames(sql.connection))
        sql.execute(stmt)
        sql.withBatch(256, ColumnMeta.insertStatement(table_name, schema)) { ps ->
            data.withInputStream { text ->
                eachCSVRecord(text, schema) { record ->
                    ps.addBatch(record)
                }
            }
        }
    }

    static void eachCSVRecord(InputStream text, List<ColumnMeta> schema, Closure<Void> f) {
        List<String> hd = null
        final byName = schema.collectEntries { [it.name, it] }
        eachCSVRow(text) { List<String> row ->
            if (hd == null) {
                hd = row
            } else {
                List<List<String>> pairs = GroovyCollections.transpose([hd, row])
                Map<String, Object> record = pairs.collectEntries { it ->
                    final pair = it as List<String>
                    final col = byName[pair[0]] as ColumnMeta
                    String lit = pair[1]
                    if (col.nulls.contains(lit)) {
                        return [col.name, null]
                    }
                    Object value
                    switch (col.dataType) {
                        case Types.VARCHAR:
                            value = lit
                            break
                        case Types.DOUBLE:
                            value = Double.parseDouble(lit)
                            break
                        case Types.INTEGER:
                            value = Integer.parseInt(lit)
                            break
                        default:
                            throw new IllegalAccessException(col.dataType.toString())
                    }
                    [col.name, value]
                } as Map<String, Object>
                f(record)
            }
            return
        }
    }

    static void eachCSVRow(InputStream text, Closure<Void> f) {
        String state = 'start'
        String cell = ''
        List<String> row = []
        final go = {
            row << cell
            f(row)
            row = []
            cell = ''
            state = 'start'
        }
        int ch = text.read()
        if (ch == -1) {
            return
        }
        do {
            if (state == 'start') {
                if (ch == ',') {
                    row << cell
                    cell = ''
                } else if (ch == '"') {
                    state = 'q1'
                } else if (ch == '\r') {
                    state = 'CR'
                } else if (ch == '\n') {
                    go()
                } else {
                    cell += Character.toChars(ch)
                }
            } else if (state == 'q1') {
                if (ch == '"') {
                    cell += '"'
                    state = 'start'
                } else {
                    cell += Character.toChars(ch)
                    state = 'quoted'
                }
            } else if (state == 'quoted') {
                if (ch == '"') {
                    state = 'qq1'
                } else {
                    cell += Character.toChars(ch)
                }
            } else if (state == 'qq1') {
                if (ch == '"') {
                    cell += '"'
                    state = 'quoted'
                } else {
                    state = 'start'
                    continue
                }
            } else if (state == 'CR') {
                if (ch == '\n') {
                    state = 'start'
                } else {
                    state = 'start'
                    continue
                }
            }
        } while ((ch = text.read()) != -1)
    }

    static List<ColumnMeta> columnDescriptions(Object meta) {
        final columns = (((meta as Map)?.tableSchema as Map)?.columns as List<Map>)
        assert columns != null
        columns.collect { Map it ->
            Integer dataType
            switch (it.datatype) {
                case "number":
                case "integer":
                    dataType = Types.INTEGER
                    break
                case "double":
                    dataType = Types.DOUBLE
                    break
                case "string":
                    dataType = Types.VARCHAR
                    break
                case "date":
                    dataType = Types.DATE
                    break
                case "datetime":
                    dataType = Types.TIMESTAMP
                    break
                default:
                    throw new IllegalAccessException(it.datatype as String)
            }
            new ColumnMeta(
                    name: it.name,
                    dataType: dataType,
                    nulls: it.null as List<String>,
                    number: it.number as Integer,
            )
        }
    }

    static URL meta_path(URL path) {
        new URL(path.toString().replace('.csv', '-metadata.json'))
    }
}
