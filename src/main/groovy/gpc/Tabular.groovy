package gpc

import groovy.json.JsonSlurper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Immutable

import java.sql.Connection
import java.sql.Types
import java.time.LocalDateTime

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

    static void importCSV(Sql sql, String table_name, URL data, Map metadata) {
        final schema = columnDescriptions(metadata)
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

    static List<Map<String, Object>> allCSVRecords(URL source) {
        final meta = columnDescriptions(metadata(source))
        source.withInputStream { text -> allCSVRecords(text, meta) }
    }

    static List<Map<String, Object>> allCSVRecords(InputStream text, List<ColumnMeta> schema) {
        final out = []
        eachCSVRecord(text, schema) { out << it; return }
        out
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
                        case Types.BOOLEAN:
                            value = Boolean.parseBoolean(lit)
                            break
                        case Types.TIMESTAMP:
                            value = LocalDateTime.parse(lit.replace(' ', 'T'))
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

    static void eachCSVRow(InputStream text, boolean tsv = false, Closure<Void> f) {
        final sep = Character.codePointAt(tsv ? '\t' : ',', 0)
        String state = 'start'
        String cell = ''
        List<String> row = []
        final emit = {
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
                if (ch == sep) {
                    row << cell
                    cell = ''
                } else if (ch as char == '"' as char) {
                    state = 'q1'
                } else if (ch as char == '\r' as char) {
                    state = 'CR'
                } else if (ch as char == '\n' as char) {
                    emit()
                } else {
                    cell += Character.toChars(ch)
                }
            } else if (state == 'q1') {
                if (ch as char == '"' as char) {
                    cell += '"'
                    state = 'start'
                } else {
                    cell += ch as char
                    state = 'quoted'
                }
            } else if (state == 'quoted') {
                if (ch as char == '"' as char) {
                    state = 'qq1'
                } else {
                    cell += Character.toChars(ch)
                }
            } else if (state == 'qq1') {
                if (ch as char == '"' as char) {
                    cell += '"'
                    state = 'quoted'
                } else {
                    state = 'start'
                    continue
                }
            } else if (state == 'CR') {
                emit()
                if (ch as char != '\n' as char) {
                    continue
                }
            }
            ch = text.read()
        } while (ch != -1)
    }

    static void writeCSV(Writer out, List<String> hd, Closure eachRecord) {
        final write1 = { List<String> row ->
            String sep = ''
            row.each { out.print(sep); out.print(it.replace('"', '""')); sep = ',' }
            out.print('\r\n')
        }

        boolean first = true
        eachRecord { Map<String, Object> record ->
            if (first) {
                if (!hd) {
                    hd = record.collect { it.key }
                }
                write1(hd)
                first = false
            }
            write1(hd.collect {
                final v = record.getOrDefault(it, null)
                v == null ? '' : v.toString()
            })
        }
    }

    static List<ColumnMeta> columnDescriptions(URL meta) {
        columnDescriptions(metadata(meta))
    }

    static List<ColumnMeta> columnDescriptions(Object meta) {
        final columns = (((meta as Map)?.tableSchema as Map)?.columns as List<Map>)
        assert columns != null
        columns.collect { Map it ->
            Integer dataType
            switch (it.datatype) {
                case "boolean":
                    dataType = Types.BOOLEAN
                    break;
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

    static Map metadata(URL path) {
        final meta = new URL(meta_path(path.toString()))
        new JsonSlurper().parse(meta) as Map
    }

    static String meta_path(String path) {
        path.replace('.csv', '-metadata.json')
    }
}
