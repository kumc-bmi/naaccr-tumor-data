import groovy.json.JsonSlurper
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import tech.tablesaw.api.*
import tech.tablesaw.columns.Column
import tech.tablesaw.io.csv.CsvReadOptions

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.ResultSet
import java.time.LocalDate
import java.util.logging.Logger
import java.util.zip.ZipFile

@CompileStatic
class TumorOnt {
    static Logger log = Logger.getLogger("")

    static class OncologyMeta {
        static Tuple morph3_info = new Tuple('ICD-O-2_CSV.zip', 'icd-o-3-morph.csv', ['code', 'label', 'notes'])
        static Tuple topo_info = new Tuple('ICD-O-2_CSV.zip', 'Topoenglish.txt', null)
        static String encoding = 'ISO-8859-1'

        static Table read_table(Path cache, Tuple info) {
            String zip = info[0] as String; String item = info[1] as String
            List<String> names = info[2] as List<String>
            def archive = new ZipFile(cache.resolve(zip).toFile())
            def infp = new InputStreamReader(archive.getInputStream(archive.getEntry(item)), Charset.forName(encoding))
            CsvReadOptions options = CsvReadOptions.builder(infp)
                    .separator((item.endsWith(".csv") ? ',' : '\t') as char)
                    .header(names == null)
                    .build()
            Table data = Table.read().csv(options)
            if (names) {
                listZip(names, data.columns()) { String name, Column<?> col -> col.setName(name) }
            }
            data
        }
    }

    static List listZip(List a, List b, Closure f) {
        def result = []
        0.upto(Math.min(a.size(), b.size()) - 1) { ix -> result << f(a[ix], b[ix]) }
        result
    }

    static class SqlScript {
        final String name
        final String code
        final List<Tuple3<String, String, List<String>>> objects

        SqlScript(String _name, String _code, List<Tuple2<String, List<String>>> object_info) {
            name = _name
            code = _code
            objects = object_info.collect { new Tuple3(it.first, find_ddl(it.first, code), it.second) }
        }

        static String find_ddl(String name, String script) {
            String comment_pattern = '((--[^\\n]*(?:\\n|$))|(?:/\\*(?:[^*]|(\\*(?!/)))*\\*/))*'
            Scanner stmts = new Scanner(script).useDelimiter(';\n\\s*' + comment_pattern)
            while (stmts.hasNext()) {
                String stmt = stmts.next().trim()
                if (stmt.startsWith('create ')) {
                    String firstLine = stmt.split('\n')[0]
                    if (firstLine.split().contains(name)) {
                        return stmt
                    }
                }
            }
            throw new IllegalArgumentException(name)
        }


        static String sql_list(List items) {
            String.join(', ', items)
        }

        static String create_ddl(String name, List<Column<?>> schema, int varchars = 1024) {
            final sqlName = { ColumnType it ->
                switch (it) {
                    case ColumnType.STRING:
                        return "VARCHAR($varchars)"
                    case ColumnType.LOCAL_DATE:
                        return "DATE"
                    default:
                        return it.toString()
                }
            }
            final coldefs = schema.collect { Column it -> "${it.name()} ${sqlName(it.type())}" }
            "create table $name (${sql_list(coldefs)})"
        }

        static String insert_dml(String name, List<Column<?>> schema) {
            final colnames = schema.collect { col -> col.name() }
            "insert into ${name} (${sql_list(colnames)}) values (${sql_list(schema.collect { '?' })})"
        }
    }

    static class NAACCR_I2B2 {
        static final String top_folder = "\\i2b2\\naaccr\\"
        static final String c_name = 'Cancer Cases (NAACCR Hierarchy)'
        static final String sourcesystem_cd = 'heron-admin@kumc.edu'

        static final tumor_item_type = read_csv(TumorOnt.getResource('heron_load/tumor_item_type.csv'))
        static final seer_recode_terms = read_csv(TumorOnt.getResource('heron_load/seer_recode_terms.csv'))

        static final cs_terms = read_csv(TumorOnt.getResource('heron_load/cs-terms.csv'))
                .removeColumns('update_date', 'sourcesystem_cd')

        /* obsolete
        static final tx_script = new SqlScript(
                'naaccr_txform.sql',
                resourceText(TumorOnt.getResource('heron_load/naaccr_txform.sql')), [])
        */

        static final String per_item_view = 'tumor_item_type'
        static final per_section = read_csv(TumorOnt.getResource('heron_load/section.csv'))

        static SqlScript ont_script = new SqlScript(
                'naaccr_concepts_load.sql',
                resourceText(NAACCR_I2B2.getResource('heron_load/naaccr_concepts_load.sql')),
                [
                        new Tuple2('i2b2_path_concept', []),
                        new Tuple2('naaccr_top_concept', ['naaccr_top', 'current_task']),
                        new Tuple2('section_concepts', ['section', 'naaccr_top']),
                        new Tuple2('item_concepts', [per_item_view]),
                        new Tuple2('code_concepts', [per_item_view, 'loinc_naaccr_answer', 'code_labels']),
                        new Tuple2('primary_site_concepts', ['icd_o_topo']),
                        // TODO: morphology
                        new Tuple2('seer_recode_concepts', ['seer_site_terms', 'naaccr_top']),
                        new Tuple2('site_schema_concepts', ['cs_terms']),
                        new Tuple2('naaccr_ontology', []),
                ])

        static def ont_view_in(Sql sql, String task_hash, LocalDate update_date, Path who_cache) {
            // TODO: make who_cache optional
            def who_topo = OncologyMeta.read_table(who_cache, OncologyMeta.topo_info)
            // TODO def icd_o_topo = OncologyMeta.read_table(who_cache, info.zip, info.items, info.names)
            Table top = fromRecords([[c_hlevel       : 1,
                                      c_fullname     : top_folder,
                                      c_name         : c_name,
                                      update_date    : update_date,
                                      sourcesystem_cd: sourcesystem_cd] as Map])
            final current_task = fromRecords([[task_hash: task_hash] as Map]).setName("current_task")
            final tables = [
                    current_task   : current_task,
                    naaccr_top     : top,
                    section        : per_section,
                    tumor_item_type: tumor_item_type,
                    // TODO loinc_naaccr_answer: LOINC_NAACCR.answer,
                    // TODO code_labels: NAACCR_R.code_labels(),
                    // TODO icd_o_topo: icd_o_topo,
                    cs_terms       : cs_terms,
                    seer_site_terms: seer_recode_terms,
            ] as Map
            sql.execute(TumorOnt.SqlScript.find_ddl("zpad", ont_script.code))
            final views = create_objects(sql, ont_script, tables)
            final String name = ont_script.objects.last().first
            views[name]
        }
    }

    static Map<String, Table> create_objects(Sql sql, SqlScript script,
                                             Map<String, Table> tables) {
        // IDEA: use a contextmanager for temp views
        final Set provided = tables.keySet()
        tables.each { key, df ->
            log.info("${script.name}: $key = ${df.columnNames()}")
            load_data_frame(sql, key, df)
        }
        script.objects.collectEntries {
            final name = it.first
            final Set missing = it.third.toSet() - provided
            if (!missing.isEmpty()) {
                throw new IllegalArgumentException(missing.toString())
            }
            log.info("${script.name}: create $name")
            sql.execute("drop view if exists $name".toString())
            sql.execute(it.second)
            def dft = null
            sql.query("select * from $name".toString()) { ResultSet results ->
                dft = Table.read().db(results, name)
            }
            [(name): dft]
        }
    }

    static void load_data_frame(Sql sql, String name, Table data) {
        sql.execute(SqlScript.create_ddl(name, data.columns()))
        sql.withBatch(SqlScript.insert_dml(name, data.columns())) { BatchingPreparedStatementWrapper ps ->
            for (Row row : data) {
                final params = []
                for (ColumnType type : data.columnTypes()) {
                    switch (type) {
                        case ColumnType.INTEGER:
                            params << row.getInt(params.size())
                            break
                        case ColumnType.STRING:
                            params << row.getString(params.size())
                            break
                        case ColumnType.LOCAL_DATE:
                            params << java.sql.Date.valueOf(row.getDate((params.size())))
                            break
                        default:
                            throw new IllegalArgumentException(type.toString())
                    }
                }
                ps.addBatch(params)
            }
        }
    }

    static Table read_csv(URL url, ColumnType[] schema = null) {
        if (schema == null) {
            schema = tabularTypes(new JsonSlurper().parse(meta_path(url)))
        }
        Table.read().usingOptions(CsvReadOptions.builder(url.openStream()).columnTypes(schema))
    }

    static ColumnType[] tabularTypes(Object meta) {
        (((meta as Map)?.tableSchema as Map)?.columns as List).collect {
            String name = (it as Map)?.datatype as String
            switch (name) {
                case "number":
                    return ColumnType.INTEGER
                case "string":
                    return ColumnType.STRING
                case "date":
                    return ColumnType.LOCAL_DATE
                default:
                    throw new IllegalAccessException(name)
            }
        } as ColumnType[]
    }

    static String resourceText(URL url) {
        new String(Files.readAllBytes(Paths.get(url.toURI())))
    }

    static URL meta_path(URL path) {
        final parent = path.toURI().resolve('./')
        final String[] segments = path.path.split('/')
        final stem = segments.last().replaceFirst('[.][^.]+$', "")
        parent.resolve(stem + '-metadata.json').toURL()
    }

    static Table fromRecords(List<Map<String, Object>> obj) {
        Collection<Column<?>> cols = ((obj[0].collect { k, v ->
            switch (v) {
                case String:
                    return StringColumn.create(k, v as String)
                case Integer:
                    return IntColumn.create(k, v as Integer)
                case LocalDate:
                    return DateColumn.create(k, v as LocalDate)
                default:
                    throw new IllegalArgumentException("Expected String or Int in 1st record, not:" + v)
            }
        }) as Collection<Column<?>>)
        Table data = Table.create("t1", cols)
        obj.tail().each { Map<String, Object> m ->
            Row row = data.appendRow()
            for (it in m) {
                switch (it.value) {
                    case String:
                        row.setString(it.key, it.value as String)
                        break
                    case Integer:
                        row.setInt(it.key, it.value as Integer)
                        break
                    case LocalDate:
                        row.setDate(it.key, it.value as LocalDate)
                        break
                    case null:
                        row.setMissing(it.key)
                        break
                    default:
                        throw new IllegalArgumentException("not supported" + it.value)
                }
            }
        }
        data
    }
}