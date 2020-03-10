import groovy.json.JsonSlurper
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.docopt.Docopt
import tech.tablesaw.api.*
import tech.tablesaw.columns.Column
import tech.tablesaw.io.csv.CsvReadOptions

import java.nio.charset.Charset
import java.nio.file.*
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.util.logging.Logger
import java.util.zip.ZipFile

@CompileStatic
class TumorOnt {
    static Logger log = Logger.getLogger("")

    static void main(String[] args) {
        String usage = "Usage: [--table-name=T | --version=NNN | --task-hash=H | --update-date=YYYY-MM-DD | --who-cache=DIR]*"
        DBConfig.CLI cli = new DBConfig.CLI(new Docopt(usage).parse(args),
                { String name ->
                    Properties ps = new Properties(); new File(name).withInputStream { ps.load(it) }; ps
                },
                { int it -> System.exit(it) },
                { String url, Properties ps -> DriverManager.getConnection(url, ps) })

        DBConfig cdw = cli.account()

        Ontology1 work = new Ontology1(
                cli.arg("--table-name", "NAACCR_ONTOLOGY"),
                Integer.parseInt(cli.arg("--version", "18")),
                cli.arg("--task-hash", "task123"),
                // TODO: current calendar date
                LocalDate.parse(cli.arg("--update-date", "2001-01-01")),
                cdw, Paths.get(cli.arg("--who-cache", ",cache")))
        if (!work.complete()) {
            work.run()
        }
    }

    static class Ontology1 {
        final String table_name
        final int version
        final String task_hash
        final LocalDate update_date
        final Path who_cache
        private final DBConfig cdw

        Ontology1(String _table_name, int _version, String _task_hash, LocalDate _update_date,
                  DBConfig _cdw, Path _who_cache) {
            table_name = _table_name
            version = _version
            task_hash = _task_hash
            update_date = _update_date
            who_cache = _who_cache
            cdw = _cdw
        }

        boolean complete() {
            try {
                cdw.withSql { Sql sql ->

                    return sql.firstRow("""
                        select 1 from ${table_name}
                            where c_fullname = ?.fullname
                            and c_basecode = ?.code
                        """, [fullname: NAACCR_I2B2.top_folder, code: version + task_hash])[0] == 1
                }
            } catch (Exception problem) {
                log.warning("not complete: $problem")
            }
            return false
        }

        void run() {
            final DBConfig mem = DBConfig.inMemoryDB("Ontology")
            Table terms
            mem.withSql { Sql sql ->
                terms = NAACCR_I2B2.ont_view_in(sql, task_hash, update_date, who_cache)
            }
            cdw.withSql { Sql sql ->
                try {
                    sql.execute("drop table $table_name".toString())
                } catch (SQLException problem) {
                    log.warning("drop $table_name: $problem")
                }
                load_data_frame(sql, table_name, terms)
            }
        }
    }

    static class NAACCR_R {
        // Names assumed by naaccr_txform.sql

        static final Table field_info = read_csv(TumorOnt.getResource('naaccr_r_raw/field_info.csv'))
        static final Table field_info_schema = Table.create(
                StringColumn.create("code"),
                StringColumn.create("label"),
                BooleanColumn.create("means_missing"),
                StringColumn.create("description"))
        static final Table field_code_scheme = read_csv(TumorOnt.getResource('naaccr_r_raw/field_code_scheme.csv'))

        static final URL _code_labels = TumorOnt.getResource('naaccr_r_raw/code-labels/')

        static Table code_labels(List<String> implicit = ['iso_country']) {
            Table all_schemes = null
            for (String scheme : field_code_scheme.stringColumn('scheme').unique()) {
                if (implicit.contains(scheme)) {
                    continue
                }
                final info = _code_labels.toURI().resolve(scheme + '.csv').toURL()
                int skiprows = 0
                info.withReader() { Reader it ->
                    if (it.readLine().startsWith('#')) {
                        skiprows = 1
                    }
                    null
                }
                final codes = read_csv(info, field_info_schema.columnTypes(), skiprows)
                final constS = { String name, Table t, String val -> StringColumn.create(name, [val] * t.rowCount() as String[]) }
                codes.addColumns(constS('scheme', codes, Pathlib.stem(info)))
                if (!codes.columnNames().contains('code') || !codes.columnNames().contains('label')) {
                    throw new IllegalArgumentException([info, codes.columnNames()].toString())
                }
                if (all_schemes == null) {
                    all_schemes = codes
                } else {
                    all_schemes = all_schemes.append(codes)
                }
            }
            final Table with_fields = all_schemes.joinOn('scheme')
                    .inner(field_code_scheme, 'scheme')
            final Table with_field_info = with_fields.joinOn('name')
                    .inner(field_info.select('item', 'name'), 'name')
            with_field_info
        }
    }

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

        static Table icd_o_topo(Table topo) {
            final major = topo.where(topo.stringColumn('Lvl').isEqualTo('3'))
            def minor = topo.where(topo.stringColumn('Lvl').isEqualTo('4'))
            minor = minor.addColumns(
                    minor.stringColumn('Kode').replaceAll('\\..*', '').setName('major'))
            final constN = { String name, Table t, int val -> IntColumn.create(name, [val] * t.rowCount() as int[]) }
            final constS = { String name, Table t, String val -> StringColumn.create(name, [val] * t.rowCount() as String[]) }
            final out3 = Table.create(
                    constN('lvl', major, 3),
                    major.stringColumn('Kode').copy().setName('concept_cd'),
                    constS('c_visualattributes', major, 'FA'),
                    major.stringColumn('Kode').map({ String it -> it + '\\' }).setName('path'),
                    major.stringColumn('Title').copy().setName('concept_name'))
            final out4 = Table.create(
                    constN('lvl', minor, 4),
                    minor.stringColumn('Kode').map({ String v -> v.replace('.', '') }).setName('concept_cd'),
                    constS('c_visualattributes', minor, 'LA'),
                    StringColumn.create('path',
                            minor.iterator().collect({ Row it -> it.getString('major') + '\\' + it.getString('Kode') + '\\' })),
                    minor.stringColumn('Title').copy().setName('concept_name'),
            )
            out3.append(out4)
        }
    }

    static List listZip(List a, List b, Closure f) {
        def result = []
        0.upto(Math.min(a.size(), b.size()) - 1) { ix -> result << f(a[ix], b[ix]) }
        result
    }

    static class SqlScript { // TODO: refactor: move SqlScript out of TumorOnt
        final String name
        final String code
        final List<Tuple3<String, String, List<String>>> objects

        SqlScript(String _name, String _code, List<Tuple2<String, List<String>>> object_info) {
            name = _name
            code = _code
            objects = object_info.collect { new Tuple3(it.first, find_ddl(it.first, code), it.second) }
        }

        static String find_ddl(String name, String script) {
            String comment_pattern = '((--[^\\n]*(?:\\n|$))\\s*|(?:/\\*(?:[^*]|(\\*(?!/)))*\\*/)\\s*)*'
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
                    case ColumnType.DOUBLE:
                        return "NUMERIC"
                    case ColumnType.LOCAL_DATE:
                        return "DATE"
                    default:
                        return it.toString()
                }
            }
            final coldefs = schema.collect { Column it -> "\"${it.name().toUpperCase()}\" ${sqlName(it.type())}" }
            "create table $name (${sql_list(coldefs)})"
        }

        static String insert_dml(String name, List<Column<?>> schema) {
            final colnames = schema.collect { col -> "\"${col.name().toUpperCase()}\"" }
            "insert into ${name} (${sql_list(colnames)}) values (${sql_list(schema.collect { '?' })})"
        }
    }

    static class LOINC_NAACCR {
        // TODO: static final measure = read_csv(TumorOnt.getResource('loinc_naaccr/loinc_naaccr.csv'))
        static final Table answer = read_csv(TumorOnt.getResource('loinc_naaccr/loinc_naaccr_answer.csv'))
        // TODO? static final answer_struct = answer.columns().collect { Column<?> it -> it.copy().setName(it.name().toLowerCase()) }
    }

    static class NAACCR_I2B2 {
        static final String top_folder = "\\i2b2\\naaccr\\"
        static final String c_name = 'Cancer Cases (NAACCR Hierarchy)'
        static final String sourcesystem_cd = 'heron-admin@kumc.edu'

        static final Table tumor_item_type = read_csv(TumorOnt.getResource('heron_load/tumor_item_type.csv'))
        static final Table seer_recode_terms = read_csv(TumorOnt.getResource('heron_load/seer_recode_terms.csv'))

        static final Table cs_terms = read_csv(TumorOnt.getResource('heron_load/cs-terms.csv'))

        /* obsolete
        static final tx_script = new SqlScript(
                'naaccr_txform.sql',
                resourceText(TumorOnt.getResource('heron_load/naaccr_txform.sql')), [])
        */

        static final String per_item_view = 'tumor_item_type'
        static final Table per_section = read_csv(TumorOnt.getResource('heron_load/section.csv'))

        static SqlScript ont_script = new SqlScript(
                'naaccr_concepts_load.sql',
                resourceText('heron_load/naaccr_concepts_load.sql'),
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

        static Table naaccr_top(LocalDate update_date) {
            fromRecords([[c_hlevel       : 1,
                          c_fullname     : top_folder,
                          c_name         : c_name,
                          update_date    : update_date,
                          sourcesystem_cd: sourcesystem_cd] as Map])
        }

        static Table ont_view_in(Sql sql, String task_hash, LocalDate update_date, Path who_cache) {
            Table icd_o_topo
            if (who_cache.toFile().exists()) {
                final Table who_topo = OncologyMeta.read_table(who_cache, OncologyMeta.topo_info)
                icd_o_topo = OncologyMeta.icd_o_topo(who_topo)
            } else {
                log.warning('skipping WHO Topology terms')
                icd_o_topo = fromRecords([[
                                                  lvl : 3, concept_cd: 'C00', c_visualattributes: 'FA',
                                                  path: 'abc', concept_path: 'LIP', concept_name: 'x'] as Map])
            }

            final current_task = fromRecords([[task_hash: task_hash] as Map]).setName("current_task")
            cs_terms.removeColumns('update_date', 'sourcesystem_cd') // KLUDGE: mutable. at least it's idempotent.
            final tables = [
                    current_task       : current_task,
                    naaccr_top         : naaccr_top(update_date),
                    section            : per_section,
                    tumor_item_type    : tumor_item_type,
                    loinc_naaccr_answer: LOINC_NAACCR.answer,
                    code_labels        : NAACCR_R.code_labels(),
                    icd_o_topo         : icd_o_topo,
                    cs_terms           : cs_terms,
                    seer_site_terms    : seer_recode_terms,
            ] as Map
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
            try {
                sql.execute("drop table if exists $name".toString())
            } catch (SQLException ignored) {
            }
            try {
                sql.execute("drop view if exists $name".toString())
            } catch (SQLException ignored) {
            }
            sql.execute(it.second)
            Table dft = null
            sql.query("select * from $name".toString()) { ResultSet results ->
                dft = Table.read().db(results, name)
            }
            [(name): dft]
        }
    }

    static String _logged(String txt) {
        log.info(txt)
        txt
    }

    static void load_data_frame(Sql sql, String name, Table data) {
        sql.execute(SqlScript.create_ddl(name, data.columns()))
        sql.withBatch(SqlScript.insert_dml(name, data.columns())) { BatchingPreparedStatementWrapper ps ->
            for (Row row : data) {
                final params = []
                for (Column col : data.columns()) {
                    if (row.isMissing(col.name())) {
                        params << null
                        continue
                    }
                    switch (col.type()) {
                        case ColumnType.INTEGER:
                            params << row.getInt(params.size())
                            break
                        case ColumnType.LONG:
                            params << row.getLong(params.size())
                            break
                        case ColumnType.STRING:
                            params << row.getString(params.size())
                            break
                        case ColumnType.LOCAL_DATE:
                            params << java.sql.Date.valueOf(row.getDate((params.size())))
                            break
                        case ColumnType.BOOLEAN:
                            params << row.getBoolean(params.size())
                            break
                        case ColumnType.DOUBLE:
                            params << row.getDouble(params.size())
                            break
                        default:
                            throw new IllegalArgumentException(col.type().toString())
                    }
                }
                ps.addBatch(params)
            }
        }
    }

    static Table read_csv(URL url, ColumnType[] _schema = null, int skiprows = 0) {
        ColumnType[] schema = _schema ? _schema : tabularTypes(new JsonSlurper().parse(meta_path(url)))
        final BufferedReader input = new BufferedReader(new InputStreamReader(url.openStream()))
        skiprows.times { input.readLine() }
        Table.read().usingOptions(CsvReadOptions.builder(input).columnTypes(schema))
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

    static String resourceText(String s) {
        TumorOnt.getResourceAsStream(s).text
    }

    static URL meta_path(URL path) {
        Pathlib.parent(path).resolve(Pathlib.stem(path) + '-metadata.json').toURL()
    }

    static class Pathlib {
        static URI parent(URL path) { path.toURI().resolve('./') }

        static String stem(URL path) {
            final String[] segments = path.path.split('/')
            segments.last().replaceFirst('[.][^.]+$', "")
        }
    }

    static Table fromRecords(List<Map<String, Object>> obj) {
        Collection<Column<?>> cols = ((obj.first().collect { k, v ->
            switch (v) {
                case String:
                    return StringColumn.create(k)
                case Integer:
                    return IntColumn.create(k)
                case LocalDate:
                    return DateColumn.create(k)
                case Boolean:
                    return BooleanColumn.create(k)
                default:
                    throw new IllegalArgumentException("Expected String or Int in 1st record, not:" + v)
            }
        }) as Collection<Column<?>>)
        Table data = Table.create("t1", cols)
        obj.each { Map<String, Object> m ->
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
                    case Boolean:
                        row.setBoolean(it.key, it.value as boolean)
                        break
                    case null:
                        row.setMissing(it.key)
                        break
                    default:
                        throw new IllegalArgumentException("not supported " + it.value)
                }
            }
        }
        data
    }
}