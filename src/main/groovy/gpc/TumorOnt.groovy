package gpc

import com.imsweb.naaccrxml.NaaccrXmlDictionaryUtils
import com.imsweb.naaccrxml.entity.dictionary.NaaccrDictionary
import gpc.DBConfig.Task
import groovy.json.JsonSlurper
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Immutable
import groovy.util.logging.Slf4j
import tech.tablesaw.api.*
import tech.tablesaw.columns.Column
import tech.tablesaw.io.csv.CsvReadOptions

import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.*
import java.time.LocalDate
import java.util.zip.ZipFile

@CompileStatic
@Slf4j
class TumorOnt {
    static void run_cli(DBConfig.CLI cli) {
        DBConfig cdw = cli.account()

        if (cli.flag("ontology")) {
            Task work = new Ontology1(
                    cli.arg("--table-name"),
                    Integer.parseInt(cli.arg("--version")),
                    cli.arg("--task-hash", "???"),
                    // TODO: current calendar date
                    LocalDate.parse(cli.arg("--update-date", "2001-01-01")),
                    cdw, cli.arg("--who-cache") ? Paths.get(cli.arg("--who-cache")) : null)
            if (!work.complete()) {
                work.run()
            }
        } else if (cli.flag("import")) {
            cdw.withSql { Sql sql ->
                Tabular.importCSV(sql,
                        cli.arg("TABLE"),
                        cli.pathArg("DATA").toUri().toURL(),
                        Tabular.metadata(cli.pathArg("META").toUri().toURL()))
            }
        }
    }

    /**
     * create_oracle_i2b2metadata_tables.sql
     * create_postgresql_i2b2metadata_tables.sql
     * https://github.com/i2b2/i2b2-data/blob/master/edu.harvard.i2b2.data/Release_1-7/NewInstall/Metadata/scripts/
     */
    static List<Tabular.ColumnMeta> metadataColumns = [
            new Tabular.ColumnMeta(name: 'C_HLEVEL', dataType: Types.INTEGER, nullable: false),
            new Tabular.ColumnMeta(name: 'C_FULLNAME', size: 700, nullable: false),
            new Tabular.ColumnMeta(name: 'C_NAME', size: 2000, nullable: false),
            new Tabular.ColumnMeta(name: 'C_SYNONYM_CD', dataType: Types.CHAR, size: 1, nullable: false),
            new Tabular.ColumnMeta(name: 'C_VISUALATTRIBUTES', dataType: Types.CHAR, size: 3, nullable: false),
            new Tabular.ColumnMeta(name: 'C_TOTALNUM', dataType: Types.INTEGER),
            new Tabular.ColumnMeta(name: 'C_BASECODE', size: 50),
            new Tabular.ColumnMeta(name: 'C_METADATAXML', dataType: Types.CLOB),
            new Tabular.ColumnMeta(name: 'C_FACTTABLECOLUMN', size: 50, nullable: false),
            new Tabular.ColumnMeta(name: 'C_TABLENAME', size: 50, nullable: false),
            new Tabular.ColumnMeta(name: 'C_COLUMNNAME', size: 50, nullable: false),
            new Tabular.ColumnMeta(name: 'C_COLUMNDATATYPE', size: 50, nullable: false),
            new Tabular.ColumnMeta(name: 'C_OPERATOR', size: 10, nullable: false),
            new Tabular.ColumnMeta(name: 'C_DIMCODE', size: 700, nullable: false),
            new Tabular.ColumnMeta(name: 'C_COMMENT', dataType: Types.CLOB),
            new Tabular.ColumnMeta(name: 'C_TOOLTIP', size: 900),
            new Tabular.ColumnMeta(name: 'M_APPLIED_PATH', size: 700, nullable: false),
            new Tabular.ColumnMeta(name: 'UPDATE_DATE', dataType: Types.TIMESTAMP, nullable: false),
            new Tabular.ColumnMeta(name: 'DOWNLOAD_DATE', dataType: Types.TIMESTAMP),
            new Tabular.ColumnMeta(name: 'IMPORT_DATE', dataType: Types.TIMESTAMP),
            new Tabular.ColumnMeta(name: 'SOURCESYSTEM_CD', size: 50),
            new Tabular.ColumnMeta(name: 'VALUETYPE_CD', size: 50),
            new Tabular.ColumnMeta(name: 'M_EXCLUSION_CD', size: 25),
            new Tabular.ColumnMeta(name: 'C_PATH', size: 700),
            new Tabular.ColumnMeta(name: 'C_SYMBOL', size: 50),
    ]
    static final URL sectionCSV = TumorOnt.getResource('heron_load/section.csv')
    static final URL itemCSV = TumorOnt.getResource('heron_load/tumor_item_type.csv')


    static void writeTerms(Writer out, LocalDate update_date, URL curated, Closure<Map> makeTerm) {
        final hd = TumorOnt.metadataColumns.collect { it.name }
        Tabular.writeCSV(out, hd) { Closure<Void> process ->
            curated.withInputStream { stream ->
                Tabular.eachCSVRecord(stream, Tabular.columnDescriptions(curated)) { Map record ->
                    process(makeTerm(record) + [UPDATE_DATE: update_date])
                }
            }
        }
    }

    static void insertTerms(Sql sql, String table_name, URL curated, Closure<Map> makeTerm) {
        final insert = Tabular.ColumnMeta.insertStatement(table_name, TumorOnt.metadataColumns)
                .replace('?.UPDATE_DATE', 'current_timestamp')
        sql.withBatch(16, insert) { ps ->
            curated.withInputStream { stream ->
                Tabular.eachCSVRecord(stream, Tabular.columnDescriptions(curated)) { Map record ->
                    final term = makeTerm(record)
                    ps.addBatch(term)
                }
            }
        }
    }

    static final Map top = [
            C_HLEVEL          : 1,
            C_FULLNAME        : "\\i2b2\\naaccr\\",
            C_DIMCODE         : "\\i2b2\\naaccr\\",
            C_NAME            : 'Cancer Cases (NAACCR Hierarchy)',
            SOURCESYSTEM_CD   : 'heron-admin@kumc.edu',
            C_VISUALATTRIBUTES: 'FA',
    ] as Map
    static final Map normal_term = [
            C_SYNONYM_CD     : 'N',
            C_FACTTABLECOLUMN: 'CONCEPT_CD',
            C_TABLENAME      : 'CONCEPT_DIMENSION',
            C_COLUMNNAME     : 'CONCEPT_PATH',
            C_COLUMNDATATYPE : 'T',
            C_OPERATOR       : 'LIKE',
            C_COMMENT        : null,
            M_APPLIED_PATH   : '@', // @@not_null
            M_EXCLUSION_CD   : '@',
            C_TOTALNUM       : null,
            VALUETYPE_CD     : null,
            C_METADATAXML    : null,
    ] as Map

    static Map makeSectionTerm(Map section) {
        String path = "${top.C_FULLNAME}S:${section.sectionid} ${section.section}\\"
        normal_term + [
                C_HLEVEL          : top.C_HLEVEL as int + 1,
                C_FULLNAME        : path,
                C_DIMCODE         : path,
                C_NAME            : "${String.format('%02d', section.sectionid)} ${section.section}".toString(),
                C_BASECODE        : null,
                C_VISUALATTRIBUTES: 'FA',
                C_TOOLTIP         : null, // TODO
        ] as Map
    }

    static String substr(String s, int lo, int hi) {
        s.length() > hi ? s.substring(lo, hi) : s
    }

    static Map makeItemTerm(Map item) {
        final sc = makeSectionTerm([sectionid: item.sectionId, section: item.section])
        String path = sc.C_FULLNAME as String + substr("${String.format('%04d', item.naaccrNum)} ${item.naaccrName}".toString(), 0, 40) + '\\'
        normal_term + [
                C_HLEVEL          : sc.C_HLEVEL as int + 1,
                C_FULLNAME        : path,
                C_DIMCODE         : path,
                C_NAME            : "${String.format('%04d', item.naaccrNum)} ${item.naaccrName}".toString(),
                C_BASECODE        : "NAACCR|${item.naaccrNum}:".toString(),
                // TODO: hide concepts where we have no data
                // TODO: hide Histology since '0420', '0522' we already have Morph--Type/Behav
                C_VISUALATTRIBUTES: item.valtype_cd == '@' ? 'FA' : 'LA',
                C_TOOLTIP         : null, // TODO
        ] as Map
    }

    static class SEERRecode {
        static URL seer_recode_terms = TumorOnt.getResource('seer_recode_terms.csv')
        static Map folder = normal_term + [
                C_HLEVEL          : top.C_HLEVEL as int + 1,
                C_FULLNAME        : top.C_FULLNAME as String + 'SEER Site\\',
                C_DIMCODE         : top.C_FULLNAME as String + 'SEER Site\\',
                C_NAME            : "SEER Site Summary",
                C_BASECODE        : null,
                C_VISUALATTRIBUTES: 'FA',
                C_TOOLTIP         : 'SEER Site Recode ICD-O-3/WHO 2008 Definition',
        ] as Map

        static Map makeTerm(Map item) {
            final path = "${folder.C_FULLNAME}${item.path}\\".toString()
            normal_term + [
                    C_HLEVEL          : folder.C_HLEVEL as int + 1,
                    C_FULLNAME        : path,
                    C_DIMCODE         : path,
                    C_NAME            : item.name,
                    C_BASECODE        : "SEER_SITE:${item.basecode}".toString(),
                    C_VISUALATTRIBUTES: item.visualattributes,
                    C_TOOLTIP         : null, // TODO
            ] as Map
        }
    }

    static class CSTerms {
        static URL cs_terms = TumorOnt.getResource('heron_load/cs-terms.csv')

        static Map makeTerm(Map item) {
            normal_term + [
                    C_HLEVEL          : item.c_hlevel,
                    C_FULLNAME        : item.c_fullname,
                    C_DIMCODE         : item.c_fullname,
                    C_NAME            : item.c_name,
                    C_BASECODE        : item.c_basecode,
                    C_VISUALATTRIBUTES: item.c_visualattributes,
                    C_TOOLTIP         : item.c_tooltip,
            ]
        }
    }

    static Table pcornet_fields = read_csv(TumorOnt.getResource('fields.csv')).setName("FIELDS")

    /**
     * PCORnet tumor table fields
     * @param strict - only include fields where v18 naaccrId is known?
     * @param includePrivate - include PHI fields?
     * @return Table
     */
    static Table fields(boolean strict = true) {
        Table pcornet_spec = read_csv(TumorOnt.getResource('fields.csv')).select(
                'item', 'FIELD_NAME'
        )
        // get naaccr-xml naaccrId
        final dd = ddictDF()
        Table items
        if (strict) {
            items = pcornet_spec.joinOn('item').inner(dd, 'naaccrNum')
        } else {
            items = pcornet_spec.joinOn('item').leftOuter(dd, 'naaccrNum')
        }
        items.column("item").setName("naaccrNum")
        items.setName("TUMOR")
        items.select('naaccrNum', 'naaccrId', 'FIELD_NAME')
    }

    @Deprecated
    static Table ddictDF(String version = "180") {
        NaaccrDictionary baseDictionary = NaaccrXmlDictionaryUtils.getBaseDictionaryByVersion(version)
        final items = baseDictionary.items
        Table.create(
                IntColumn.create("naaccrNum", items.collect { it.naaccrNum } as int[]),
                StringColumn.create("naaccrId", items.collect { it.naaccrId }),
                StringColumn.create("naaccrName", items.collect { it.naaccrName }),
                IntColumn.create("startColumn", items.collect { it.startColumn } as int[]),
                IntColumn.create("length", items.collect { it.length } as int[]),
                StringColumn.create("parentXmlElement", items.collect { it.parentXmlElement }))

    }

    static class Ontology1 implements Task {
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
                    final row = sql.firstRow("""
                        select 1 from ${table_name}
                            where c_fullname = ?.fullname
                            and c_basecode = ?.code
                        """, [fullname: NAACCR_I2B2.top_folder, code: version + task_hash])
                    return row && row[0] == 1
                }
            } catch (SQLException problem) {
                log.warn("not complete: $problem")
            }
            return false
        }

        void run() {
            cdw.withSql { Sql sql ->
                final toTypeName = Tabular.ColumnMeta.typeNames(sql.connection)
                TumorFile.dropIfExists(sql, table_name)
                sql.execute(Tabular.ColumnMeta.createStatement(table_name, metadataColumns, toTypeName))

                final top_term = normal_term + top
                final insert1 = Tabular.ColumnMeta.insertStatement(table_name, metadataColumns)
                        .replace('?.UPDATE_DATE', 'current_timestamp')
                sql.execute(insert1, top_term)
                insertTerms(sql, table_name, sectionCSV, { Map s -> makeSectionTerm(s) })
                insertTerms(sql, table_name, itemCSV, { Map s -> makeItemTerm(s) })
                LOINC_NAACCR.insertTerms(sql, table_name)
                // TODO: OncologyMeta

                sql.execute(insert1, SEERRecode.folder)
                insertTerms(sql, table_name, SEERRecode.seer_recode_terms, { Map s -> SEERRecode.makeTerm(s) })

                insertTerms(sql, table_name, CSTerms.cs_terms, { Map s -> CSTerms.makeTerm(s) })
            }
        }
    }

    static class NAACCR_R {
        // Names assumed by naaccr_txform.sql

        static final URL field_info_csv = TumorOnt.getResource('naaccr_r_raw/field_info.csv')
        static final Map field_info_meta = [
                tableSchema: [columns: [
                        [number: 1, name: "code", datatype: "string", nulls: [""]],
                        [number: 2, name: "label", datatype: "string", nulls: [""]],
                        [number: 3, name: "means_missing", datatype: "boolean", nulls: [""]],
                        [number: 4, name: "description", datatype: "string", nulls: [""]],
                ]]
        ]
        static final Table field_info = read_csv(field_info_csv, Table.create(
                IntColumn.create("item"),
                StringColumn.create("name"),
                StringColumn.create("type"),
        ).columnTypes())
        static final Table field_info_schema = Table.create(
                StringColumn.create("code"),
                StringColumn.create("label"),
                BooleanColumn.create("means_missing"),
                StringColumn.create("description"))
        static final URL field_code_scheme_csv = TumorOnt.getResource('naaccr_r_raw/field_code_scheme.csv')
        static final Table field_code_scheme = read_csv(field_code_scheme_csv, Table.create(
                StringColumn.create("name"),
                StringColumn.create("scheme"),
        ).columnTypes())

        static final URL _code_labels = TumorOnt.getResource('naaccr_r_raw/code-labels/')

        static Table code_labels(List<String> implicit = ['iso_country']) {
            Table all_schemes = null
            for (String scheme : field_code_scheme.stringColumn('scheme').unique()) {
                if (implicit.contains(scheme)) {
                    continue
                }
                final info = new URL(_code_labels.toString() + scheme + '.csv')  // ugh.
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

    @Immutable
    static class OncologyMeta {
        List<Map<String, Object>> topo
        List<Map<String, Object>> morph2
        List<Map<String, Object>> morph3

        static OncologyMeta load(Path cache) {
            final icdo2zip = new ZipFile(cache.resolve('ICD-O-2_CSV.zip').toFile())
            final icdo3zip = new ZipFile(cache.resolve('ICD-O-3_CSV-metadata.zip').toFile())

            final allRows = { ZipFile zip, String name ->
                final List<List<String>> rows = []
                final entry = zip.getInputStream(zip.getEntry(name))
                println("@@@allRows ${[zip, name, entry, !name.endsWith('.csv')]}")
                Tabular.eachCSVRow(entry, !name.endsWith('.csv')) { rows << (it as List<String>); return }
                rows
            }
            final topo = allRows(icdo2zip, 'Topoenglish.txt')
                .drop(1)
                .collect {
                    println("@@topo collect" + it)
                    [Kode: it[0], Lvl: it[1], Title: it[2]]  as Map<String, Object>
                }
            final morph2 = allRows(icdo2zip, 'icd-o-3-morph.csv')
                    .collect {  it ->
                        [code: it[0].replace('M', ''), label: it[1] ] as Map<String, Object>
                    }
            final morph3 = allRows(icdo3zip, 'Morphenglish.txt')
                    .drop(1)
                    .collect { [code: it[0], struct: it[1], label: it[2] ] as Map<String, Object> }
            new OncologyMeta(topo: topo, morph2: morph2, morph3: morph3)
        }

        List<Map> major() {
            morph2.findAll { !(it.code as String).contains('/') }.collect { [level: 3 as Object] + it }
        }

        def primarySiteTerms() {
            def major = topo.findAll { it.Lvl == '3' }.collect {[
                    lvl: 3,
                    concept_cd: it.Kode,
                    c_visualattributes: 'FA',
                    path: "${it.Kode}\\".toString(),
                    concept_name: it.Title,
            ]}

            def minor = topo.findAll { it.Lvl == '4' }.collect {[
                    lvl: 4,
                    concept_cd: (it.Kode as String).replace('.', ''),
                    c_visualattributes: 'LA',
                    path: "${(it.Kode as String).replaceAll('\\..*', '')}\\${it.Kode}\\".toString(),
                    concept_name: it.Title,
            ]}
            major + minor
        }

        List histBehaviorWIP() {
            // ISSUE: this is actually designed for the combined 419, 521 items
            final itemTerms = Tabular.allCSVRecords(TumorOnt.itemCSV)
                    .findAll { ['histologyIcdO2', 'histologicTypeIcdO3'].contains(it.naaccrId) }
                    .collect { Map s -> TumorOnt.makeItemTerm(s) }


            itemTerms.collect { parent ->
                major().collect {
                    [
                            level: parent.C_HLEVEL as int + 1,
                            code: it.code, // TODO: more to it
                            visualattributes: 'FA',
                            path: "${parent.C_FULLNAME}${it.code}\\".toString(),
                            concept_name: "${it.code} ${it.label}".toString(),
                    ]
                }
            }.flatten()
        }

        static Tuple morph3_info = new Tuple('ICD-O-2_CSV.zip', 'icd-o-3-morph.csv', ['code', 'label', 'notes'])
        static Tuple topo_info = new Tuple('ICD-O-2_CSV.zip', 'Topoenglish.txt', null)
        static String encoding = 'ISO-8859-1'

        @Deprecated
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

        @Deprecated
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

    @Deprecated
    static class SqlScript { // TODO: refactor: move SqlScript out of gpc.TumorOnt
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
                    case ColumnType.BOOLEAN:
                        return "NUMBER(1)"
                    case ColumnType.DOUBLE:
                        return "NUMERIC"
                    case ColumnType.LOCAL_DATE:
                        return "DATE"
                    case ColumnType.LOCAL_DATE_TIME:
                        return "TIMESTAMP"
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
        @Deprecated
        static final Table answer = read_csv(TumorOnt.getResource('loinc_naaccr/loinc_naaccr_answer.csv'))
        // TODO? static final answer_struct = answer.columns().collect { Column<?> it -> it.copy().setName(it.name().toLowerCase()) }
        // TODO: static final measure = read_csv(gpc.TumorOnt.getResource('loinc_naaccr/loinc_naaccr.csv'))

        // LOINC_NUMBER,COMPONENT,CODE_SYSTEM,CODE_VALUE,AnswerListId,AnswerListName,ANSWER_CODE,SEQUENCE_NO,ANSWER_STRING
        static URL loinc_naaccr_answer = TumorOnt.getResource('loinc_naaccr/loinc_naaccr_answer.csv')

        static void eachTerm(Closure<Void> thunk) {
            final Map<Integer, Map> byNum = Tabular.allCSVRecords(itemCSV)
                    .findAll { it.valtype_cd == '@' }
                    .collectEntries { [it.naaccrNum as int, it ] }
            Tabular.allCSVRecords(loinc_naaccr_answer)
                    .findAll {
                        if (it.ANSWER_CODE as String <= '') {
                            return false
                        }
                        final item = byNum[it.CODE_VALUE as int]
                        if (item == null) {
                            return false
                        }
                        it.AnswerListId == item.AnswerListId
                    }
                    .collect {
                        final c_name = substr("${it.ANSWER_CODE} ${it.ANSWER_STRING}", 0, 200)
                        final item = byNum[it.CODE_VALUE as int]
                        final ic = makeItemTerm(item)
                        String c_basecode = "NAACCR|${item.naaccrNum}:${it.ANSWER_CODE}"
                        String path = "${ic.C_FULLNAME}${it.ANSWER_CODE}\\"
                        thunk(normal_term + [
                                C_HLEVEL          : ic.C_HLEVEL as int + 1,
                                C_FULLNAME        : path,
                                C_DIMCODE         : path,
                                C_NAME            : c_name,
                                C_BASECODE        : c_basecode,
                                C_VISUALATTRIBUTES: 'LA',
                                C_TOOLTIP         : null,  // TODO
                        ])
                    }
        }

        static void insertTerms(Sql sql, String table_name) {
            final insert = Tabular.ColumnMeta.insertStatement(table_name, TumorOnt.metadataColumns)
                    .replace('?.UPDATE_DATE', 'current_timestamp')
            sql.withBatch(16, insert) { ps ->
                eachTerm { ps.addBatch(it) }
            }
        }
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
                resourceText(gpc.TumorOnt.getResource('heron_load/naaccr_txform.sql')), [])
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

        static Table ont_view_in(Sql sql, String task_hash, LocalDate update_date, Path who_cache = null) {
            Table icd_o_topo
            if (who_cache != null && who_cache.toFile().exists()) {
                final Table who_topo = OncologyMeta.read_table(who_cache, OncologyMeta.topo_info)
                icd_o_topo = OncologyMeta.icd_o_topo(who_topo)
            } else {
                log.warn('skipping WHO Topology terms')
                icd_o_topo = fromRecords([[
                                                  lvl : 3, concept_cd: 'C00', c_visualattributes: 'FA',
                                                  path: 'abc', concept_path: 'LIP', concept_name: 'x'] as Map])
            }

            final current_task = fromRecords([[task_hash: task_hash] as Map]).setName("current_task")
            // KLUDGE: mutable.
            if (cs_terms.columnNames().contains('update_date')) {
                cs_terms.removeColumns('update_date', 'sourcesystem_cd')
            }
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
            views[name](sql)
        }
    }

    static Map<String, Closure<Table>> create_objects(Sql sql, SqlScript script,
                                                      Map<String, Table> tables) {
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
            dropObject(sql, name)
            sql.execute(it.second)
            Closure<Table> getDF = { Sql sql2 ->
                Table dft = null
                sql2.query("select * from $name".toString()) { ResultSet results ->
                    dft = Table.read().db(results, name)
                }
                dft
            }
            [(name): getDF]
        }
    }

    private static void dropObject(Sql sql, String name) {
        try {
            sql.execute("drop table if exists $name".toString())
        } catch (SQLException ignored) {
        }
        try {
            sql.execute("drop view if exists $name".toString())
        } catch (SQLException ignored) {
        }
    }

    static String _logged(String txt) {
        log.info(txt)
        txt
    }

    @Deprecated
    static void load_data_frame(Sql sql, String name, Table data,
                                boolean dropFirst = false) {
        assert name
        if (dropFirst) {
            try {
                sql.execute("drop table $name".toString())
            } catch (SQLException problem) {
                log.warn("drop $name: $problem")
            }
        }
        log.debug("creating table ${name}")
        sql.execute(SqlScript.create_ddl(name, data.columns()))
        append_data_frame(data, name, sql)
    }

    @Deprecated
    static void append_data_frame(Table data, String name, Sql sql) {
        log.debug("inserting ${data.rowCount()} rows into ${name}")
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
                            params << Date.valueOf(row.getDate((params.size())))
                            break
                        case ColumnType.LOCAL_DATE_TIME:
                            params << Timestamp.valueOf(row.getDateTime((params.size())))
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
            log.debug("inserted ${data.rowCount()} rows into ${name}")
        }
    }

    @Deprecated
    static Table importCSV(Sql sql, String name, URL data, URL meta) {
        ColumnType[] schema = tabularTypes(new JsonSlurper().parse(meta))
        final Table df = read_csv(data, schema)
        load_data_frame(sql, name, df)
        df
    }

    @Deprecated
    static Table read_csv(URL url, ColumnType[] _schema = null, int skiprows = 0) {
        ColumnType[] schema = _schema ? _schema : tabularTypes(Tabular.metadata(url))
        final BufferedReader input = new BufferedReader(new InputStreamReader(url.openStream()))
        skiprows.times { input.readLine() }
        Table.read().usingOptions(CsvReadOptions.builder(input).columnTypes(schema).maxCharsPerColumn(32767))
    }

    static ColumnType[] tabularTypes(Object meta) {
        final schema = Tabular.columnDescriptions(meta)
        schema.collect {
            switch (it.dataType) {
                case Types.INTEGER:
                    return ColumnType.INTEGER
                case Types.DOUBLE:
                    return ColumnType.DOUBLE
                case Types.VARCHAR:
                    return ColumnType.STRING
                default:
                    throw new IllegalAccessException(it.dataType.toString())
            }
        } as ColumnType[]
    }

    static String resourceText(String s) {
        TumorOnt.getResourceAsStream(s).text
    }


    static class Pathlib {
        static String stem(URL path) {
            final String[] segments = path.path.split('/')
            segments.last().replaceFirst('[.][^.]+$', "")
        }
    }

    @Deprecated
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
