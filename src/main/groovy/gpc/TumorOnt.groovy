package gpc

import com.imsweb.naaccrxml.NaaccrXmlDictionaryUtils
import com.imsweb.staging.Staging
import com.imsweb.staging.cs.CsDataProvider
import gpc.DBConfig.Task
import gpc.Tabular.ColumnMeta
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Immutable
import groovy.util.logging.Slf4j

import java.nio.file.Path
import java.nio.file.Paths
import java.sql.SQLException
import java.sql.Types
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
    static List<ColumnMeta> metadataColumns = [
            new ColumnMeta(name: 'C_HLEVEL', dataType: Types.INTEGER, nullable: false),
            new ColumnMeta(name: 'C_FULLNAME', size: 700, nullable: false),
            new ColumnMeta(name: 'C_NAME', size: 2000, nullable: false),
            new ColumnMeta(name: 'C_SYNONYM_CD', dataType: Types.CHAR, size: 1, nullable: false),
            new ColumnMeta(name: 'C_VISUALATTRIBUTES', dataType: Types.CHAR, size: 3, nullable: false),
            new ColumnMeta(name: 'C_TOTALNUM', dataType: Types.INTEGER),
            new ColumnMeta(name: 'C_BASECODE', size: 50),
            new ColumnMeta(name: 'C_METADATAXML', dataType: Types.CLOB),
            new ColumnMeta(name: 'C_FACTTABLECOLUMN', size: 50, nullable: false),
            new ColumnMeta(name: 'C_TABLENAME', size: 50, nullable: false),
            new ColumnMeta(name: 'C_COLUMNNAME', size: 50, nullable: false),
            new ColumnMeta(name: 'C_COLUMNDATATYPE', size: 50, nullable: false),
            new ColumnMeta(name: 'C_OPERATOR', size: 10, nullable: false),
            new ColumnMeta(name: 'C_DIMCODE', size: 700, nullable: false),
            new ColumnMeta(name: 'C_COMMENT', dataType: Types.CLOB),
            new ColumnMeta(name: 'C_TOOLTIP', size: 900),
            new ColumnMeta(name: 'M_APPLIED_PATH', size: 700, nullable: false),
            new ColumnMeta(name: 'UPDATE_DATE', dataType: Types.TIMESTAMP, nullable: false),
            new ColumnMeta(name: 'DOWNLOAD_DATE', dataType: Types.TIMESTAMP),
            new ColumnMeta(name: 'IMPORT_DATE', dataType: Types.TIMESTAMP),
            new ColumnMeta(name: 'SOURCESYSTEM_CD', size: 50),
            new ColumnMeta(name: 'VALUETYPE_CD', size: 50),
            new ColumnMeta(name: 'M_EXCLUSION_CD', size: 25),
            new ColumnMeta(name: 'C_PATH', size: 700),
            new ColumnMeta(name: 'C_SYMBOL', size: 50),
    ]
    static final URL sectionCSV = TumorOnt.getResource('heron_load/section.csv')
    static final URL itemCSV = TumorOnt.getResource('heron_load/tumor_item_type.csv')

    static Map<Integer, Map> itemsByNum() {
        final Map<Integer, Map> byNum = Tabular.allCSVRecords(itemCSV)
                .findAll { it.valtype_cd == '@' }
                .collectEntries { [it.naaccrNum as int, it] }
        byNum
    }

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
        final insert = ColumnMeta.insertStatement(table_name, TumorOnt.metadataColumns)
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

    static List<Map<String, Object>> pcornet_fields = Tabular.allCSVRecords(TumorOnt.getResource('fields.csv'))

    /**
     * PCORnet tumor table fields
     * @param strict - only include fields where v18 naaccrId is known?
     * @param includePrivate - include PHI fields?
     * @return Table
     */
    static List<Map<String, Object>> fields(boolean strict = true, String version = "180") {
        final pcornet_spec = pcornet_fields.collect { [item: it.item, FIELD_NAME: it.FIELD_NAME] }

        final byNum = NaaccrXmlDictionaryUtils.getBaseDictionaryByVersion(version).items
                .collectEntries { [it.naaccrNum, it.naaccrId] }
        def items = pcornet_spec.collect {
            [naaccrId: byNum[it.item as int], naaccrNum: it.item, FIELD_NAME: it.FIELD_NAME] as Map<String, Object>
        }
        if (strict) {
            items = items.findAll { it.naaccrId != null }
        }
        items
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
                        """, [fullname: top.C_FULLNAME, code: version + task_hash])
                    return row && row[0] == 1
                }
            } catch (SQLException problem) {
                log.warn("not complete: $problem")
            }
            return false
        }

        void run() {
            cdw.withSql { Sql sql ->
                createTable(sql, table_name)
            }
        }


    }

    static void createTable(Sql sql, String table_name) {
        final toTypeName = ColumnMeta.typeNames(sql.connection)
        TumorFile.dropIfExists(sql, table_name)
        sql.execute(ColumnMeta.createStatement(table_name, metadataColumns, toTypeName))

        final top_term = normal_term + top
        final insert1 = ColumnMeta.insertStatement(table_name, metadataColumns)
                .replace('?.UPDATE_DATE', 'current_timestamp')
        sql.execute(insert1, top_term)
        insertTerms(sql, table_name, sectionCSV, { Map s -> makeSectionTerm(s) })
        insertTerms(sql, table_name, itemCSV, { Map s -> makeItemTerm(s) })

        sql.withBatch(16, insert1) { ps ->
            final done = []
            NAACCR_R.eachCodeTerm { Map it ->
                ps.addBatch(it)
                done << it.C_FULLNAME
            }
            LOINC_NAACCR.eachAnswerTerm { Map it ->
                if (!done.contains(it.C_FULLNAME)) {
                    ps.addBatch(it)
                }
            }
        }

        // TODO: OncologyMeta

        sql.execute(insert1, SEERRecode.folder)

        insertTerms(sql, table_name, SEERRecode.seer_recode_terms, { Map s -> SEERRecode.makeTerm(s) })
        insertTerms(sql, table_name, CSTerms.cs_terms, { Map s -> CSTerms.makeTerm(s) })
    }

    static class NAACCR_R {
        // Names assumed by naaccr_txform.sql

        static final URL field_info_csv = TumorOnt.getResource('naaccr_r_raw/field_info.csv')
        static final URL field_code_scheme_csv = TumorOnt.getResource('naaccr_r_raw/field_code_scheme.csv')
        static final List<ColumnMeta> field_info_meta = [
                new ColumnMeta(name: "item", dataType: Types.INTEGER),
                new ColumnMeta(name: "name"),
                new ColumnMeta(name: "type"),
        ]
        static final Map code_label_meta = [
                tableSchema: [columns: [
                        [number: 1, name: "code", datatype: "string", null: [""]],
                        [number: 2, name: "label", datatype: "string", null: [""]],
                        [number: 3, name: "means_missing", datatype: "boolean", null: [""]],
                        [number: 4, name: "description", datatype: "string", null: [""]],
                ]]
        ]
        static final List<ColumnMeta> field_code_scheme_meta = [
                new ColumnMeta(name: "name"),
                new ColumnMeta(name: "scheme"),
        ]
        static final URL _code_labels = TumorOnt.getResource('naaccr_r_raw/code-labels/')

        static void eachFieldScheme(Closure thunk) {
            field_code_scheme_csv.withInputStream { fs ->
                Tabular.eachCSVRecord(fs, field_code_scheme_meta) { Map it ->
                    final url = new URL(_code_labels, "${it.scheme}.csv")
                    thunk(it + [url: url])
                    return
                }
            }
        }

        static void eachCodeLabel(Closure thunk,
                                  List<String> implicit = ['iso_country']) {
            Map<Integer, Map> byNum = itemsByNum()
            Map byName = field_info_csv.withInputStream { csv ->
                Tabular.allCSVRecords(csv, field_info_meta)
                        .collectEntries { it -> [it.name as String, it] }
            }
            eachFieldScheme { Map fs ->
                if (implicit.contains(fs.scheme)) {
                    return
                }

                final field = byName[fs.name] as Map
                assert field != null
                final ty = byNum[field.item as int]
                if (ty == null) {
                    log.warn("no tumor_item_type info about ${field}")
                    return
                }
                final ic = makeItemTerm(ty)
                final seen = [:]
                new URL(fs.url as String).withInputStream { csv ->
                    Tabular.eachCSVRecord(csv, Tabular.columnDescriptions(code_label_meta)) { Map code ->
                        if (seen[code.code]) {
                            log.debug("ignoring duplicate code for ${fs.name}: ${code}")
                            return
                        }
                        seen[code.code] = true
                        thunk(fs + field + ty + ic + code)
                        return
                    }
                }
            }
        }

        static void eachCodeTerm(Closure thunk) {
            eachCodeLabel { Map it ->
                final c_name = substr("${it.code} ${it.label}", 0, 200)
                String c_basecode = "NAACCR|${it.naaccrNum}:${it.code}"
                String path = "${it.C_FULLNAME}${it.code}\\"
                thunk(normal_term + [
                        C_HLEVEL          : it.C_HLEVEL as int + 1,
                        C_FULLNAME        : path,
                        C_DIMCODE         : path,
                        C_NAME            : c_name,
                        C_BASECODE        : c_basecode,
                        C_VISUALATTRIBUTES: 'LA',
                        C_TOOLTIP         : it.description,
                ])
            }
        }
    }

    @Immutable
    static class OncologyMeta {
        List<Map<String, Object>> topo
        List<Map<String, Object>> morph2
        List<Map<String, Object>> morph3

        static OncologyMeta load(Path cache,
                                 String zip2 = 'ICD-O-2_CSV.zip',
                                 String zip3 = 'ICD-O-3_CSV-metadata.zip') {
            final icdo2zip = new ZipFile(cache.resolve(zip2).toFile())
            final icdo3zip = new ZipFile(cache.resolve(zip3).toFile())

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
                        [Kode: it[0], Lvl: it[1], Title: it[2]] as Map<String, Object>
                    }
            final morph2 = allRows(icdo2zip, 'icd-o-3-morph.csv')
                    .collect { it ->
                        [code: it[0].replace('M', ''), label: it[1]] as Map<String, Object>
                    }
            final morph3 = allRows(icdo3zip, 'Morphenglish.txt')
                    .drop(1)
                    .collect { [code: it[0], struct: it[1], label: it[2]] as Map<String, Object> }
            new OncologyMeta(topo: topo, morph2: morph2, morph3: morph3)
        }

        List<Map> major() {
            morph2.findAll { !(it.code as String).contains('/') }.collect { [level: 3 as Object] + it }
        }

        def primarySiteTerms() {
            def major = topo.findAll { it.Lvl == '3' }.collect {
                [
                        lvl               : 3,
                        concept_cd        : it.Kode,
                        c_visualattributes: 'FA',
                        path              : "${it.Kode}\\".toString(),
                        concept_name      : it.Title,
                ]
            }

            def minor = topo.findAll { it.Lvl == '4' }.collect {
                [
                        lvl               : 4,
                        concept_cd        : (it.Kode as String).replace('.', ''),
                        c_visualattributes: 'LA',
                        path              : "${(it.Kode as String).replaceAll('\\..*', '')}\\${it.Kode}\\".toString(),
                        concept_name      : it.Title,
                ]
            }
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
                            level           : parent.C_HLEVEL as int + 1,
                            code            : it.code, // TODO: more to it
                            visualattributes: 'FA',
                            path            : "${parent.C_FULLNAME}${it.code}\\".toString(),
                            concept_name    : "${it.code} ${it.label}".toString(),
                    ]
                }
            }.flatten()
        }
    }

    static List listZip(List a, List b, Closure f) {
        def result = []
        0.upto(Math.min(a.size(), b.size()) - 1) { ix -> result << f(a[ix], b[ix]) }
        result
    }

    static class LOINC_NAACCR {
        // TODO? static final answer_struct = answer.columns().collect { Column<?> it -> it.copy().setName(it.name().toLowerCase()) }
        // TODO: static final measure = read_csv(gpc.TumorOnt.getResource('loinc_naaccr/loinc_naaccr.csv'))

        // LOINC_NUMBER,COMPONENT,CODE_SYSTEM,CODE_VALUE,AnswerListId,AnswerListName,ANSWER_CODE,SEQUENCE_NO,ANSWER_STRING
        static URL loinc_naaccr_answer = TumorOnt.getResource('loinc_naaccr/loinc_naaccr_answer.csv')

        static void eachAnswerTerm(Closure<Void> thunk) {
            Map<Integer, Map> byNum = itemsByNum()
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
    }

    static String _logged(String txt) {
        log.info(txt)
        txt
    }

    static Staging staging = Staging.getInstance(CsDataProvider.getInstance(CsDataProvider.CsVersion.v020550));
    static List<List<String>> primarySiteTable() {
        staging.getTable("primary_site").rawRows
    }
}
