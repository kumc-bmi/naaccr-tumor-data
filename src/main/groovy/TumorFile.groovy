import DBConfig.Task
import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.record.fixed.FixedColumnsField
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import com.imsweb.naaccrxml.NaaccrXmlDictionaryUtils
import com.imsweb.naaccrxml.PatientFlatReader
import com.imsweb.naaccrxml.PatientReader
import com.imsweb.naaccrxml.entity.Item
import com.imsweb.naaccrxml.entity.Patient
import com.imsweb.naaccrxml.entity.Tumor
import com.imsweb.naaccrxml.entity.dictionary.NaaccrDictionary
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.docopt.Docopt
import tech.tablesaw.api.*
import tech.tablesaw.columns.Column
import tech.tablesaw.columns.strings.StringFilters

import java.nio.charset.Charset
import java.sql.Clob
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.zip.CRC32

import static TumorOnt.load_data_frame

@CompileStatic
@Slf4j
class TumorFile {
    // see also: buildUsageDoc groovy task
    static final String usage = TumorOnt.resourceText('usage.txt')

    static void main(String[] args) {
        DBConfig.CLI cli = new DBConfig.CLI(new Docopt(usage).parse(args),
                { String name ->
                    Properties ps = new Properties()
                    new File(name).withInputStream { ps.load(it) }
                    if (ps.containsKey('db.passkey')) {
                        ps.setProperty('db.password', System.getenv(ps.getProperty('db.passkey')))
                    }
                    ps
                },
                { int it -> System.exit(it) },
                { String url, Properties ps -> DriverManager.getConnection(url, ps) })

        run_cli(cli)
    }

    static void run_cli(DBConfig.CLI cli) {
        DBConfig cdw = cli.account()

        Closure<URL> flat_file = { ->
            cli.flag('--no-file') ? null : cli.urlProperty("naaccr.flat-file")
        }

        // IDEA: support disk DB in place of memdb
        //noinspection GroovyUnusedAssignment -- avoids weird cast error
        Task work = null
        String task_id = cli.arg("--task-id", "task123")  // TODO: replace by date, NPI?
        if (cli.flag('load-records')) {
            work = new NAACCR_Records(cdw, flat_file(), cli.property("naaccr.records-table"))
        } else if (cli.flag('discrete-data')) {
            work = new NAACCR_Extract(cdw, task_id,
                    flat_file(),
                    cli.property("naaccr.records-table"),
                    cli.property("naaccr.extract-table"),
            )
        } else if (cli.flag('summary')) {
            work = new NAACCR_Summary(cdw, task_id,
                    flat_file(),
                    cli.property("naaccr.records-table"),
                    cli.property("naaccr.extract-table"),
                    cli.property("naaccr.stats-table"),
            )
        } else if (cli.flag('tumors')) {
            work = new NAACCR_Visits(cdw, flat_file(), task_id, 2000000)
        } else if (cli.flag('facts')) {
            work = new NAACCR_Facts(cdw, flat_file(), task_id)
        } else if (cli.flag('patients')) {
            work = new NAACCR_Patients(cdw, flat_file(), task_id)
        } else if (cli.flag('ontology') || cli.flag('import')) {
            TumorOnt.run_cli(cli)
        } else {
            Loader.run_cli(cli)
        }

        if (work && !work.complete()) {
            work.run()
        }
    }

    static class NAACCR_Records implements Task {
        static int batchSize = 1000
        private final DBConfig cdw
        private final URL flat_file
        final String table_name

        NAACCR_Records(DBConfig cdw, URL flat_file, String table) {
            this.cdw = cdw
            this.flat_file = flat_file
            this.table_name = table
        }

        boolean complete() {
            boolean done = false
            cdw.withSql { Sql sql ->
                try {
                    final row = sql.firstRow("select count(*) from ${table_name}" as String)
                    if (row == null || row.size() == 0) {
                        return null
                    }
                    int rowCount = row[0] as int
                    if (rowCount > 0) {
                        log.info("complete: table ${table_name} already has ${rowCount} records.")
                        done = true
                    }
                } catch (SQLException problem) {
                    log.warn("not complete: $problem")
                }
                null
            }
            done
        }

        void run() {
            String stmt = "insert into ${table_name} (line, record) values (?, ?)"
            log.info("loading ${flat_file}: $stmt")
            flat_file.withInputStream { InputStream naaccr_text_lines ->
                cdw.withSql { Sql sql ->
                    try {
                        sql.execute("create table ${table_name} (line int, record clob)" as String)
                    } catch (SQLException problem) {
                        log.warn("cannot create ${table_name}: ${problem}")
                    }
                    int line = 0
                    sql.withBatch(batchSize, stmt) { BatchingPreparedStatementWrapper ps ->
                        new Scanner(naaccr_text_lines).useDelimiter("\r\n|\n") each { String record ->
                            line += 1
                            ps.addBatch([line as Object, record as Object])
                        }
                    }
                    log.info("inserted ${line} records into $table_name")
                }
            }
        }

    }

    private static class TableBuilder {
        String table_name
        String task_id

        boolean complete(DBConfig account) {
            boolean done = false
            account.withSql { Sql sql ->
                try {
                    final row = sql.firstRow("select 1 from ${table_name} where task_id = ?.task_id",
                            [task_id: task_id])
                    if (row != null && row[0] == 1) {
                        log.info("complete: $task_id")
                        done = true
                    }
                } catch (SQLException problem) {
                    log.warn("not complete: $problem")
                }
                null
            }
            done
        }

        void build(Sql sql, Table data) {
            data.addColumns(constS('task_id', data, task_id))
            try {
                sql.execute("drop table ${table_name}".toString())
            } catch (SQLException ignored) {
            }
            // TODO: case fold?
            load_data_frame(sql, table_name, data)
            log.info("inserted ${data.rowCount()} rows into ${table_name}")
        }
    }

    private static StringColumn constS(String name, Table t, String val) {
        StringColumn.create(name, [val] * t.rowCount() as String[])
    }

    static class NAACCR_Summary implements Task {
        final TableBuilder tb
        final DBConfig cdw
        final NAACCR_Extract extract_task

        NAACCR_Summary(DBConfig cdw, String task_id,
                       URL flat_file = null, String records_table, String extract_table, String stats_table) {
            tb = new TableBuilder(task_id: task_id, table_name: stats_table)
            this.cdw = cdw
            extract_task = new NAACCR_Extract(cdw, task_id, flat_file, records_table, extract_table)
        }

        // TODO: factor out common parts of NAACCR_Summary, NAACCR_Visits as TableBuilder a la python?
        boolean complete() { tb.complete(cdw) }

        void run() {
            final DBConfig mem = DBConfig.inMemoryDB("Stats")

            final Table extract = extract_task.results()

            Table data = mem.withSql { Sql memdb ->
                DataSummary.stats(extract, memdb)
            }
            cdw.withSql { Sql sql ->
                tb.build(sql, data)
            }
        }
    }

    static class NAACCR_Extract implements Task {
        final TableBuilder tb
        final DBConfig cdw
        final URL flat_file
        final String records_table

        NAACCR_Extract(DBConfig cdw, String task_id, URL flat_file = null, String records_table, String extract_table) {
            this.cdw = cdw
            tb = new TableBuilder(task_id: task_id, table_name: extract_table)
            this.flat_file = flat_file
            this.records_table = records_table
        }

        @Override
        boolean complete() { tb.complete(cdw) }

        /**
         * avoid ORA-00972: identifier is too long
         * @return records, after mutating column names
         */
        static Table to_db_ids(Table records,
                               int max_length = 30,
                               int max_digits = 7) {
            final Map<String, Integer> byId = ddictDF().iterator().collectEntries { Row row ->
                [(row.getString('naaccrId')): row.getInt('naaccrNum')]
            }
            records.columns().forEach { Column column ->
                final String naaccrId = column.name()
                final naaccrNum = byId[naaccrId]
                final String slug = naaccrId.substring(0, Math.min(naaccrId.length(), max_length - max_digits - 1))
                column.setName("${slug.toUpperCase()}_${naaccrNum}")
            }
            records
        }

        static Table from_db_ids(Table records,
                                 int max_length = 30,
                                 int max_digits = 7) {
            final Map<String, String> toId = ddictDF().iterator().collectEntries { Row row ->
                String naaccrId = row.getString('naaccrId')
                int naaccrNum = row.getInt('naaccrNum')
                final String slug = naaccrId.substring(0, Math.min(naaccrId.length(), max_length - max_digits - 1))
                [("${slug.toUpperCase()}_${naaccrNum}" as String): naaccrId]
            }
            records.columns().forEach { Column column ->
                String naaccrId = toId[column.name()]
                if (naaccrId != null) {
                    column.setName(toId[column.name()])
                }
            }
            records
        }

        Table results() {
            if (!complete()) {
                run()
            }
            cdw.withSql { Sql sql ->
                Table from_db = null
                sql.query("select * from ${tb.table_name}" as String) { ResultSet results ->
                    from_db = Table.read().db(results)
                }
                from_db_ids(from_db)
            }
        }

        @Override
        void run() {
            final Table dd = ddictDF()
            Table extract = withRecords() { Reader naaccr_text_lines ->
                log.info("extracting discrete data from ${records_table} to ${tb.table_name}")
                read_fwf(naaccr_text_lines, dd.collect { it.getString('naaccrId') })
            }
            cdw.withSql { Sql sql ->
                log.info("inserting ${extract.rowCount()} records into ${tb.table_name}")
                tb.build(sql, to_db_ids(extract))
            }
        }

        def <V> V withRecords(Closure<V> thunk) {
            V result
            if (flat_file == null) {
                log.info("reading records from table ${records_table}")
                result = cdw.withSql { Sql sql ->
                    withClobReader(sql, "select record from $records_table order by line" as String) { Reader lines ->
                        thunk(lines)
                    }
                }
            } else {
                log.info("reading records from ${flat_file}")
                result = thunk(new InputStreamReader(flat_file.openStream()))
            }
            result
        }
    }

    /** Make a per-tumor table for use in encounter_mapping etc.
     */
    static class NAACCR_Visits implements Task {
        static final String table_name = "NAACCR_TUMORS"
        int encounter_num_start

        final TableBuilder tb
        private final DBConfig cdw
        private final URL flat_file

        NAACCR_Visits(DBConfig _cdw, URL _flat_file, String _task_id, int start) {
            tb = new TableBuilder(task_id: _task_id, table_name: table_name)
            flat_file = _flat_file
            cdw = _cdw
            encounter_num_start = start
        }

        boolean complete() { tb.complete(cdw) }

        void run() {
            Table tumors = _data(flat_file, encounter_num_start)
            cdw.withSql { Sql sql ->
                tb.build(sql, tumors)
            }
        }

        static Table _data(URL flat_file, int encounter_num_start) {
            Reader naaccr_text_lines = new InputStreamReader(flat_file.openStream()) // TODO try, close
            Table tumors = TumorKeys.with_tumor_id(
                    TumorKeys.pat_tmr(naaccr_text_lines))
            tumors = TumorKeys.with_rownum(
                    tumors, encounter_num_start)
            tumors
        }
    }

    /** Make a per-patient table for use in patient_mapping etc.
     */
    static class NAACCR_Patients implements Task {
        static String table_name = "NAACCR_PATIENTS"
        static String patient_ide_source = 'SMS@kumed.com'
        static String schema = 'NIGHTHERONDATA'

        final TableBuilder tb
        private final DBConfig cdw
        private final URL flat_file

        NAACCR_Patients(DBConfig _cdw, URL _flat_file, String _task_id) {
            tb = new TableBuilder(task_id: _task_id, table_name: table_name)
            flat_file = _flat_file
            cdw = _cdw
        }

        boolean complete() { tb.complete(cdw) }

        void run() {
            cdw.withSql { Sql sql ->
                Table patients = _data(sql, flat_file)
                tb.build(sql, patients)
            }
        }

        static Table _data(Sql cdwdb, URL flat_file) {
            Reader naaccr_text_lines = new InputStreamReader(flat_file.openStream()) // TODO try, close
            Table patients = TumorKeys.patients(naaccr_text_lines)
            TumorKeys.with_patient_num(patients, cdwdb, schema, patient_ide_source)
        }
    }

    static class NAACCR_Facts implements Task {
        static final String table_name = "NAACCR_OBSERVATIONS"

        final TableBuilder tb
        private final DBConfig cdw
        private final URL flat_file

        NAACCR_Facts(DBConfig _cdw, URL _flat_file, String _task_id) {
            tb = new TableBuilder(task_id: _task_id, table_name: table_name)
            flat_file = _flat_file
            cdw = _cdw
        }

        boolean complete() { tb.complete(cdw) }

        void run() {
            final DBConfig mem = DBConfig.inMemoryDB("Facts")
            Table data = null
            mem.withSql { Sql memdb ->
                Reader naaccr_text_lines = new InputStreamReader(flat_file.openStream())
                data = _data(memdb, naaccr_text_lines)
            }

            cdw.withSql { Sql sql ->
                tb.build(sql, data)
            }
        }

        static Table _data(Sql sql, Reader naaccr_text_lines) {
            final Table dd = ddictDF()
            final Table extract = read_fwf(naaccr_text_lines, dd.collect { it.getString('naaccrId') })
            Table item = ItemObs.make(sql, extract)
            // TODO: Table seer = SEER_Recode.make(sql, extract)
            // TODO: Table ssf = SiteSpecificFactors.make(sql, extract)
            // TODO: item.append(seer).append(ssf)
            item
        }
    }

    static class ItemObs {
        static final TumorOnt.SqlScript script = new TumorOnt.SqlScript('naaccr_txform.sql',
                TumorOnt.resourceText('heron_load/naaccr_txform.sql'),
                [
                        new Tuple2('tumor_item_value', ['naaccr_obs_raw', 'tumor_item_type']),
                        new Tuple2('tumor_reg_facts', ['record_layout', 'section']),
                ])

        static Table make(Sql sql, Table extract) {
            final item_ty = TumorOnt.NAACCR_I2B2.tumor_item_type

            Table raw_obs = DataSummary.stack_obs(extract, item_ty, TumorKeys.key4 + TumorKeys.dtcols)
            raw_obs = naaccr_dates(raw_obs, TumorKeys.dtcols)
            DBConfig.parseDateExInstall(sql)

            final views = TumorOnt.create_objects(
                    sql, script, [
                    naaccr_obs_raw : raw_obs,
                    tumor_item_type: item_ty,
                    record_layout  : record_layout,
                    section        : TumorOnt.NAACCR_I2B2.per_section,
            ])

            return views.values().last()(sql)
        }
    }

    static long _stable_hash(String text) {
        final CRC32 out = new CRC32()
        final byte[] bs = text.getBytes('UTF-8')
        out.update(bs, 0, bs.size())
        out.value

    }

    /**
     * @param max_length - avoid ORA-00972: identifier is too long
     */
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

    static Table read_fwf(Reader lines, List<String> items) {
        Table data = Table.create(items.collect { it -> StringColumn.create(it) }
                as Collection<Column<?>>)
        PatientReader reader = new PatientFlatReader(lines)
        Patient patient = reader.readPatient()
        while (patient != null) {
            patient.getTumors().each { Tumor tumor ->
                Row tumorRow = data.appendRow()
                patient.items.each { Item item ->
                    if (items.contains(item.naaccrId)) {
                        tumorRow.setString(item.naaccrId, item.value)
                    }
                }
                tumor.items.each { Item item ->
                    if (items.contains(item.naaccrId)) {
                        tumorRow.setString(item.naaccrId, item.value)
                    }
                }
                // TODO: get items from NAACCR Data?
            }
            patient = reader.readPatient()
        }
        data
    }

    static class TumorKeys {
        static List<String> pat_ids = ['patientSystemIdHosp', 'patientIdNumber']
        static List<String> pat_attrs = pat_ids + ['dateOfBirth', 'dateOfLastContact', 'sex', 'vitalStatus']
        static final List<String> tmr_ids = ['tumorRecordNumber']
        static final List<String> tmr_attrs = tmr_ids + [
                'dateOfDiagnosis',
                'sequenceNumberCentral', 'sequenceNumberHospital', 'primarySite',
                'ageAtDiagnosis', 'dateOfInptAdm', 'dateOfInptDisch', 'classOfCase',
                'dateCaseInitiated', 'dateCaseCompleted', 'dateCaseLastChanged',
        ]
        static List<String> report_ids = ['naaccrRecordVersion', 'npiRegistryId']
        static List<String> report_attrs = report_ids + ['dateCaseReportExported']
        static List<String> dtcols = ['dateOfBirth', 'dateOfDiagnosis', 'dateOfLastContact',
                                      'dateCaseCompleted', 'dateCaseLastChanged']
        static List<String> key4 = [
                'patientSystemIdHosp',  // NAACCR stable patient ID
                'tumorRecordNumber',    // NAACCR stable tumor ID
                'patientIdNumber',      // patient_mapping
                'abstractedBy',         // IDEA/YAGNI?: provider_id
        ]

        static Table pat_tmr(Reader naaccr_text_lines) {
            _pick_cols(tmr_attrs + pat_attrs + report_attrs, naaccr_text_lines)
        }

        static Table patients(Reader naaccr_text_lines) {
            _pick_cols(pat_attrs + report_attrs, naaccr_text_lines)
        }

        private static Table _pick_cols(List<String> attrs, Reader lines) {
            Table pat_tmr = Table.create(attrs.collect { it -> StringColumn.create(it) }
                    as Collection<Column<?>>)
            PatientReader reader = new PatientFlatReader(lines)
            Patient patient = reader.readPatient()
            while (patient != null) {
                Row patientRow = pat_tmr.appendRow()
                attrs.each { String naaccrId ->
                    patientRow.setString(naaccrId, patient.getItemValue(naaccrId))
                }
                patient = reader.readPatient()
            }
            pat_tmr = naaccr_dates(pat_tmr, pat_tmr.columnNames().findAll { it.startsWith('date') })
            pat_tmr
        }

        static Table with_tumor_id(Table data,
                                   String name = 'recordId',
                                   List<String> extra = ['dateOfDiagnosis',
                                                         'dateCaseCompleted'],
                                   // keep recordId length consistent
                                   String extra_default = null) {
            // ISSUE: performance: add encounter_num column here?
            if (extra_default == null) {
                extra_default = '0000-00-00'
            }
            StringColumn id_col = data.stringColumn('patientIdNumber')
                    .join('', data.stringColumn('tumorRecordNumber'))
            extra.forEach { String it ->
                StringColumn sc = data.column(it).copy().asStringColumn()
                sc.set((sc as StringFilters).isMissing(), extra_default)
                id_col = id_col.join('', sc)
            }
            data = data.copy()
            data.addColumns(id_col.setName(name))
            data
        }

        static Table with_rownum(Table tumors, int start,
                                 String new_col = 'encounter_num',
                                 String key_col = 'recordId') {
            tumors.sortOn(key_col)
            tumors.addColumns(IntColumn.indexColumn(new_col, tumors.rowCount(), start))
            tumors
        }

        static void export_patient_ids(Table df, Sql cdw,
                                       String tmp_table = 'NAACCR_PMAP',
                                       String id_col = 'patientIdNumber') {
            log.info("writing $id_col to $tmp_table")
            Table pat_ids = df.select(id_col).dropDuplicateRows()
            load_data_frame(cdw, tmp_table, pat_ids)
        }

        static Table with_patient_num(Table df, Sql cdw, String schema,
                                      String source,
                                      String tmp_table = 'NAACCR_PMAP',
                                      String id_col = 'patientIdNumber') {
            export_patient_ids(df, cdw, tmp_table, id_col)
            Table src_map = null
            cdw.query("""(
                select ea."${id_col}", pmap.PATIENT_NUM as "patient_num"
                from ${tmp_table} ea
                join ${schema}.PATIENT_MAPPING pmap
                on pmap.patient_ide_source = ?.source
                and ltrim(pmap.patient_ide, '0') = ltrim(ea."${id_col}", '0')
                )""", [source: source]) { ResultSet results ->
                src_map = Table.read().db(results)
            }
            Table out = df.joinOn(id_col).leftOuter(src_map)
            out.removeColumns(id_col)
            out
        }
    }

    static <V> V withClobReader(Sql sql, String query, Closure<V> thunk) {
        final PipedOutputStream wr = new PipedOutputStream()
        PipedInputStream rd = new PipedInputStream(wr)
        Reader lines = new InputStreamReader(rd)
        byte CR = 13
        byte LF = 10

        Thread worker = new Thread({ ->
            try {
                sql.eachRow(query) { row ->
                    // println("clob length: ${(row.getClob(1)).length()}")
                    Clob text = row.getClob(1)
                    String str = text.getSubString(1L, text.length() as int)
                    wr.write(str.getBytes(Charset.forName("UTF-8")))
                    wr.write(CR)
                    wr.write(LF)
                }
            } finally {
                wr.close()
            }
        })
        worker.start()
        V result = null
        try {
            result = thunk(lines)
        } finally {
            lines.close()
            worker.join()
        }
        result
    }

    static final FixedColumnsLayout layout18 = LayoutFactory.getLayout(LayoutFactory.LAYOUT_ID_NAACCR_18_INCIDENCE) as FixedColumnsLayout
    static final Table record_layout = TumorOnt.fromRecords(
            layout18.getAllFields().collect { FixedColumnsField it ->
                [('long-label')     : it.longLabel,
                 start              : it.start,
                 length             : it.length,
                 ('naaccr-item-num'): it.naaccrItemNum,
                 section            : it.section,
                 grouped            : it.subFields != null && it.subFields.size() > 0
                ] as Map
            })

    static Table naaccr_dates(Table df, List<String> date_cols,
                              boolean keep = false) {
        final orig_cols = df.columnNames()
        for (String dtname : date_cols) {
            final strname = dtname + '_'
            final StringColumn strcol = df.column(dtname).copy().asStringColumn().setName(strname)
            df = df.replaceColumn(dtname, strcol)
            df.addColumns(naaccr_date_col(strcol).setName(dtname))
        }
        if (!keep) {
            // ISSUE: df.select(*orig_cols) uses spread which doesn't work with CompileStatic
            df = Table.create(orig_cols.collect { String it -> df.column(it) })
        }
        df
    }

    static DateColumn naaccr_date_col(StringColumn sc) {
        String name = sc.name()
        sc = sc.trim().concatenate('01019999').substring(0, 8)
        // type of StringColumn.map(fun, creator) is too fancy for groovy CompileStatic
        final data = sc.asList().collect { String txt ->
            LocalDate value
            try {
                value = LocalDate.parse(txt, DateTimeFormatter.BASIC_ISO_DATE) // 'yyyyMMdd'
            } catch (DateTimeParseException ignored) {
                value = null
            }
            value
        }
        DateColumn.create(name, data)
    }

    static class DataSummary {
        static final TumorOnt.SqlScript script = new TumorOnt.SqlScript('data_char_sim.sql',
                TumorOnt.resourceText('heron_load/data_char_sim.sql'),
                [new Tuple2('data_agg_naaccr', ['naaccr_extract', 'tumors_eav', 'tumor_item_type']),
                 new Tuple2('data_char_naaccr', ['record_layout'])])

        static Table stats(Table tumors_raw, Sql sql) {
            final Table ty = TumorOnt.NAACCR_I2B2.tumor_item_type
            final Table tumors = naaccr_dates(tumors_raw, ['dateOfDiagnosis'], false)
            final Map<String, Closure<Table>> views = TumorOnt.create_objects(sql, script, [
                    section        : TumorOnt.NAACCR_I2B2.per_section,
                    naaccr_extract : tumors,
                    record_layout  : record_layout,
                    tumor_item_type: ty,
                    tumors_eav     : stack_obs(tumors, ty, ['dateOfDiagnosis']),
            ])
            Table out = views.values().last()(sql)
            DoubleColumn sd = out.doubleColumn('sd')
            sd.set((sd as NumericColumn).isMissing(), 0 as Double)
            out
        }

        static Table stack_obs(Table data, Table ty,
                               List<String> id_vars = [],
                               List<String> valtype_cds = ['@', 'D', 'N'],
                               String var_name = 'naaccrId',
                               String id_col = 'recordId') {
            final StringColumn value_vars = ty
                    .where(ty.stringColumn('valtype_cd').isIn(valtype_cds)
                            & ty.stringColumn('naaccrId').isNotIn(id_vars)).stringColumn('naaccrId')
            data = data.copy().addColumns(IntColumn.indexColumn(id_col, data.rowCount(), 0))
            final Table df = melt(data, value_vars.asList(), [id_col] + id_vars, var_name)
            df.where(df.stringColumn('value').isLongerThan(0))
        }

        static Table melt(Table data, List<String> value_vars, List<String> id_vars,
                          String var_name, String value_col = 'value') {
            final Table entity = Table.create(id_vars.collect { String it -> data.column(it) })
            final List<Column> dataCols = value_vars.collect { String it -> data.column(it) }
            Table out = null
            dataCols.forEach { Column valueColumn ->
                Table slice = entity.copy()
                StringColumn attribute = StringColumn.create(var_name, [valueColumn.name()] * data.rowCount())
                slice.addColumns(attribute, valueColumn.copy().setName(value_col))
                if (out == null) {
                    out = slice
                } else {
                    out = out.append(slice)
                }
            }
            out
        }
    }
}
