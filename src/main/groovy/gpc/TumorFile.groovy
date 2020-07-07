package gpc

import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.record.fixed.FixedColumnsField
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import com.imsweb.naaccrxml.PatientFlatReader
import com.imsweb.naaccrxml.PatientReader
import com.imsweb.naaccrxml.entity.Item
import com.imsweb.naaccrxml.entity.Patient
import com.imsweb.naaccrxml.entity.Tumor
import gpc.DBConfig.Task
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
import groovy.text.SimpleTemplateEngine
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
        DBConfig.CLI cli
        Closure<Properties> getProps = { String name ->
            Properties ps = new Properties()
            new File(name).withInputStream { ps.load(it) }
            if (ps.containsKey('db.passkey')) {
                ps.setProperty('db.password', cli.mustGetEnv(ps.getProperty('db.passkey')))
            }
            ps
        }
        cli = new DBConfig.CLI(new Docopt(usage).parse(args),
                { String name -> getProps(name) },
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
        } else if (cli.flag('load-files')) {
            cli.files('NAACCR_FILE').each { URL naaccr_file ->
                //noinspection GrReassignedInClosureLocalVar
                work = new NAACCR_Records(cdw, naaccr_file, cli.property("naaccr.records-table"))
                if (work && !work.complete()) {
                    work.run()
                }
            }
            return
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
            work = new NAACCR_Visits(cdw, task_id,
                    flat_file(),
                    cli.property("naaccr.records-table"),
                    cli.property("naaccr.extract-table"),
                    2000000)
        } else if (cli.flag('facts')) {
            work = new NAACCR_Facts(cdw, task_id,
                    flat_file(),
                    cli.property("naaccr.records-table"),
                    cli.property("naaccr.extract-table"))
        } else if (cli.flag('patients')) {
            work = new NAACCR_Patients(cdw, task_id,
                    flat_file(),
                    cli.property("naaccr.records-table"),
                    cli.property("naaccr.extract-table"))
            TumorOnt.run_cli(cli)
        } else if (cli.flag('load') || cli.flag('run') || cli.flag('query')) {
            Loader.run_cli(cli)
        } else {
            TumorOnt.run_cli(cli)
        }

        if (work && !work.complete()) {
            work.run()
        }
    }

    static int line_count(URL input) {
        int count = 0
        input.withInputStream { InputStream lines ->
            new Scanner(lines).useDelimiter("\r\n|\n") each { String it ->
                count += 1
            }
        }
        count
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
            int lines = line_count(flat_file)
            final source_cd = new File(flat_file.path).name
            cdw.withSql { Sql sql ->
                try {
                    final row = sql.firstRow("select count(*) from ${table_name} where source_cd = :source_cd" as String,
                            [source_cd: source_cd])
                    if (row == null || row.size() == 0) {
                        return null
                    }
                    int rowCount = row[0] as int
                    if (rowCount >= lines) {
                        log.info("complete: table ${table_name} already has ${rowCount} records from ${source_cd}.")
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
            final source_cd = new File(flat_file.path).name
            String stmt = "insert into ${table_name} (source_cd, encounter_num, observation_blob) values (?, ?, ?)"
            log.info("loading ${flat_file}: $stmt")
            flat_file.withInputStream { InputStream naaccr_text_lines ->
                cdw.withSql { Sql sql ->
                    try {
                        sql.execute("create table ${table_name} (source_cd varchar(50), encounter_num int, observation_blob clob)" as String)
                    } catch (SQLException problem) {
                        log.warn("cannot create ${table_name}: ${problem}")
                    }
                    int encounter_num = 0
                    sql.withBatch(batchSize, stmt) { BatchingPreparedStatementWrapper ps ->
                        new Scanner(naaccr_text_lines).useDelimiter("\r\n|\n") each { String observation_blob ->
                            encounter_num += 1
                            ps.addBatch([source_cd as Object, encounter_num as Object, observation_blob as Object])
                        }
                    }
                    log.info("inserted ${encounter_num} records into $table_name")
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
                    final row = sql.firstRow("select count(*) from ${table_name} where task_id = ?.task_id",
                            [task_id: task_id])
                    if (row != null && (row[0] as int) >= 1) {
                        log.info("complete: ${row[0]} rows with task_id = $task_id")
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
            drop(sql)
            load_data_frame(sql, table_name, data)
            log.info("inserted ${data.rowCount()} rows into ${table_name}")
        }

        void drop(Sql sql) {
            try {
                sql.execute("drop table ${table_name}".toString())
            } catch (SQLException ignored) {
                // TODO: distinguish "not found" from others, esp. "because OBJ2 depends on it"
            }
        }

        void reset(Sql sql, Table data) {
            drop(sql)
            log.debug("creating table ${table_name}")
            sql.execute(TumorOnt.SqlScript.create_ddl(table_name, data.columns()))
        }

        void appendChunk(Sql sql, Table data) {
            TumorOnt.append_data_frame(data, table_name, sql)
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

        boolean complete() { tb.complete(cdw) }

        void run() {
            cdw.withSql { Sql sql ->
                final loadTable = { String name, Table data -> load_data_frame(sql, name.toUpperCase(), data, true) }
                loadTable('section', TumorOnt.NAACCR_I2B2.per_section)
                loadTable('record_layout', record_layout)
                loadTable('tumor_item_type', TumorOnt.NAACCR_I2B2.tumor_item_type)
                Loader ld = new Loader(sql)
                URL script = TumorFile.getResource('heron_load/data_char_sim.sql')
                ld.runScript(script)
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
        static Table to_db_ids(Table records) {
            final Map<String, String> byId = TumorOnt.fields().iterator().collectEntries { Row row ->
                [(row.getString('naaccrId')): row.getString('FIELD_NAME')]
            }
            records.columns().findAll { byId[it.name()] == null }.forEach { Column missingId ->
                records.removeColumns(missingId)
            }
            records.columns().forEach { Column column -> column.setName(byId[column.name()]) }
            records
        }

        static Table from_db_ids(Table records) {
            final Map<String, String> toId = TumorOnt.fields().iterator().collectEntries { Row row ->
                [(row.getString('FIELD_NAME')): row.getString('naaccrId')]
            }
            records.columns().forEach { Column column -> column.setName(toId[column.name()]) }
            records
        }

        void runTooSlowToo() {
            // TODO: support other NAACCR file versions
            final FixedColumnsLayout v18 = LayoutFactory.getLayout(LayoutFactory.LAYOUT_ID_NAACCR_18_INCIDENCE) as FixedColumnsLayout
            // TODO: non-Oracle databases
            // TODO: support SUBSTRING vs. SUBSTR
            def substr = "SUBSTR"

            final parts = layoutToSQL(v18, tb.table_name, "OBSERVATION_BLOB", "TASK_ID", substr)
            def (create, colNames, exprs) = [parts[0], parts[1], parts[2]]
            cdw.withSql { Sql sql ->
                tb.drop(sql)
                log.info("creating ${tb.table_name}")
                sql.execute(create)

                def offset = 0
                final limit = 1000

                do {
                    final pg = pageStatement(cdw.url, records_table, offset, limit)
                    log.info("inserting <= ${limit} rows at ${offset} into ${tb.table_name}...")
                    if ((sql.firstRow("select count(*) from (${pg})".toString())[0] as int) <= 0) {
                        break
                    }
                    sql.execute("insert /*+ append */ into ${tb.table_name} ($colNames) select ${exprs} from (${pg}) S".toString())
                    offset += limit
                } while (1);
                // only fill in task_id after all rows are done
                log.info("updating task_id in ${tb.table_name}")
                sql.execute("update ${tb.table_name} set task_id = ?.task_id",
                        [task_id: tb.task_id])
            }
        }

        @Override
        void run() {
            cdw.withSql { Sql sql ->
                TumorFile.NAACCR_Extract.loadFlatFile(
                        sql, new File(flat_file.path), tb.table_name, tb.task_id, TumorOnt.pcornet_fields)
            }
        }

        static int loadFlatFile(Sql sql, File flat_file, String table_name, String task_id, Table fields,
                                String varchar = "VARCHAR2", int batchSize = 64) {
            FixedColumnsLayout layout = theLayout(flat_file)
            final source_cd = flat_file.name

            final colInfo = columnInfo(fields, layout, varchar)
            String ddl = """
            create table ${table_name} (
              source_cd varchar(50),
              encounter_num int,
              task_id ${varchar}(1024),
              ${colInfo.collect { it.colDef }.join(",\n  ")},
              observation_blob clob
            )
            """

            String dml = """
            insert into ${table_name} (
              source_cd, encounter_num, observation_blob,
              ${colInfo.collect { it.name }.join(',\n  ')})
            values (:source_cd, :encounter_num, :observation_blob, ${colInfo.collect { it.param }.join('\n,  ')})
            """
            int encounter_num = 0
            flat_file.withInputStream { InputStream naaccr_text_lines ->
                dropIfExists(sql, table_name)
                sql.execute(ddl)
                sql.withBatch(batchSize, dml) { BatchingPreparedStatementWrapper ps ->
                    new Scanner(naaccr_text_lines).useDelimiter("\r\n|\n") each { String line ->
                        encounter_num += 1
                        Map<String, Object> record = colInfo.collectEntries {
                            final start = it.start as int
                            final length = it.length as int
                            [it.name, line.substring(start, start + length)]
                        }
                        ps.addBatch([
                                source_cd       : source_cd as Object,
                                encounter_num   : encounter_num as Object,
                                observation_blob: line as Object
                        ] + record)
                        if (encounter_num % 1000 == 0) {
                            log.info('inserted {} records', encounter_num)
                        }
                    }
                }
                log.info("inserted ${encounter_num} records into $table_name")
            }
            // only fill in task_id after all rows are done
            log.info("updating task_id in ${table_name}")
            sql.execute("update ${table_name} set task_id = ?.task_id",
                    [task_id: task_id])

            encounter_num
        }

        static boolean dropIfExists(Sql sql, String table_name) {
            final results = sql.connection.getMetaData().getTables(null, null, table_name, null);
            if (results.next())
            {
                sql.execute("drop table ${table_name}" as String)
                return true
            }
            return false
        }

        static FixedColumnsLayout theLayout(File flat_file) {
            final layouts = LayoutFactory.discoverFormat(flat_file)
            if (layouts.size() < 1) {
                throw new RuntimeException("cannot discover format of ${flat_file}")
            } else if (layouts.size() > 1) {
                throw new RuntimeException("ambiguous format: ${flat_file}: ${layouts.collect { it.layoutId }}.join(',')")
            }
            final layout = LayoutFactory.getLayout(layouts[0].layoutId) as FixedColumnsLayout
            layout
        }

        static public List<Map> columnInfo(Table fields, FixedColumnsLayout layout, String varchar) {
            fields.collect {
                final num = it.getInt("item")
                final name = it.getString("FIELD_NAME")
                final item = layout.getFieldByNaaccrItemNumber(num)
                [num: num, name: name, item: item]
            }.findAll {
                if (it.item == null) {
                    log.warn("item not found in ${layout.layoutId}: ${it.num} ${it.name}")
                }
                it.item != null
            }.collect {
                final item = it.item as FixedColumnsField
                [
                        name  : it.name,
                        start : item.start,
                        length: item.length,
                        colDef: "${it.name} ${varchar}(${item.length}) ",
                        param : ":${it.name}",
                ]
            } as List<Map>
        }

        static String pageStatement(String dbURL, String src, int offset, int limit) {
            def h2SQL = '''
select *
from ${src}
order by source_cd, encounter_num
limit ${limit} offset ${offset}
'''

            def oraSQL = '''
select *
from (select rownum as rn, s.*
    from (select *
        from ${src}
        order by source_cd, encounter_num) s
    where rownum <= ${offset + limit}
)
where rn > ${offset}'''

            def template = new SimpleTemplateEngine().createTemplate(dbURL.contains("ora") ? oraSQL : h2SQL)

            def binding = ["offset": offset, "limit": limit, "src": src]
            final txt = template.make(binding).toString()
            txt
        }

        void runRillyRillySlowly() {
            boolean firstChunk = true
            def colNames = (TumorOnt.fields()
                    .iterator()
                    .collect { it.getString('FIELD_NAME') }
            ) + ['task_id']
            Table blank = Table.create(colNames.collect { StringColumn.create(it) } as Collection<Column>)
            cdw.withSql { Sql sql ->
                withRecords() { Reader naaccr_text_lines ->
                    log.info("extracting discrete data from ${records_table} to ${tb.table_name}")
                    read_fwf(naaccr_text_lines) { Table extract ->
                        Table chunk = to_db_ids(extract)
                        if (firstChunk) {
                            tb.reset(sql, blank)
                            firstChunk = false
                        }
                        log.info("inserting ${chunk.rowCount()} records into ${tb.table_name}")
                        tb.appendChunk(sql, chunk)
                    }
                }
                // only fill in task_id after all rows are done
                sql.execute("update ${tb.table_name} set task_id = ?.task_id",
                        [task_id: tb.task_id])
            }
        }

        def <V> V withRecords(Closure<V> thunk) {
            V result
            if (flat_file == null) {
                log.info("reading records from table ${records_table}")
                result = cdw.withSql { Sql sql ->
                    String select_blobs = "select observation_blob from $records_table order by source_cd, encounter_num"
                    withClobReader(sql, select_blobs) { Reader lines ->
                        thunk(lines)
                    }
                }
            } else {
                log.info("reading records from ${flat_file}")
                result = thunk(new InputStreamReader(flat_file.openStream()))
            }
            result
        }

        static List<String> layoutToSQL(FixedColumnsLayout layout, String dest, String clob,
                                        String taskCol = "TASK_ID",
                                        String substr = "SUBSTR",
                                        String varchar = "VARCHAR2") {
            final Map<Integer, String> itemToCol = TumorOnt.fields()
                    .collectEntries {
                        [it.getInt("naaccrNum"), it.getString("FIELD_NAME")]
                    }
            final keys = itemToCol.keySet()
            final items = layout.allFields.findAll { keys.contains(it.naaccrItemNum) }
            final colDefs = items.collect {
                "${itemToCol[it.naaccrItemNum]} ${varchar}(${it.length})"
            }.join(',\n  ')
            final insertCols = items.collect { itemToCol[it.naaccrItemNum] }.join(",\n  ")
            final selectCols = items.collect {
                "TRIM(${substr}(S.${clob}, ${it.start}, ${it.length})) as ${itemToCol[it.naaccrItemNum]}"
            }.join(",\n  ")
            [
                    "create table ${dest} (${colDefs},\n  ${taskCol} ${varchar}(1024))",
                    insertCols,
                    selectCols
            ] as List<String>
        }
    }


    /** Make a per-tumor table for use in encounter_mapping etc.
     */
    static class NAACCR_Visits implements Task {
        static final String table_name = "NAACCR_TUMORS"
        int encounter_num_start

        final TableBuilder tb
        private final DBConfig cdw
        private final NAACCR_Extract extract_task

        NAACCR_Visits(DBConfig cdw, String task_id, URL flat_file, String records_table, String extract_table, int start) {
            tb = new TableBuilder(task_id: task_id, table_name: table_name)
            this.cdw = cdw
            extract_task = new NAACCR_Extract(cdw, task_id, flat_file, records_table, extract_table)
            encounter_num_start = start
        }

        boolean complete() { tb.complete(cdw) }

        void run() {
            Table tumors = _data(encounter_num_start)
            cdw.withSql { Sql sql ->
                tb.build(sql, tumors)
            }
        }

        Table _data(int encounter_num_start) {
            extract_task.withRecords { Reader naaccr_text_lines ->
                Table tumors = TumorKeys.with_tumor_id(
                        TumorKeys.pat_tmr(naaccr_text_lines))
                tumors = TumorKeys.with_rownum(
                        tumors, encounter_num_start)
                tumors
            }
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
        private final NAACCR_Extract extract_task

        NAACCR_Patients(DBConfig cdw, String task_id, URL flat_file, String records_table, String extract_table) {
            tb = new TableBuilder(task_id: task_id, table_name: table_name)
            this.cdw = cdw
            extract_task = new NAACCR_Extract(cdw, task_id, flat_file, records_table, extract_table)

        }

        boolean complete() { tb.complete(cdw) }

        void run() {
            cdw.withSql { Sql sql ->
                Table patients = _data(sql)
                tb.build(sql, patients)
            }
        }

        Table _data(Sql cdwdb) {
            extract_task.withRecords { Reader naaccr_text_lines ->
                Table patients = TumorKeys.patients(naaccr_text_lines)
                TumorKeys.with_patient_num(patients, cdwdb, schema, patient_ide_source)
            }
        }
    }

    static class NAACCR_Facts implements Task {
        static final String table_name = "NAACCR_OBSERVATIONS"

        final TableBuilder tb
        private final DBConfig cdw
        private final URL flat_file
        private final NAACCR_Extract extract_task

        NAACCR_Facts(DBConfig cdw, String task_id, URL flat_file, String records_table, String extract_table) {
            tb = new TableBuilder(task_id: task_id, table_name: table_name)
            this.flat_file = flat_file
            this.cdw = cdw
            extract_task = new NAACCR_Extract(cdw, task_id, flat_file, records_table, extract_table)
        }

        boolean complete() { tb.complete(cdw) }

        void run() {
            final DBConfig mem = DBConfig.inMemoryDB("Facts")
            boolean firstChunk = true
            cdw.withSql { Sql sql ->
                mem.withSql { Sql memdb ->
                    extract_task.withRecords { Reader naaccr_text_lines ->
                        read_fwf(naaccr_text_lines) { Table extract ->
                            Table item = ItemObs.make(memdb, extract)
                            // TODO: Table seer = SEER_Recode.make(sql, extract)
                            // TODO: Table ssf = SiteSpecificFactors.make(sql, extract)
                            // TODO: item.append(seer).append(ssf)
                            if (firstChunk) {
                                tb.reset(sql, item)
                                firstChunk = false
                            }
                            tb.appendChunk(sql, item)
                            memdb.execute('drop all objects')
                            return null
                        }
                    }
                }
            }
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

    static void read_fwf(Reader lines, Closure<Void> f,
                         int chunkSize = 200) {
        Table empty = Table.create(TumorKeys.required_cols.collect { StringColumn.create(it) }
                as Collection<Column<?>>)
        Table data = empty.copy()
        PatientReader reader = new PatientFlatReader(lines)
        Patient patient = reader.readPatient()

        final ensureCol = { Item item ->
            if (!data.columnNames().contains(item.naaccrId)) {
                data.addColumns(StringColumn.create(item.naaccrId, data.rowCount()))
                empty.addColumns(StringColumn.create(item.naaccrId, 0))
            }
        }

        while (patient != null) {
            patient.getTumors().each { Tumor tumor ->
                if (data.rowCount() >= chunkSize) {
                    f(data)
                    data = empty.copy()
                }
                data.appendRow()
                final consume = { Item it ->
                    if (it.value.trim() > '') {
                        ensureCol(it)
                        data.row(data.rowCount() - 1).setString(it.naaccrId, it.value)
                    }
                }
                patient.items.each consume
                tumor.items.each consume
                reader.getRootData().items.each consume
            }
            patient = reader.readPatient()
        }
        if (data.rowCount() > 0) {
            f(data)
        }
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
        static List<String> required_cols = (pat_attrs + tmr_attrs + report_attrs + key4.drop(3) +
                dtcols.collect { it + 'Flag' })

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
        int progress = 0
        int report_interval = 1000

        Thread worker = new Thread({ ->
            try {
                sql.eachRow(query) { row ->
                    // println("clob length: ${(row.getClob(1)).length()}")
                    Clob text = row.getClob(1)
                    String str = text.getSubString(1L, text.length() as int)
                    wr.write(str.getBytes(Charset.forName("UTF-8")))
                    wr.write(CR)
                    wr.write(LF)
                    progress++
                    if (progress % report_interval == 0) {
                        log.info("processed ${progress} records of: ${query}")
                    }
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
        /* TODO: fix sd col?
        DoubleColumn sd = out.doubleColumn('sd')
        sd.set((sd as NumericColumn).isMissing(), 0 as Double)
        */

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
            final dataColNames = data.columnNames()
            List<String> value_vars_absent = value_vars.findAll { String v -> !dataColNames.contains(v) }
            final value_vars_present = value_vars.findAll { String v -> dataColNames.contains(v) }
            if (value_vars_absent.size() > 0) {
                log.warn("cols missing in melt(): $value_vars_absent")
            }
            final List<Column> dataCols = value_vars_present.collect { String it -> data.column(it) }
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
