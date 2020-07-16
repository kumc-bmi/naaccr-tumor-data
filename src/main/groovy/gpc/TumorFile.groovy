package gpc

import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.record.fixed.FixedColumnsField
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import gpc.DBConfig.Task
import gpc.Tabular.ColumnMeta
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.docopt.Docopt
import tech.tablesaw.api.Row
import tech.tablesaw.api.Table

import javax.annotation.Nullable
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Types
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.zip.CRC32

@CompileStatic
@Slf4j
class TumorFile {
    // see also: buildUsageDoc groovy task
    static final String usageText = TumorFile.getResourceAsStream('usage.txt').text
    static final Docopt docopt = new Docopt(usageText).withExit(false)

    static void main(String[] args) {
        final cwd = Paths.get('')
        final io = [
                exit           : System::exit,
                getConnection  : DriverManager::getConnection,
                resolve        : cwd::resolve,
                fetchProperties: { String name -> getProps(cwd, System.getenv(), name) },
        ] as DBConfig.IO

        final opts = docopt.withExit(true).parse(args)
        run(new DBConfig.CLI(opts, io))
    }

    static Properties getProps(Path cwd, Map<String, String> env, String name) {
        Properties ps = new Properties()
        cwd.resolve(name).toFile().withInputStream { ps.load(it) }
        if (ps.containsKey('db.passkey')) {
            ps.setProperty('db.password', DBConfig.CLI.mustGetEnv(env, ps.getProperty('db.passkey')))
        }
        ps
    }

    static void run(DBConfig.CLI cli) {
        DBConfig cdw = cli.account()

        Task work = null
        String task_id = cli.arg("--task-id", "task123")  // TODO: replace by date, NPI?

        if (cli.flag('tumor-table')) {
            work = new NAACCR_Extract(cdw, task_id,
                    [cli.pathProperty("naaccr.flat-file")],
                    cli.property("naaccr.tumor-table"),
            )
        } else if (cli.flag('tumor-files')) {
            work = new NAACCR_Extract(
                    cdw, task_id,
                    cli.files('NAACCR_FILE'), cli.property("naaccr.tumor-table"))
        } else if (cli.flag('facts')) {
            final upload = new I2B2Upload(
                    cli.property("i2b2.star-schema", null),
                    cli.intArg('--upload-id'),
                    cli.arg('--obs-src'),
                    cli.property("i2b2.template-fact-table", null),
            )
            work = new NAACCR_Facts(cdw,
                    upload,
                    cli.pathProperty("naaccr.flat-file"),
                    cli.property("i2b2.patient-mapping-query"),
                    cli.arg('--mrn-item'),
                    cli.intArg('--encounter-start'),
            )
        } else if (cli.flag('load-layouts')) {
            work = new LoadLayouts(cdw, cli.arg('--layout-table'))
        } else if (cli.flag('run') || cli.flag('query')) {
            Loader.run_cli(cli)
            return
        } else {
            TumorOnt.run_cli(cli)
            return
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

    private static class TableBuilder {
        String table_name
        String task_id

        boolean complete(DBConfig account) {
            boolean done = false
            account.withSql { Sql sql ->
                if (!tableExists(sql, table_name)) {
                    return false
                }

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
    }

    static class NAACCR_Extract implements Task {
        final TableBuilder tb
        final DBConfig cdw
        final List<Path> flat_files

        NAACCR_Extract(DBConfig cdw, String task_id, List<Path> flat_files, String extract_table) {
            this.cdw = cdw
            tb = new TableBuilder(task_id: task_id, table_name: extract_table)
            this.flat_files = flat_files
        }

        @Override
        boolean complete() { tb.complete(cdw) }

        @Override
        void run() {
            Table fields = TumorOnt.pcornet_fields.copy()

            // PCORnet spec doesn't include MRN column, but we need it for patient mapping.
            Row patid = fields.appendRow()
            patid.setInt('item', 20)
            patid.setString('FIELD_NAME', 'PATIENT_ID_NUMBER_N20')

            cdw.withSql { Sql sql ->
                dropIfExists(sql, tb.table_name)
                int encounter_num = 0
                flat_files.eachWithIndex { flat_file, ix ->
                    final create = ix == 0
                    final update = ix == flat_files.size() - 1
                    encounter_num = loadFlatFile(
                            sql, flat_file, tb.table_name, tb.task_id, fields,
                            encounter_num, create, update)
                }
            }
        }

        static int loadFlatFile(Sql sql, Path flat_file, String table_name, String task_id, Table fields,
                                int encounter_num = 0,
                                boolean create = true, boolean update = true,
                                int batchSize = 64) {
            FixedColumnsLayout layout = theLayout(flat_file)
            final source_cd = flat_file.fileName.toString()

            final colInfo = columnInfo(fields, layout)
            final cols = [
                    new ColumnMeta(name: "source_cd", size: 50),
                    new ColumnMeta(name: "encounter_num", dataType: Types.INTEGER),
                    new ColumnMeta(name: "patient_num", dataType: Types.INTEGER),
                    new ColumnMeta(name: "task_id", size: 1024),
            ] + colInfo.collect { it.v2 } + [
                    new ColumnMeta(name: "observation_blob", dataType: Types.CLOB),
            ]
            String ddl = ColumnMeta.createStatement(table_name, cols, ColumnMeta.typeNames(sql.connection))
            String dml = ColumnMeta.insertStatement(table_name, cols)

            flat_file.toFile().withInputStream { InputStream naaccr_text_lines ->
                if (create) {
                    sql.execute(ddl)
                }
                sql.withBatch(batchSize, dml) { BatchingPreparedStatementWrapper ps ->
                    new Scanner(naaccr_text_lines).useDelimiter("\r\n|\n") each { String line ->
                        encounter_num += 1
                        final record = fixedRecord(colInfo, line)
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
            if (update) {
                log.info("updating task_id in ${table_name}")
                sql.execute("update ${table_name} set task_id = ?.task_id",
                        [task_id: task_id])
            }

            encounter_num
        }

        static Map<String, Object> fixedRecord(List<Tuple2<Integer, ColumnMeta>> colInfo, String line) {
            colInfo.collect {
                final start = it.v1 - 1
                final length = it.v2.size
                [it.v2.name, line.substring(start, start + length).trim()]
            }.findAll { (it[1] as String) > '' }.collectEntries { it }
        }

        static FixedColumnsLayout theLayout(Path flat_file) {
            final layouts = LayoutFactory.discoverFormat(flat_file.toFile())
            if (layouts.size() < 1) {
                throw new RuntimeException("cannot discover format of ${flat_file}")
            } else if (layouts.size() > 1) {
                throw new RuntimeException("ambiguous format: ${flat_file}: ${layouts.collect { it.layoutId }}.join(',')")
            }
            final layout = LayoutFactory.getLayout(layouts[0].layoutId) as FixedColumnsLayout
            log.info('{}: layout {}', flat_file.fileName, layout.layoutName)
            layout
        }

        static List<Tuple2<Integer, ColumnMeta>> columnInfo(Table fields, FixedColumnsLayout layout) {
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
                new Tuple2(item.start, new ColumnMeta(name: it.name, size: item.length))
            }
        }
    }

    /**
     * Drop a table using JDBC metadata to check whether it exists first.
     * @param table_qname either schema.name or just name
     * @return true iff the table existed (and hence was dropped)
     */
    static boolean dropIfExists(Sql sql, String table_qname) {
        if (tableExists(sql, table_qname)) {
            sql.execute("drop table ${table_qname}" as String)
        }
        return false
    }

    static boolean tableExists(Sql sql, String table_qname) {
        final parts = table_qname.split('\\.')
        String schema = parts.length == 2 ? parts[0].toUpperCase() : null
        String table_name = parts[-1].toUpperCase()
        final results = sql.connection.getMetaData().getTables(null, schema, table_name, null)
        if (results.next()) {
            return true
        }
        return false
    }

    static class NAACCR_Facts implements Task {
        final TableBuilder tb
        final int encounter_num_start
        final String mrnItem
        private final DBConfig cdw
        private final Path flat_file
        private final I2B2Upload upload
        final String patientMappingQuery

        NAACCR_Facts(DBConfig cdw, I2B2Upload upload, Path flat_file,
                     String patientMappingQuery, String mrnItem, int encounter_num_start) {
            final task_id = "upload_id_${upload.upload_id}"  // TODO: transition from task_id to upload_id
            tb = new TableBuilder(task_id: task_id, table_name: upload.factTable)
            this.flat_file = flat_file
            this.cdw = cdw
            this.upload = upload
            this.patientMappingQuery = patientMappingQuery
            this.mrnItem = mrnItem
            this.encounter_num_start = encounter_num_start
        }

        boolean complete() { tb.complete(cdw) }

        void run() {
            cdw.withSql { Sql sql ->
                final toPatientNum = getPatientMapping(sql, patientMappingQuery)

                makeTumorFacts(
                        flat_file, encounter_num_start,
                        sql, mrnItem, toPatientNum,
                        upload)
            }
        }

        static Map<String, Integer> getPatientMapping(Sql sql, String patientMappingQuery) {
            sql.rows(patientMappingQuery).collectEntries { [it.MRN, it.PATIENT_NUM] }
        }
    }

    static class I2B2Upload {
        final String schema
        final Integer upload_id
        final String sourcesystem_cd
        final String template_table = null

        I2B2Upload(@Nullable String schema, @Nullable Integer upload_id, String sourcesystem_cd,
                   String template_table = null) {
            this.schema = schema
            this.upload_id = upload_id
            this.sourcesystem_cd = sourcesystem_cd
            this.template_table = template_table
        }

        String getFactTable() {
            qname("OBSERVATION_FACT" + (upload_id == null ? "" : "_${upload_id}"))
        }

        private String qname(String object_name) {
            (schema == null ? '' : (schema + '.')) + object_name
        }

        static final List<ColumnMeta> obs_cols = [
                new ColumnMeta(name: "ENCOUNTER_NUM", dataType: Types.INTEGER, nullable: false),
                new ColumnMeta(name: "PATIENT_NUM", dataType: Types.INTEGER, nullable: false),
                new ColumnMeta(name: "CONCEPT_CD", size: 50, nullable: false),
                new ColumnMeta(name: "PROVIDER_ID", size: 50, nullable: false),
                // TODO: check timestamp vs. date re partition exchange; switch to create table as?
                new ColumnMeta(name: "START_DATE", dataType: Types.TIMESTAMP, nullable: false),
                new ColumnMeta(name: "MODIFIER_CD", size: 100, nullable: false),
                new ColumnMeta(name: "INSTANCE_NUM", dataType: Types.INTEGER, nullable: false),
                new ColumnMeta(name: "VALTYPE_CD", size: 50),
                new ColumnMeta(name: "TVAL_CHAR", size: 4000),
                new ColumnMeta(name: "NVAL_NUM", dataType: Types.FLOAT),
                new ColumnMeta(name: "END_DATE", dataType: Types.TIMESTAMP),
                new ColumnMeta(name: "UPDATE_DATE", dataType: Types.TIMESTAMP),
                new ColumnMeta(name: "DOWNLOAD_DATE", dataType: Types.TIMESTAMP),
                new ColumnMeta(name: "IMPORT_DATE", dataType: Types.TIMESTAMP),
                new ColumnMeta(name: "SOURCESYSTEM_CD", size: 50),
                new ColumnMeta(name: "UPLOAD_ID", dataType: Types.INTEGER),
        ]

        String getFactTableDDL(Map<Integer, String> toName) {
            if (template_table) {
                return """create table ${factTable} as select * from ${template_table} where 1 = 0"""
            }

            """
            create table ${factTable} (
                ${obs_cols.collect { it.ddl(toName) }.join(",\n  ")},
                primary key (
                    ENCOUNTER_NUM, CONCEPT_CD, PROVIDER_ID, START_DATE, MODIFIER_CD, INSTANCE_NUM)
            )
            """
        }

        String getInsertStatement() {
            """
            insert into ${factTable} (
            ${obs_cols.collect { it.name }.join(",\n  ")})
            values (${obs_cols.collect {
                it.name == 'IMPORT_DATE' ? 'current_timestamp' : "?.${it.name}".toLowerCase()
            }.join(",\n  ")})
            """.trim()
        }
        static final not_null = '@'
    }

    static int makeTumorFacts(Path flat_file, int encounter_num,
                              Sql sql, String mrnItem, Map<String, Integer> toPatientNum,
                              I2B2Upload upload,
                              boolean include_phi = false) {
        log.info("fact DML: {}", upload.insertStatement)

        final layout = NAACCR_Extract.theLayout(flat_file)

        final itemInfo = TumorOnt.NAACCR_I2B2.tumor_item_type.iterator().collect {
            final num = it.getInt('naaccrNum')
            final lf = layout.getFieldByNaaccrItemNumber(num)
            if (lf != null) {
                assert num == lf.naaccrItemNum
                assert it.getString('naaccrId') == lf.name
            }
            [num       : num, layout: lf,
             valtype_cd: it.getString('valtype_cd')]
        }.findAll { it.layout != null && (include_phi || !(it.valtype_cd as String).contains('i')) }

        final patIdField = layout.getFieldByName(mrnItem)
        final dxDateField = layout.getFieldByName('dateOfDiagnosis')
        final dateFields = [
                'dateOfBirth', 'dateOfDiagnosis', 'dateOfLastContact',
                'dateCaseCompleted', 'dateCaseLastChanged', 'dateCaseReportExported'
        ].collect { layout.getFieldByName(it) }

        dropIfExists(sql, upload.factTable)
        sql.execute(upload.getFactTableDDL(ColumnMeta.typeNames(sql.connection)))
        sql.withBatch(4096, upload.insertStatement) { ps ->
            int fact_qty = 0
            new Scanner(flat_file).useDelimiter("\r\n|\n") each { String line ->
                encounter_num += 1
                String patientId = fieldValue(patIdField, line)
                if (!toPatientNum.containsKey(patientId)) {
                    log.warn('tumor {}: cannot find {} in patient_mapping', encounter_num, patientId)
                    return
                }
                final patient_num = toPatientNum[patientId]
                Map<String, LocalDate> dates = dateFields.collectEntries { FixedColumnsField dtf ->
                    [dtf.name, parseDate(fieldValue(dtf, line))]
                }
                if (dates.dateOfDiagnosis == null) {
                    log.info('tumor {} patient {}: cannot parse dateOfDiagnosis: {}',
                            encounter_num, patientId, fieldValue(dxDateField, line))
                    return
                }
                itemInfo.each { item ->
                    Map record
                    final field = item.layout as FixedColumnsField
                    try {
                        record = itemFact(encounter_num, patient_num, line, dates,
                                field, item.valtype_cd as String,
                                upload.sourcesystem_cd)
                    } catch (badItem) {
                        log.warn('tumor {} patient {}: cannot make fact for item {}: {}',
                                encounter_num, patientId, field.name, badItem.toString())
                    }
                    if (record != null) {
                        ps.addBatch(record)
                        fact_qty += 1
                    }
                }
                if (encounter_num % 1000 == 0) {
                    log.info('tumor {}: {} facts', encounter_num, fact_qty)
                }
            }
        }
        // only fill in upload_id after all rows are done
        sql.execute("update ${upload.factTable} set upload_id = ?.upload_id".toString(), [upload_id: upload.upload_id])
        encounter_num
    }

    static String fieldValue(FixedColumnsField field, String line) {
        line.substring(field.start - 1, field.start + field.length - 1).trim()
    }

    static Map itemFact(int encounter_num, int patient_num, String line, Map<String, LocalDate> dates,
                        FixedColumnsField fixed, String valtype_cd,
                        String sourcesystem_cd) {
        final value = fieldValue(fixed, line)
        if (value == '') {
            return null
        }
        final nominal = valtype_cd == '@' ? value : ''
        LocalDate start_date = dates.dateOfDiagnosis
        if (valtype_cd == 'D') {
            if (value == '99999999') {
                // "blank" date value
                return null
            }
            start_date = parseDate(value)
            if (start_date == null) {
                log.warn('tumor {} patient {}: cannot parse {}: [{}]',
                        encounter_num, patient_num, fixed.name, value)
                return null
            }
        } else if (fixed.section == 'Follow-up/Recurrence/Death'
                && dates.dateOfLastContact !== null) {
            start_date = dates.dateOfLastContact
        }
        String concept_cd = "NAACCR|${fixed.naaccrItemNum}:${nominal}"
        assert concept_cd.length() <= 50
        Double num = null
        if (valtype_cd == 'N' && !value.startsWith(('XX'))) {
            try {
                num = Double.parseDouble(value)
            } catch (badNum) {
                log.warn('tumor {} patient {}: cannot parse number {}: [{}] {}',
                        encounter_num, patient_num, fixed.name, value, badNum.toString())
                return null
            }
        }
        final update_date = [
                dates.dateCaseLastChanged, dates.dateCaseCompleted,
                dates.dateOfLastContact, dates.dateOfDiagnosis,
        ].find { it != null }
        [
                encounter_num  : encounter_num,
                patient_num    : patient_num,
                concept_cd     : concept_cd,
                provider_id    : I2B2Upload.not_null,
                start_date     : start_date,
                modifier_cd    : I2B2Upload.not_null,
                instance_num   : 1,
                valtype_cd     : valtype_cd,
                tval_char      : valtype_cd == 'T' ? value : null,
                nval_num       : num,
                end_date       : start_date,
                update_date    : update_date,
                download_date  : dates.dateCaseReportExported,
                sourcesystem_cd: sourcesystem_cd,
                upload_id      : null,
        ]
    }

    static long _stable_hash(String text) {
        final CRC32 out = new CRC32()
        final byte[] bs = text.getBytes('UTF-8')
        out.update(bs, 0, bs.size())
        out.value
    }

    static LocalDate parseDate(String txt) {
        LocalDate value
        int nch = txt.length()
        // final y2k = { String yymmdd -> (yymmdd < '50' ? '20' : '19') + yymmdd }
        String full = nch == 4 ? txt + '0101' : nch == 6 ? txt + '01' : txt
        try {
            value = LocalDate.parse(full, DateTimeFormatter.BASIC_ISO_DATE) // 'yyyyMMdd'
        } catch (DateTimeParseException ignored) {
            value = null
        }
        value
    }


    static class LoadLayouts implements Task {
        final private DBConfig account
        final String table_name

        LoadLayouts(DBConfig account, String table_name) {
            this.account = account
            this.table_name = table_name
        }

        boolean complete() {
            account.withSql { Sql sql ->
                def fieldsPerVersion
                try {
                    fieldsPerVersion = sql.rows(
                            "select layoutVersion, count(*) field_qty from ${table_name} group by layoutVersion".toString())
                    log.info("load-layout complete? {}", fieldsPerVersion)
                } catch (SQLException oops) {
                    log.warn("failed to check layout records: {}", oops.toString())
                    return false
                }
                (fieldsPerVersion.findAll { it.field_qty as int >= 100 }).size() >= 3
            }
        }

        static List<ColumnMeta> columns() {
            final pretty_long = 64
            [
                    new ColumnMeta(name: 'layoutVersion', size: 3),
                    new ColumnMeta(name: 'naaccrItemNum', dataType: Types.INTEGER),
                    new ColumnMeta(name: 'section', size: pretty_long),
                    new ColumnMeta(name: 'name', size: pretty_long),
                    new ColumnMeta(name: 'longLabel', size: pretty_long),
                    new ColumnMeta(name: 'shortLabel', size: pretty_long),
                    new ColumnMeta(name: 'startPos', dataType: Types.INTEGER),
                    new ColumnMeta(name: 'endPos', dataType: Types.INTEGER),
                    new ColumnMeta(name: 'length', dataType: Types.INTEGER),
                    new ColumnMeta(name: 'trim', dataType: Types.BOOLEAN),
                    new ColumnMeta(name: 'padChar', size: 1),
                    new ColumnMeta(name: 'align', size: 16), // actually enum: LEFT, ...
                    new ColumnMeta(name: 'defaultValue', size: pretty_long),
                    // subFields?
            ]
        }

        void run() {
            final layouts = [
                    LayoutFactory.getLayout(LayoutFactory.LAYOUT_ID_NAACCR_12) as FixedColumnsLayout,
                    LayoutFactory.getLayout(LayoutFactory.LAYOUT_ID_NAACCR_14) as FixedColumnsLayout,
                    LayoutFactory.getLayout(LayoutFactory.LAYOUT_ID_NAACCR_16) as FixedColumnsLayout,
                    LayoutFactory.getLayout(LayoutFactory.LAYOUT_ID_NAACCR_18) as FixedColumnsLayout,
            ]
            account.withSql { Sql sql ->
                dropIfExists(sql, table_name)
                sql.execute(ColumnMeta.createStatement(table_name, columns(), ColumnMeta.typeNames(sql.connection)))
                final String stmt = ColumnMeta.insertStatement(table_name, columns())
                log.info("layout insert: {}", stmt)
                layouts.each { layout ->

                    layout.allFields.each { field ->
                        final Map params = [
                                layoutVersion: layout.layoutVersion,
                                naaccrItemNum: field.naaccrItemNum,
                                section      : field.section,
                                name         : field.name,
                                longLabel    : field.longLabel,
                                shortLabel   : field.shortLabel,
                                startPos     : field.start,
                                endPos       : field.end,
                                length       : field.length,
                                trim         : field.trim,
                                padChar      : field.padChar,
                                align        : field.align.toString(),
                                defaultValue : field.defaultValue
                        ]
                        sql.executeInsert(params, stmt)
                    }
                }
            }
        }
    }
}
