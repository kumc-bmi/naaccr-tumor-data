import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.record.fixed.FixedColumnsField
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import com.imsweb.naaccrxml.NaaccrXmlDictionaryUtils
import com.imsweb.naaccrxml.PatientFlatReader
import com.imsweb.naaccrxml.PatientReader
import com.imsweb.naaccrxml.entity.Patient
import com.imsweb.naaccrxml.entity.Tumor
import com.imsweb.naaccrxml.entity.dictionary.NaaccrDictionary
import groovy.sql.Sql
import groovy.transform.CompileStatic
import tech.tablesaw.api.*
import tech.tablesaw.columns.Column
import tech.tablesaw.columns.strings.StringFilters

import java.nio.file.Paths
import java.sql.SQLException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.logging.Logger
import java.util.zip.CRC32

import static TumorOnt.load_data_frame

@CompileStatic
class TumorFile {
    static Logger log = Logger.getLogger("")

    static void main(String[] args) {
        DBConfig.CLI cli = new DBConfig.CLI(args,
                { String name -> System.getenv(name) },
                { int it -> System.exit(it) })

        DBConfig cdw = cli.account()

        URL flat_file = Paths.get(cli.arg("--flat-file")).toUri().toURL()

        // IDEA: support disk DB in place of memdb
        Task work
        String task_id = cli.arg("--task-hash", "task123")
        if (cli.arg('--summary')) {
            work = new NAACCR_Summary(cdw, flat_file, task_id)
        } else if (cli.arg('--visits')) {
            work = new NAACCR_Visits(cdw, flat_file, task_id, 2000000)
        } else {
            throw new IllegalArgumentException('which task???')
        }

        if (!work.complete()) {
            work.run()
        }
    }

    interface Task {
        boolean complete()

        void run()
    }

    static class TableBuilder {
        String table_name
        String task_id

        boolean complete(Sql sql) {
            try {
                final row = sql.firstRow("select 1 from ${table_name} where task_id = ?.task_id)",
                        [task_id: task_id])
                row && row[0] == 1
            } catch (SQLException problem) {
                log.warning("not complete: $problem")
            }
            return false
        }

        void build(Sql sql, Table data) {
            data.addColumns(constS('task_id', data, task_id))
            try {
                sql.execute("drop table ${table_name}".toString())
            } catch (SQLException ignored) {
            }
            // TODO: case fold?
            load_data_frame(sql, table_name, data)
        }
    }

    static StringColumn constS(String name, Table t, String val) {
        StringColumn.create(name, [val] * t.rowCount() as String[])
    }

    static class NAACCR_Summary implements Task {
        final String table_name = "NAACCR_EXPORT_STATS"
        final TableBuilder tb
        final DBConfig cdw
        final URL flat_file  // TODO: serializable?

        NAACCR_Summary(DBConfig _cdw, URL _flat_file, String _task_id) {
            tb = new TableBuilder(task_id: _task_id, table_name: table_name)
            flat_file = _flat_file
            cdw = _cdw
        }

        // TODO: ISSUE: Sql.withInstance is ambient
        // TODO: factor out common parts of NAACCR_Summary, NAACCR_Visits as TableBuilder a la python?
        boolean complete() {
            boolean done = false
            Sql.withInstance(cdw.url, cdw.username, cdw.password.value, cdw.driver) { Sql sql ->
                done = tb.complete(sql)
            }
            done
        }

        void run() {
            final Table dd = ddictDF()
            final DBConfig mem = DBConfig.memdb()
            Table data = null
            Sql.withInstance(mem.url, mem.username, mem.password.value, mem.driver) { Sql memdb ->
                Reader naaccr_text_lines = new InputStreamReader(flat_file.openStream())
                final Table extract = read_fwf(naaccr_text_lines, dd.collect { it.getString('naaccrId') })
                data = DataSummary.stats(extract, memdb)
            }
            Sql.withInstance(cdw.url, cdw.username, cdw.password.value, cdw.driver) { Sql sql ->
                tb.build(sql, data)
            }
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

        boolean complete() {
            boolean done = false
            Sql.withInstance(cdw.url, cdw.username, cdw.password.value, cdw.driver) { Sql sql ->
                done = tb.complete(sql)
            }
            done
        }

        void run() {
            Table tumors = _data(flat_file, encounter_num_start)
            Sql.withInstance(cdw.url, cdw.username, cdw.password.value, cdw.driver) { Sql sql ->
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

    static long _stable_hash(String text) {
        final CRC32 out = new CRC32()
        final byte[] bs = text.getBytes('UTF-8')
        out.update(bs, 0, bs.size())
        out.value

    }

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
        Collection<Column<?>> cols = (items.collect { it -> StringColumn.create(it) }) as Collection<Column<?>>;
        Table data = Table.create(cols)
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

        static Table pat_tmr(Reader naaccr_text_lines) {
            _pick_cols(tmr_attrs + pat_attrs + report_attrs, naaccr_text_lines)
        }

        static Table patients(Reader naaccr_text_lines) {
            _pick_cols(pat_attrs + report_attrs, naaccr_text_lines)
        }

        private static Table _pick_cols(List<String> attrs, Reader lines) {
            Collection<Column<?>> cols = (attrs.collect { it -> StringColumn.create(it, []) }) as Collection<Column<?>>;
            Table pat_tmr = Table.create(cols)
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
                StringColumn sc = data.column(it).asStringColumn()
                sc.set((sc as StringFilters).isMissing(), extra_default)
                id_col = id_col.join('', sc)
            }
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
    }


    static final FixedColumnsLayout layout18 = LayoutFactory.getLayout(LayoutFactory.LAYOUT_ID_NAACCR_18_INCIDENCE) as FixedColumnsLayout
    static final Table record_layout = TumorOnt.fromRecords(
            layout18.getAllFields().collect { FixedColumnsField it ->
                [('long-label')     : it.longLabel,
                 start              : it.start,
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
            final StringColumn strcol = df.stringColumn(dtname).copy().setName(strname)
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
                TumorOnt.resourceText(TumorFile.getResource('heron_load/data_char_sim.sql')),
                [new Tuple2('data_agg_naaccr', ['naaccr_extract', 'tumors_eav', 'tumor_item_type']),
                 new Tuple2('data_char_naaccr', ['record_layout'])])

        static Table stats(Table tumors_raw, Sql sql) {
            final Table ty = TumorOnt.NAACCR_I2B2.tumor_item_type
            final Table tumors = naaccr_dates(tumors_raw, ['dateOfDiagnosis'], false)
            final Map<String, Table> views = TumorOnt.create_objects(sql, script, [
                    section        : TumorOnt.NAACCR_I2B2.per_section,
                    naaccr_extract : tumors,
                    record_layout  : record_layout,
                    tumor_item_type: ty,
                    tumors_eav     : stack_obs(tumors, ty, ['dateOfDiagnosis']),
            ])
            Table out = views.values().last()
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
