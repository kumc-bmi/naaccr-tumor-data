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

import java.util.logging.Logger
import java.util.zip.CRC32

import static TumorOnt.load_data_frame

@CompileStatic
class TumorFile {
    static Logger log = Logger.getLogger("")

    static class NAACCR_Summary {
        final String task_id
        final String table_name = "NAACCR_EXPORT_STATS"
        final String z_design_id = "fill NaN (${_stable_hash(DataSummary.script.code)})"
        private final URL flat_file
        private final DBConfig cdw

        NAACCR_Summary(String _task_id, DBConfig _cdw, URL _flat_file) {
            cdw = _cdw
            task_id = _task_id
            flat_file = _flat_file
        }

        boolean complete() {
            try {
                Sql.withInstance(cdw.url, cdw.username, cdw.password.value, cdw.driver) { Sql sql ->
                    return sql.firstRow("""
                        select 1 from ${table_name}
                        where task_id = ?.task_id)
                        """, [task_id: task_id])[0] == 1
                }
            } catch (Exception problem) {
                log.warning("not complete: $problem")
            }
            return false
        }

        void run() {
            Sql.withInstance(cdw.url, cdw.username, cdw.password.value, cdw.driver) { Sql sql ->
                Table data = _data(sql, new InputStreamReader(flat_file.openStream()))
                final constS = { String name, Table t, String val -> StringColumn.create(name, [val] * t.rowCount() as String[]) }
                data.addColumns(constS('task_id', data, task_id))
                // TODO: case fold?
                load_data_frame(sql, table_name, data)
            }
        }

        static Table _data(Sql sql,
                           Reader naaccr_text_lines) {
            final Table dd = ddictDF()
            final Table extract = read_fwf(naaccr_text_lines, dd.collect { it.getString('naaccrId') })
            DataSummary.stats(extract, sql)
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
        Collection<Column<?>> cols = (items.collect { it -> StringColumn.create(it, []) }) as Collection<Column<?>>;
        Table data = Table.create(cols)
        PatientReader reader = new PatientFlatReader(lines)
        Patient patient = reader.readPatient()
        while (patient != null) {
            patient.getTumors().each { Tumor it ->
                Row tumorRow = data.appendRow()
                items.each { String naaccrId ->
                    tumorRow.setString(naaccrId, it.getItemValue(naaccrId))
                }

            }
            patient = reader.readPatient()
        }
        data
    }

    static class TumorKeys {
        static List<String> pat_ids = ['patientSystemIdHosp', 'patientIdNumber']
        static List<String> pat_attrs = pat_ids + ['dateOfBirth', 'dateOfLastContact', 'sex', 'vitalStatus']
        static List<String> report_ids = ['naaccrRecordVersion', 'npiRegistryId']
        static List<String> report_attrs = report_ids + ['dateCaseReportExported']

        static Table patients(Reader lines) {
            List<String> attrs = pat_attrs + report_attrs
            Collection<Column<?>> cols = (attrs.collect { it -> StringColumn.create(it, []) }) as Collection<Column<?>>;
            Table patientData = Table.create("patient", cols)
            PatientReader reader = new PatientFlatReader(lines)
            Patient patient = reader.readPatient()
            while (patient != null) {
                Row patientRow = patientData.appendRow()
                attrs.each { String naaccrId ->
                    patientRow.setString(naaccrId, patient.getItemValue(naaccrId))
                }
                patient = reader.readPatient()
            }
            patientData
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

    static class DataSummary {
        static final TumorOnt.SqlScript script = new TumorOnt.SqlScript('data_char_sim.sql',
                TumorOnt.resourceText(TumorFile.getResource('heron_load/data_char_sim.sql')),
                [new Tuple2('data_agg_naaccr', ['naaccr_extract', 'tumors_eav', 'tumor_item_type'])])

        static Table stats(Table tumors, Sql sql) {
            // TODO: tumors_raw -> naaccr_dates
            final Table ty = TumorOnt.NAACCR_I2B2.tumor_item_type
            final Map<String, Table> views = TumorOnt.create_objects(sql, script, [
                    section        : TumorOnt.NAACCR_I2B2.per_section,
                    naaccr_extract : tumors,
                    record_layout  : record_layout,
                    tumor_item_type: ty,
                    tumors_eav     : stack_obs(tumors, ty, ['dateOfDiagnosis']),
            ])
            Table out = views.values().last()
            DoubleColumn sd = out.doubleColumn('sd')
            // TODO: sd.set(sd.isMissing(), 0 as Double)
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
            final Table entity = Table.create(data.columns().findAll { Column it -> id_vars.contains(it.name()) })
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
