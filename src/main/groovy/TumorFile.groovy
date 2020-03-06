import com.imsweb.naaccrxml.NaaccrXmlDictionaryUtils
import com.imsweb.naaccrxml.PatientFlatReader
import com.imsweb.naaccrxml.PatientReader
import com.imsweb.naaccrxml.entity.Patient
import com.imsweb.naaccrxml.entity.dictionary.NaaccrDictionary
import groovy.sql.Sql
import tech.tablesaw.api.IntColumn
import tech.tablesaw.api.Row
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.Column

class TumorFile {
    static class NAACCR_Summary {
        final String table_name = "NAACCR_EXPORT_STATS"

        final String z_design_id = "fill NaN (${_stable_hash(td.DataSummary.script.code)})"

        Table _data(Sql sql,
                    Table naaccr_text_lines) {
            final dd = tr_ont.ddictDF(sql)
            final extract = td.naaccr_read_fwf(naaccr_text_lines, dd)
            td.DataSummary.stats(extract, spark).na.fill(0, subset = ['sd'])
        }
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
}
