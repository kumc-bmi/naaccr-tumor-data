import com.imsweb.naaccrxml.PatientFlatReader
import com.imsweb.naaccrxml.PatientReader
import com.imsweb.naaccrxml.entity.Patient
import tech.tablesaw.api.Row
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.Column

class TumorKeys {
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
