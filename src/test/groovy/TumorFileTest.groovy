import com.imsweb.layout.LayoutInfo
import com.imsweb.layout.LayoutFactory
import com.imsweb.naaccrxml.NaaccrOptions
import com.imsweb.naaccrxml.PatientReader
import com.imsweb.naaccrxml.PatientFlatReader
import com.imsweb.naaccrxml.entity.Patient
import com.imsweb.naaccrxml.NaaccrXmlUtils
import com.imsweb.naaccrxml.NaaccrObserver
import groovy.transform.CompileStatic
import junit.framework.TestCase
import tech.tablesaw.api.DoubleColumn
import tech.tablesaw.api.IntColumn
import tech.tablesaw.api.NumericColumn
import tech.tablesaw.api.Row
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.Column


@CompileStatic
class TumorFileTest extends TestCase{
    String testDataPath = 'naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt'

    void testDF() {
        double[] numbers = [1, 2, 3, 4]
        DoubleColumn nc = DoubleColumn.create("nc", numbers)
        System.out.println(nc.print())
    }

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

    void testPatients() {
        new File(testDataPath).withReader { reader ->
            Table patientData = TumorKeys.patients(reader)

            // got expected columns?
            assert patientData.columnNames() == TumorKeys.pat_attrs + TumorKeys.report_attrs

            // count patient records. Note: 6 tumors were on existing patients
            assert patientData.rowCount() == 94
            // just as many patientIdNumber values, right?
            assert patientData.select('patientIdNumber').dropDuplicateRows().rowCount() == 94
            println(patientData.first(5))
        }
    }
    void testLayout() {
        List<LayoutInfo> possibleFormats = LayoutFactory.discoverFormat(new File(testDataPath));
        assert !possibleFormats.isEmpty()
        println(possibleFormats)
    }

    class EachPatient implements NaaccrObserver {
        void patientRead(Patient patient) {
            //System.err.println(patient)
            //println(patient.getItem("patientIdNumber"))
            //patient.getItems() each { println(it.naaccrId + "=" + it.value) }
        }
        void patientWritten(Patient patient) {
            // skip
        }
    }
    void testReadFlatFile() {
        System.err.println(System.getProperty("user.dir"))
        NaaccrXmlUtils.flatToXml(new File(testDataPath), new File('/tmp/XXX.xml'), new NaaccrOptions(), [], new EachPatient())
    }
}
