import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.LayoutInfo
import com.imsweb.naaccrxml.NaaccrObserver
import com.imsweb.naaccrxml.NaaccrOptions
import com.imsweb.naaccrxml.NaaccrXmlUtils
import com.imsweb.naaccrxml.entity.Patient
import groovy.transform.CompileStatic
import junit.framework.TestCase
import tech.tablesaw.api.DoubleColumn
import tech.tablesaw.api.Table

@CompileStatic
class TumorFileTest extends TestCase{
    String testDataPath = 'naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt'

    void testDF() {
        double[] numbers = [1, 2, 3, 4]
        DoubleColumn nc = DoubleColumn.create("nc", numbers)
        System.out.println(nc.print())
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
