import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.LayoutInfo
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase
import tech.tablesaw.api.DateColumn
import tech.tablesaw.api.DoubleColumn
import tech.tablesaw.api.StringColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.dates.DateFilters

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.logging.Logger

@CompileStatic
class TumorFileTest extends TestCase {
    static Logger log = Logger.getLogger("")

    static final String testDataPath = 'naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt'

    void testDF() {
        double[] numbers = [1, 2, 3, 4]
        DoubleColumn nc = DoubleColumn.create("nc", numbers)
        System.out.println(nc.print())
    }

    void testDDict() {
        final dd = TumorFile.ddictDF()
        assert dd.rowCount() > 700

        final primarySite = dd.where(dd.intColumn("naaccrNum").isEqualTo(400))

        assert primarySite[0].getString("naaccrId") == "primarySite"
        assert primarySite[0].getString("naaccrName") == 'Primary Site'
        assert primarySite[0].getInt("startColumn") == 554
        assert primarySite[0].getInt("length") == 4
        assert primarySite[0].getString("parentXmlElement") == 'Tumor'
    }

    void testPatients() {
        new File(testDataPath).withReader { reader ->
            Table patientData = TumorFile.TumorKeys.patients(reader)

            // got expected columns?
            assert patientData.columnNames() == TumorFile.TumorKeys.pat_attrs + TumorFile.TumorKeys.report_attrs

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
        assert possibleFormats.first().layoutId == 'naaccr-18-incidence'

        assert LayoutFactory.LAYOUT_ID_NAACCR_18_INCIDENCE == 'naaccr-18-incidence'
        final FixedColumnsLayout v18 = LayoutFactory.getLayout('naaccr-18-incidence') as FixedColumnsLayout
        assert v18.getFieldByNaaccrItemNumber(400).start == 554
        assert v18.allFields.take(3).collect {
            [('long-label')     : it.longLabel,
             start              : it.start,
             ('naaccr-item-num'): it.naaccrItemNum,
             section            : it.section,
             grouped            : it.subFields != null && it.subFields.size() > 0
            ]
        } == [['long-label': 'Record Type', 'start': 1, 'naaccr-item-num': 10, 'section': 'Record ID', 'grouped': false],
              ['long-label': 'Registry Type', 'start': 2, 'naaccr-item-num': 30, 'section': 'Record ID', 'grouped': false],
              ['long-label': 'Reserved 00', 'start': 3, 'naaccr-item-num': 37, 'section': 'Record ID', 'grouped': false]]
    }

    static final Table _extract = new File(testDataPath).withReader { naaccr_text_lines ->
        log.info("tr_file: ${testDataPath}")
        final Table dd = TumorFile.ddictDF()
        TumorFile.read_fwf(naaccr_text_lines, dd.collect { it.getString('naaccrId') })
    }

    static final Sql _spark = ({ DBConfig config ->
        Sql.newInstance(config.url, config.username, config.password.value, config.driver)
    })(LoaderTest.config1)

    static Table _SQL(String query, int limit=100) {
        Table out
        _spark.query(query) { results ->
            out = Table.read().db(results).first(limit)
        }
        out
    }


    void testDates() {
        def cases = [[label: 'normal', text: '19700101',
                      date : LocalDate.of(1970, 1, 1)],
                     [label: 'no day', text: '197001',
                      date : LocalDate.of(1970, 1, 1)],
                     [label: 'no month', text: '1970',
                      date : LocalDate.of(1970, 1, 1)],
                     [label: 'leading space', text: '    1970',
                      date : LocalDate.of(1970, 1, 1)],
                     [label: 'no month, variation', text: '19709999',
                      date : null],
                     [label: 'all 9s', text: '99999999',
                      date : null],
                     [label: 'all 0s', text: '00000000',
                      date : null],
                     [label: 'almost all 9s', text: '99990',
                      date : null],
                     [label: 'empty', text: '',
                      date : null],
                     [label: 'inscruitable', text: '12001024',
                      date : LocalDate.of(1200, 10, 24)]
        ]
        def caseTable = TumorOnt.fromRecords(cases as List<Map>)
        def results = TumorFile.naaccr_date_col(caseTable.stringColumn('text'))
        assert caseTable.column('date').asList() == results.asList()

        Table actual = TumorFile.naaccr_dates(
                _extract.select('dateOfDiagnosis', 'dateOfLastContact'),
                ['dateOfDiagnosis', 'dateOfLastContact'],
                true).first(10)
        println(actual)
        assert actual.where((actual.dateColumn('dateOfDiagnosis') as DateFilters).isMissing()).rowCount() == 0
    }

    void testStats() {
        _spark.execute("DROP SCHEMA PUBLIC CASCADE")  // KLUDGE
        TumorFile.DataSummary.stats(_extract, _spark)

        Table actual = _SQL("select * from data_char_naaccr limit 10")
        // println(actual)
        assert actual.columnCount() == 12
    }

    /**
     * _stable_hash uses a published algorithm (CRC32)
     */
    void testStableHash() {
        assert TumorFile._stable_hash("abc") == 891568578
    }
}
