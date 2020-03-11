import TumorFile.DataSummary
import TumorFile.TumorKeys
import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.LayoutInfo
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import groovy.sql.Sql
import groovy.transform.CompileStatic
import junit.framework.TestCase
import org.docopt.Docopt
import tech.tablesaw.api.ColumnType
import tech.tablesaw.api.DoubleColumn
import tech.tablesaw.api.Table
import tech.tablesaw.columns.dates.DateFilters
import tech.tablesaw.columns.strings.StringFilters

import java.nio.file.Paths
import java.sql.DriverManager
import java.time.LocalDate
import java.util.logging.Logger

@CompileStatic
class TumorFileTest extends TestCase {
    static Logger log = Logger.getLogger("")

    static final String testDataPath = 'naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt'

    void testCLI() {
        final String doc = TumorFile.usage
        final args = ['facts', '--flat-file', testDataPath]
        DBConfig.CLI cli = new DBConfig.CLI(new Docopt(doc).withExit(false).parse(args),
                { String name -> LoaderTest.dbProps1 },
                { int it -> throw new RuntimeException('unexpected exit') },
                { String url, Properties ps -> DriverManager.getConnection(url, ps) } )
        TumorFile.run_cli(cli)
    }

    void testDocOpt() {
        final String doc = TumorFile.usage
        assert doc.startsWith('Usage:')
        final args = ['tumors', '--flat-file', testDataPath, '--task-id', 'abc123']
        final actual = new Docopt(doc).withExit(false).parse(args)
        assert actual['--db'] == 'db.properties'
        assert actual['tumors'] == true
        assert actual['facts'] == false

        final both = new Docopt(doc).withExit(false).parse(
                ['tumors', '--flat-file', testDataPath, '--task-id', 'abc123', '--db', 'deid.properties'])
        assert both['--db'] == 'deid.properties'
        assert both["--update-date"] == null

        final more = new Docopt(doc).withExit(false).parse(
                ['ontology', '--task-hash=1234', '--update-date=2002-02-02', '--who-cache=,cache'])
        assert more['--task-hash'] == '1234'
        assert LocalDate.parse(more["--update-date"] as String) == LocalDate.of(2002, 2, 2)
    }

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
        List<LayoutInfo> possibleFormats = LayoutFactory.discoverFormat(new File(testDataPath))
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

    static Table _SQL(Sql sql, String query, int limit = 100) {
        Table out = null
        sql.query(query) { results ->
            out = Table.read().db(results).first(limit)
        }
        out
    }

    void notYettestPatientsTask() {
        final cdw = DBConfig.inMemoryDB("PT", true)
        URL flat_file = Paths.get(testDataPath).toUri().toURL()
        TumorFile.Task work = new TumorFile.NAACCR_Patients(cdw, flat_file,
                "task123456")
        if (!work.complete()) {
            work.run()
        }

        Table actual = cdw.withSql { Sql sql -> _SQL(sql, 'select * from NAACCR_PATIENTS limit 20') }
        println(actual)
        assert 1 == 0
    }

    /*****
     * Date parsing. Ugh.

     p. 97:
     "Below are the common formats to handle the situation where only
     certain components of date are known.
     YYYYMMDD - when complete date is known and valid
     YYYYMM - when year and month are known and valid, and day is unknown
     YYYY - when year is known and valid, and month and day are unknown
     Blank - when no known date applies"

     But we also see wierdness such as '    2009' and '19719999'; see
     test cases below.

     In Date of Last Contact, we've also seen 19919999
     */
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
        DBConfig acct1 = DBConfig.inMemoryDB("stats", true)
        acct1.withSql { Sql sql ->
            final Table faster = _extract.first(20)
            DataSummary.stats(faster, sql)

            Table actual = _SQL(sql, "select * from data_char_naaccr limit 10")
            // println(actual)
            assert actual.columnCount() == 12
        }
    }

    void testVisits() {
        URL flat_file = Paths.get(testDataPath).toUri().toURL()
        Table tumors = TumorFile.NAACCR_Visits._data(flat_file, 12345)
        assert tumors.stringColumn('recordId').countUnique() == tumors.rowCount()
        assert tumors.intColumn('encounter_num').min() == 12345
    }

    void testObsRaw() {
        Table _ty = TumorOnt.NAACCR_I2B2.tumor_item_type
        Table _raw_obs = DataSummary.stack_obs(_extract, _ty, TumorKeys.key4 + TumorKeys.dtcols)
        _raw_obs = TumorFile.naaccr_dates(_raw_obs, TumorKeys.dtcols)

        assert _raw_obs.columnNames() == [
                'recordId', 'patientSystemIdHosp', 'tumorRecordNumber', 'patientIdNumber', 'abstractedBy',
                'dateOfBirth', 'dateOfDiagnosis', 'dateOfLastContact', 'dateCaseCompleted', 'dateCaseLastChanged',
                'naaccrId', 'value']
        assert _raw_obs.intColumn('recordId').countUnique() == _extract.rowCount()
        assert _raw_obs.stringColumn('naaccrId').countUnique() > 100
        // at least 1 in 20 fields populated
        assert _raw_obs.rowCount() >= _ty.rowCount() * _extract.rowCount() / 20
    }

    void testItemObs() {
        DBConfig.inMemoryDB("obs").withSql { Sql memdb ->
            final Table actual = TumorFile.ItemObs.make(memdb, _extract)
            assert actual.columnNames() == [
                    'RECORDID', 'PATIENTIDNUMBER', 'NAACCRID',
                    'CONCEPT_CD', 'PROVIDER_ID', 'START_DATE', 'MODIFIER_CD', 'INSTANCE_NUM',
                    'VALTYPE_CD', 'TVAL_CHAR', 'NVAL_NUM', 'VALUEFLAG_CD', 'UNITS_CD',
                    'END_DATE', 'LOCATION_CD', 'UPDATE_DATE']
            assert actual.columnTypes() as List == [
                    ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING,
                    ColumnType.STRING, ColumnType.STRING, ColumnType.LOCAL_DATE, ColumnType.STRING, ColumnType.INTEGER,
                    ColumnType.STRING, ColumnType.STRING, ColumnType.DOUBLE, ColumnType.STRING, ColumnType.STRING,
                    ColumnType.LOCAL_DATE, ColumnType.STRING, ColumnType.LOCAL_DATE]
            assert actual.rowCount() >= 300
            assert actual.where((actual.stringColumn('CONCEPT_CD') as StringFilters).isMissing()).rowCount() == 0
        }
    }

    /**
     * _stable_hash uses a published algorithm (CRC32)
     */
    void testStableHash() {
        assert TumorFile._stable_hash("abc") == 891568578
    }
}
