package gpc.unit

import com.imsweb.layout.LayoutFactory
import com.imsweb.layout.LayoutInfo
import com.imsweb.layout.record.fixed.FixedColumnsLayout
import gpc.DBConfig
import gpc.DBConfig.Task
import gpc.TumorFile
import gpc.TumorFile.DataSummary
import gpc.TumorFile.TumorKeys
import gpc.TumorOnt
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase
import org.docopt.Docopt
import org.junit.Ignore
import tech.tablesaw.api.ColumnType
import tech.tablesaw.api.Row
import tech.tablesaw.api.Table
import tech.tablesaw.columns.dates.DateFilters
import tech.tablesaw.columns.strings.StringFilters

import java.nio.file.Paths
import java.sql.DriverManager
import java.time.LocalDate

@CompileStatic
@Slf4j
class TumorFileTest extends TestCase {
    static final String testDataPath = 'naaccr_xml_samples/naaccr-xml-sample-v180-incidence-100.txt'

    void testCLI() {
        final Properties config = new Properties()
        config.putAll(LoaderTest.dbInfo1 + [('naaccr.flat-file')    : testDataPath,
                                            ('naaccr.extract-table'): 'TR_EX'])
        DBConfig.CLI cli = buildCLI(['facts'], config)
        TumorFile.run_cli(cli)
    }

    /**
     * CAUTION: Ambient access to DB Connection
     */
    public static DBConfig.CLI buildCLI(List<String> args, Properties config) {
        final String doc = TumorFile.usage
        new DBConfig.CLI(new Docopt(doc).withExit(false).parse(args),
                { String name -> config },
                { int it -> throw new RuntimeException('unexpected exit') },
                { String url, Properties ps -> DriverManager.getConnection(url, ps) })
    }

    // NOTE: if you rename this, update CONTRIBUTING.md
    void testDocOpt() {
        final String doc = TumorFile.usage
        assert doc.startsWith('Usage:')
        final args = ['tumors']
        final actual = new Docopt(doc).withExit(false).parse(args)
        assert actual['--db'] == 'db.properties'
        assert actual['tumors'] == true
        assert actual['facts'] == false

        final both = new Docopt(doc).withExit(false).parse(
                ['tumors', '--task-id', 'abc123', '--db', 'deid.properties'])
        assert both['--db'] == 'deid.properties'
        assert both["--update-date"] == null

        def args2 = ['ontology', '--task-hash=1234', '--update-date=2002-02-02', '--who-cache=,cache']
        final more = new Docopt(doc).withExit(false).parse(
                args2)
        assert more['--task-hash'] == '1234'
        assert LocalDate.parse(more["--update-date"] as String) == LocalDate.of(2002, 2, 2)

        final Properties config = new Properties()
        config.putAll(LoaderTest.dbInfo1 + [('naaccr.flat-file'): testDataPath, ('naaccr.records-table'): 'T1'])
        DBConfig.CLI cli = new DBConfig.CLI(new Docopt(doc).withExit(false).parse(args2),
                { String ignored -> config }, null, null)
        assert cli.urlArg('--who-cache').toString().endsWith(',cache')
        assert cli.property("naaccr.records-table") == "T1"

        final tfiles = new Docopt(doc).withExit(false).parse(['load-files', 'F1', 'F2', 'F3'])
        assert tfiles['NAACCR_FILE'] == ['F1', 'F2', 'F3']
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

    void testExtractDiscrete() {
        final cdw = DBConfig.inMemoryDB("TR", true)
        final task_id = "task123"
        URL flat_file = Paths.get(testDataPath).toUri().toURL()
        Task extract = new TumorFile.NAACCR_Extract(cdw, task_id,
                [flat_file],
                "NAACCR_DISCRETE",
        )

        cdw.withSql { sql ->
            assert extract.complete() == false
            extract.run()
            assert extract.complete() == true
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

    void "test loading naaccr flat file"() {
        DBConfig.inMemoryDB("TR").withSql { Sql sql ->
            final claimed = TumorFile.NAACCR_Extract.loadFlatFile(
                    sql, new File(testDataPath), "TUMOR", "task1234", TumorOnt.pcornet_fields)
            assert claimed == 100
            final actual = sql.firstRow('select count(PRIMARY_SITE_N400) from TUMOR')[0]
            assert actual == claimed
        }
    }


    static final Table _extract() {
        new File(testDataPath).withReader { naaccr_text_lines ->
            // log.info("tr_file: ${testDataPath}")
            Table result = null
            TumorFile.read_fwf(naaccr_text_lines) { Table chunk ->
                if (result == null) {
                    result = chunk
                } else {
                    result = result.append(chunk)
                }
                return
            }
            result
        }
    }

    static Table _SQL(Sql sql, String query, int limit = 100) {
        Table out = null
        sql.query(query) { results ->
            out = Table.read().db(results).first(limit)
        }
        out
    }

    @Ignore
    static class ToDo extends TestCase {
        void testPatientsTask() {
            final cdw = DBConfig.inMemoryDB("PT", true)
            URL flat_file = Paths.get(testDataPath).toUri().toURL()
            Task work = new TumorFile.NAACCR_Patients(cdw, "task123456", [flat_file], "TR_EX")
            if (!work.complete()) {
                work.run()
            }

            Table actual = cdw.withSql { Sql sql -> _SQL(sql, 'select * from NAACCR_PATIENTS limit 20') }
            println(actual)
            assert 1 == 0
        }
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
        def actual = TumorFile.naaccr_date_col(date_cases.stringColumn('text'))
        assert date_cases_ymd == actual.asList()
    }

    static final Table date_cases = TumorOnt.read_csv(TumorFileTest.getResource('date_cases.csv'))
    static final List<LocalDate> date_cases_ymd = date_cases.iterator().collect { Row it ->
        it.isMissing('year') ?
                null : LocalDate.of(
                it.getInt('year'),
                it.getInt('month'),
                it.getInt('day'))
    }

    void testDatesInH2() {
        DBConfig acct1 = DBConfig.inMemoryDB("dates", true)
        acct1.withSql { Sql sql ->
            TumorOnt.load_data_frame(sql, "date_cases", date_cases)
            DBConfig.parseDateExInstall(sql)
            final actual = sql.rows("""
                 select parseDateEx(substring(concat(text, '0101'), 1, 8), 'yyyyMMdd') as date_value
                 from date_cases
                  """ as String).collect { it.date_value.toString() }
            // H2 parses 19709999 as 1978-06-07; we'll let that slide
            assert actual.take(4) == date_cases_ymd.collect { it == null ? null : it.toString() }.take(4)
        }
    }

    void testItemDates() {
        Table actual = TumorFile.naaccr_dates(
                _extract().select('dateOfDiagnosis', 'dateOfLastContact'),
                ['dateOfDiagnosis', 'dateOfLastContact'],
                true).first(10)
        println(actual)
        assert actual.where((actual.dateColumn('dateOfDiagnosis') as DateFilters).isMissing()).rowCount() == 0
    }

    void testVisits() {
        URL flat_file = Paths.get(testDataPath).toUri().toURL()
        Table tumors = new TumorFile.NAACCR_Visits(null, "task123", [flat_file], "TR_EX", 12345)._data(12345)
        assert tumors.stringColumn('recordId').countUnique() == tumors.rowCount()
        assert tumors.intColumn('encounter_num').min() == 12345
    }

    void testObsRaw() {
        final _extract = _extract()
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
            final Table actual = TumorFile.ItemObs.make(memdb, _extract())
            assert actual.columnNames() == [
                    'RECORDID', 'PATIENTIDNUMBER', 'NAACCRID', 'NAACCRNUM', 'DATEOFDIAGNOSIS',
                    'CONCEPT_CD', 'PROVIDER_ID', 'START_DATE', 'MODIFIER_CD', 'INSTANCE_NUM',
                    'VALTYPE_CD', 'TVAL_CHAR', 'NVAL_NUM', 'VALUEFLAG_CD', 'UNITS_CD',
                    'END_DATE', 'LOCATION_CD', 'UPDATE_DATE']
            assert actual.columnTypes() as List == [
                    ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.LOCAL_DATE,
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

    void testTumorFields() {
        Table actual = TumorOnt.fields(false)
        assert actual.rowCount() == 640
        assert actual.rowCount() > 100
        assert actual.where(actual.stringColumn('FIELD_NAME').isEqualTo('RECORD_TYPE_N10')).rowCount() == 1

        Table pcornet_spec = TumorOnt.read_csv(TumorFileTest.getResource('tumor table.version1.2.csv')).select(
                'NAACCR Item', 'FLAG', 'FIELD_NAME'
        )
        pcornet_spec = pcornet_spec.where(pcornet_spec.stringColumn('FLAG').isNotEqualTo('PRIVATE'))

        actual.column('FIELD_NAME').setName('name_test')
        Table items = pcornet_spec.joinOn('NAACCR Item').fullOuter(actual, 'naaccrNum')
        // println(items.first(3))
        Table problems = items.first(0)
        for (Row item : items) {
            if (item.getString('FIELD_NAME') != item.getString('name_test')) {
                problems.addRow(item)
            }
        }
        def missingId = problems.where(problems.column('naaccrId').isMissing())
        assert missingId.rowCount() == 26
        def noMatch = problems.where(problems.column('name_test').isMissing())
        assert noMatch.rowCount() == 26
        def renamed = problems.where(problems.stringColumn('name_test').isNotIn(''))
        assert renamed.rowCount() == 23
    }
}
