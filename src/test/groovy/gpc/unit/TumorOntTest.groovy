package gpc.unit

import gpc.DBConfig
import gpc.Tabular
import gpc.Tabular.ColumnMeta
import gpc.TumorFile
import gpc.TumorOnt
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase

import java.time.LocalDate

@CompileStatic
@Slf4j
class TumorOntTest extends TestCase {
    void 'test import sections'() {
        final table_name = 'NAACCR_ONTOLOGY'

        DBConfig.inMemoryDB("sections", true).withSql { Sql sql ->
            final toTypeName = ColumnMeta.typeNames(sql.connection)
            sql.execute(ColumnMeta.createStatement(table_name, TumorOnt.metadataColumns, toTypeName))
            TumorOnt.insertTerms(sql, table_name, TumorOnt.sectionCSV, { Map s -> TumorOnt.makeSectionTerm(s) })
            final actual = sql.firstRow('select min(c_fullname) fn1, min(c_name) n1, count(*) qty from NAACCR_ONTOLOGY')
            assert actual == [
                    N1 : '01 Cancer Identification',
                    FN1: '\\i2b2\\naaccr\\S:1 Cancer Identification\\',
                    QTY: 17,
            ]
        }
    }

    void 'test write sections'() {
        final update_date = LocalDate.of(2020, 7, 17)
        final out = new StringWriter()
        TumorOnt.writeTerms(out, update_date, TumorOnt.sectionCSV, { Map s -> TumorOnt.makeSectionTerm(s) })
        final actual = out.toString().split('\r\n')
        assert actual.size() == 18
        assert actual[0].startsWith('C_HLEVEL,C_FULLNAME,C_NAME')
        assert actual[1].startsWith('2,\\i2b2\\naaccr\\S:1 Cancer Identification\\')
        assert actual[1].count(',') == TumorOnt.metadataColumns.size() - 1
    }

    void 'test import items'() {
        final table_name = 'NAACCR_ONTOLOGY'

        DBConfig.inMemoryDB("sections", true).withSql { Sql sql ->
            final toTypeName = ColumnMeta.typeNames(sql.connection)
            sql.execute(ColumnMeta.createStatement(table_name, TumorOnt.metadataColumns, toTypeName))
            TumorOnt.insertTerms(sql, table_name, TumorOnt.itemCSV, { Map s -> TumorOnt.makeItemTerm(s) })
            final actual = sql.firstRow('select min(c_fullname) fn1, min(c_name) n1, count(*) qty from NAACCR_ONTOLOGY')
            assert actual == [
                    'FN1': '\\i2b2\\naaccr\\S:1 Cancer Identification\\0380 Sequence Number--Central\\',
                    'N1' : '0010 Record Type',
                    'QTY': 732,
            ]
        }
    }

    void 'test SEER Recode terms'() {
        final recodeRules = TumorFile.SEERRecode.fromLines(TumorFile.SEERRecode.site_recode.text)

        final terms = recodeRules.collect { it.asTerm() }.unique()
        assert terms[2] == [hlevel: 1, path: 'Oral Cavity and Phar\\Tongue', name: 'Tongue', basecode: '20020', visualattributes: 'LA']

        final t2 = Tabular.allCSVRecords(TumorOnt.SEERRecode.seer_recode_terms)
        [t2, terms].transpose().each {
            final parts = it as List
            assert parts[0] == parts[1]
        }
        assert t2.size() == terms.size()
    }

    static final String cache = ',cache/'

    void testLoadCSV() {
        final res = TumorOnt.getResource('heron_load/section.csv')
        final meta = Tabular.columnDescriptions(res)
        assert meta.collect { it.dataType } == [java.sql.Types.INTEGER, java.sql.Types.VARCHAR]
        final per_section = Tabular.allCSVRecords(res)
        assert per_section
                .findAll { it.section == "Record ID" }
                .collect { it.sectionid } == [9]
    }

    void "test loinc answer codes"() {
        def qty = 0
        TumorOnt.LOINC_NAACCR.eachAnswerTerm { Map it ->
            qty += 1
            assert it.keySet().size() == 19
            assert (it.C_BASECODE as String).matches('NAACCR\\|\\d+:.+')
        }
        assert qty > 100
    }

    void "test R code labels"() {
        TumorOnt.NAACCR_R.eachFieldScheme {
            println(it)
        }
        TumorOnt.NAACCR_R.eachCodeLabel {
            println(it)
            final expected = ['code', 'label', 'means_missing', 'description', 'scheme'] as Set<String>
            final actual = (it as Map).keySet() as Set<String>
            assert expected - actual == [] as Set
        }
        TumorOnt.NAACCR_R.eachCodeTerm {
            println(it)
        }
    }

    /*@@@
    void testOncologyMeta() {
        Path cachePath = Paths.get(cache)
        if (!cachePath.toFile().exists()) {
            log.warn('skipping OncologyMeta test. cache does not exist: ' + cache)
            return
        }
        final meta = TumorOnt.OncologyMeta
        final morph = meta.read_table(cachePath, meta.morph3_info)
        assert morph.columnNames() == ["code", "label", "notes"]

        // test encoding
        final non_ascii = morph.where(morph.stringColumn("code").isEqualTo("M8950/3"))
        assert non_ascii.get(0, 1) == "M\u009Fllerian mixed tumour"

        final topo = meta.read_table(Paths.get(cache), meta.topo_info)
        assert topo.columnNames() == ["Kode", "Lvl", "Title"]

        final icd_o_topo = meta.icd_o_topo(topo)
        assert icd_o_topo.columnNames() == ['lvl', 'concept_cd', 'c_visualattributes', 'path', 'concept_name']
    }
*/

    void testTableSql() {
        final metadataColumns = TumorOnt.metadataColumns
        DBConfig.inMemoryDB("ont", true).withSql { Sql sql ->
            final toTypeName = ColumnMeta.typeNames(sql.connection)

            final create = ColumnMeta.createStatement("tumor_item_type", metadataColumns, toTypeName)
                    .replaceAll('\\s+', ' ')
            assert create.startsWith("create table tumor_item_type (")
            assert create.contains("C_HLEVEL INTEGER")
            assert create.endsWith('C_SYMBOL VARCHAR(50) )')

            final insert = ColumnMeta.insertStatement("tumor_item_type", metadataColumns)
                    .replaceAll('\\s+', ' ')
            assert insert.contains("( C_HLEVEL, C_FULLNAME")
            assert insert.contains("C_SYMBOL)")
            assert insert.contains("(?.C_HLEVEL, ?.C_FULLNAME")
        }
    }

    void testOnt() {
        def update_date = LocalDate.of(2000, 1, 1)
        def top = TumorOnt.top
        assert top.C_HLEVEL == 1

        DBConfig.inMemoryDB("ont", true).withSql { Sql sql ->
            TumorOnt.createTable(sql, "META") // TODO: Paths.get(cache)

            final sections = sql.rows("""
                select * from META where c_hlevel = 2 and c_fullname like '\\i2b2\\naaccr\\S:%' escape '@' """)
            assert sections[0].keySet().size() == 25
            assert sections[0].keySet().contains("C_FULLNAME")
            // top concept
            assert sql.firstRow("select count(*) qty from META where C_HLEVEL = 1").qty == 1

            assert sections.size() == 17

            assert sql.firstRow("""
                select count(*) qty from META where c_hlevel = 3
                and c_fullname like '\\i2b2\\naaccr\\S:%' escape '@' """).qty as int > 500

            // TODO: separate LOINC, R codes?
            // code concepts
            assert sql.firstRow("""
                select count(*) qty from META where c_hlevel = 4
                and c_fullname like '\\i2b2\\naaccr\\S:%' escape '@' """).qty as int > 5000

            // TODO: separate SEER site table method?
            // seer site
            assert sql.firstRow("""
                select count(*) qty from META
                where c_fullname like '\\i2b2\\naaccr\\SEER Site\\%' escape '@' """).qty == 103

            // TODO: separate site-specific factor method?
            // cancer staging site-specific terms
            assert sql.firstRow("""
                select count(*) qty from META
                where c_fullname like '\\i2b2\\naaccr\\csterms\\%' escape '@' """).qty as int > 10000
        }
    }

    void testSqlDialect() {
        DBConfig.inMemoryDB("dialect").withSql { Sql sql ->
            assert sql.firstRow("select 1 as x from (values('X'))  ")[0] == 1
            assert sql.firstRow("select lpad(10, 4, '0') from (values(1))")[0] == "0010"
            assert sql.firstRow("select lpad(9, 2, '0') || ' xyz' from (values(1))")[0] == "09 xyz"
            assert sql.firstRow("select 1 from (values('X')) where not regexp_like('XXXX.9', '^[0-9].*')")[0] == 1
        }
    }
}