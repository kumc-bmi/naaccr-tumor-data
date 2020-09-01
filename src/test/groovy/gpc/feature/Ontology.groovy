package gpc.feature

import gpc.TumorFile
import gpc.TumorOnt
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import junit.framework.TestCase

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

@CompileStatic
@Slf4j
class Ontology extends TestCase {
    Path workDir

    void setUp() {
        workDir = Files.createTempDirectory('Staging')
    }

    void tearDown() {
        Staging.deleteFolder(workDir.toFile())
    }

    void "test build NAACCR_ONTOLOGY in a local disk h2 DB"() {
        final cli = Staging.cli1(['ontology'], workDir)
        TumorFile.run(cli)

        cli.account().withSql { Sql sql ->
            final actual = sql.rows("""
            select c_hlevel, count(*) qty from NAACCR_ONTOLOGY
            group by c_hlevel order by c_hlevel
            """)
            assert actual.collectEntries { [it.C_HLEVEL, it.QTY] } == [1: 1, 2: 19, 3: 987, 4: 6836, 5: 10545]
        }
    }

    void "test ICO-O-3 OncologyMeta"() {
        // final cache = Paths.get(',cache')
        final cache = Paths.get('/home/connolly/projects/naaccr-tumor-data/,cache') //@@@
        if (!cache.toFile().exists()) {
            log.warn('skipping OncologyMeta test. cache does not exist: ' + cache)
            return
        }

        final onc = TumorOnt.OncologyMeta.load(cache)

        assert onc.major()[0] == ['level': 3, 'code': '800', 'label': 'Neoplasms NOS']


        /*
        static String encoding = 'ISO-8859-1'

         */
    }

    static URL checkRes(Class<?> cls, String name) {
        URL it = cls.getResource(name)
        assert [it, name] != [null, name]
        it
    }

    void "test import 2017 ontology into local disk h2 DB"() {
        return // shrine_ont.naaccr_ontology.csv is not (yet?) checked in due to size
        final installed = [
                'heron_load/shrine_ont.naaccr_ontology.csv',
                'heron_load/shrine_ont.naaccr_ontology-metadata.json',
        ].collect { Staging.installTestData(workDir, checkRes(TumorOnt, it)) }
        def import_args = ['import', 'ONT_2017'] + installed.collect { it.toString() }
        def cli1 = Staging.cli1(import_args, workDir)
        // KLUDGE: clean up 2017 ont with basecode = '"' in place; TODO: update?
        // TODO: update to strip code names out of paths?
        TumorFile.run(cli1)
        TumorFile.run(Staging.cli1(['ontology'], workDir, null, false))

        cli1.account().withSql { Sql sql ->
            final actual17 = sql.rows("""
            select c_hlevel, count(*) qty from ONT_2017
            group by c_hlevel order by c_hlevel
            """)

            assert actual17.collectEntries { [it.C_HLEVEL, it.QTY] } == [1: 1, 2: 19, 3: 707, 4: 21423, 5: 14640, 6: 11]

            final fmt = { groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(it)) }
            final check = { String label, List<List> results ->
                final expected = results[0]
                final actual = results[1]
                final extra = actual.findAll { !expected.contains(it) }
                final missing = expected.findAll { !actual.contains(it) }

                if (extra != [] || missing != []) {
                    println(label)
                    println(fmt([extra: extra, missing: missing]))
                }
                // assert extra == []
                // assert missing == []
            }
            final tables = ['ONT_2017', 'NAACCR_ONTOLOGY']
            final subsets = { String cols, String clause, Map params ->
                tables.collect {
                    String q = """select ${cols} from ${it} where ${clause} order by c_fullname"""
                    sql.rows(q, params)
                }
            }
            check("NAACCR Folders",
                    subsets('c_hlevel, c_fullname, c_name, substr(c_visualattributes, 1, 1) as c_visualattributes',
                            'c_hlevel <= 2', [:]))
            sql.eachRow("""select c_fullname from ONT_2017
                    where c_hlevel = 3 and c_fullname like '\\\\i2b2\\\\naaccr\\\\S:%'
                    order by c_fullname""") { rs ->
                final path = rs.getString('C_FULLNAME')
                check(path,
                        subsets('c_basecode, substr(c_visualattributes, 1, 1) as c_visualattributes',
                                """
                    c_hlevel = 4 and c_fullname like (:path || '%') escape '@'
                    and c_fullname not like '%Site-Specific%' -- skip; no LOINC answer list for these
                    and c_fullname not like '%3110 Comorbid%'
                    and c_fullname not like '%\\Blank%' escape '@'
                    and c_fullname not like '%0400 Primary Site%'
                    and c_fullname not like '%Coding Sys%' -- boring; makes my eyes glaze over
                    and c_fullname not like '%0419 Morph%'
                    and c_fullname not like '%0521 Morph%'
                    and c_basecode not like 'NAACCR|522:%'
                    and c_basecode not like 'NAACCR|820:%' -- skip regional nodes positive; should be numeric?
                    and c_fullname not like '%0446 Multiplicity Counter%'
                    and c_name like '% %' -- labelled codes only
                    """, [path: path]))
            }
        }
    }
}

