import junit.framework.TestCase

import java.nio.file.Paths
import java.time.LocalDate

class TumorOntTest extends TestCase  {
    static def cache = ',cache/'

    void testOncologyMeta() {
        def info = TumorOnt.OncologyMeta.morph3_info
        def morph = TumorOnt.OncologyMeta.read_table(Paths.get(cache), info)
        assert morph.columnNames() == ["code", "label", "notes"]

        // test encoding
        def non_ascii = morph.where(morph.stringColumn("code").isEqualTo("M8950/3"))
        println(non_ascii)
        assert non_ascii.get(0, 1) == "M\u009Fllerian mixed tumour"

        info = TumorOnt.OncologyMeta.topo_info
        def topo = TumorOnt.OncologyMeta.read_table(Paths.get(cache), info)
        println(topo.first(5))
        assert topo.columnNames() == ["Kode", "Lvl", "Title"]
    }


    void testTableExpr() {
        def obj = [
                [c_facttablecolumn: "x", c_comment: "xx", c_totalnum: 1],
                [c_facttablecolumn: "CONCEPT_CD", c_comment: null, c_totalnum: null]
        ]
        println(obj[0])
        def actual = TumorOnt.build(obj)
        println(actual)
    }

    void testOnt() {
        def actual = TumorOnt.NAACCR_I2B2.ont_view_in("task123", LocalDate.of(2000, 1, 1), Paths.get(cache))
        for (Object x: actual) {
            println(x)
        }
    }
}