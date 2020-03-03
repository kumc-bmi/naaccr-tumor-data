import junit.framework.TestCase

import java.nio.file.Paths

class TumorOntTest extends TestCase  {
    static def cache = ',cache/'

    void testOncologyMeta() {
        def info = TumorOnt.OncologyMeta.morph3_info
        def morph = TumorOnt.OncologyMeta.read_table(Paths.get(cache), info.zip, info.item, info.names)
        assert morph.columnNames() == ["code", "label", "notes"]

        // test encoding
        def non_ascii = morph.where(morph.stringColumn("code").isEqualTo("M8950/3"))
        println(non_ascii)
        assert non_ascii.get(0, 1) == "M\u009Fllerian mixed tumour"

        info = TumorOnt.OncologyMeta.topo_info
        def topo = TumorOnt.OncologyMeta.read_table(Paths.get(cache), info.zip, info.item, info.names)
        println(topo.first(5))
        assert topo.columnNames() == ["Kode", "Lvl", "Title"]
    }
}
