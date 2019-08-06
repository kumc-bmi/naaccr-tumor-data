/** tumor_load - use naaccr-xml to load tumor registry as XML
 *
 * tested with openjdk 1.8, apache-groovy-2.5.7
 * sha256 3d905dfe4f739c8c0d9dd181e6687ac816e451bf327a9ec0740da473cfebc9e0
 *
 * Output can be used a la:
 *
 * select line
 *      , to_date(extract(tumor,
 *                        '* / * / * / *[@naaccrId="dateOfDiagnosis"]/text()'),
 *               'YYYYMMDD') as dateOfDiagnosis
 * from tumors;
 */
import java.io.StringWriter
import java.sql.SQLXML
import java.util.logging.Logger

import groovy.sql.Sql
import groovy.sql.GroovyResultSet
import groovy.sql.BatchingPreparedStatementWrapper as PS
import groovy.transform.CompileStatic

// "the [xpp3] library needs to be loaded by the system classloader"
// -- https://codeday.me/en/qa/20190306/5613.html
@GrabConfig(systemClassLoader= true)
@Grab('com.imsweb:naaccr-xml:6.1')

import com.imsweb.naaccrxml.PatientFlatReader
import com.imsweb.naaccrxml.PatientXmlWriter
import com.imsweb.naaccrxml.entity.Patient
import com.imsweb.naaccrxml.entity.NaaccrData


@CompileStatic
class Loader {
    static String oraDriver = 'oracle.jdbc.OracleDriver'
    static String oraUrl = 'jdbc:oracle:thin:@localhost:1521:'
    static int xml_batch_size = 100  // patients per record
    static int sql_batch_size = 50   // records per batch insert
    static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)

    static void main(String[] args) {
        String flat_file_name = args[0]
        String database = args[1]
        String username = System.getenv('LOGNAME')
        String password = System.getenv("${username}_${database}".toUpperCase())
        String db_url = oraUrl + database

        xmlLinkage.each { java.lang.System.setProperty(it.name, it.value) }

        Sql.withInstance(db_url, username, password, oraDriver) { Sql sql ->
            sql.eachRow('select * from global_name') { GroovyResultSet row ->
                logger.info("${username}@${row.getAt('global_name')}: loading TUMORS ...")
            }
            def ld = new Loader(sql)
            new File(flat_file_name).withReader('utf-8') { reader ->
                int qty = ld.load(reader)
                logger.info("... loaded ${qty}")
            }
        }
    }

    private final Sql sql

    Loader(Sql sql) {
        this.sql = sql
    }

    int load(Reader flatReader) {
        int patQty = 0
        Patient p1

        logger.info("deleting from tumors...")
        sql.execute 'delete from tumors' // ISSUE: purge?
        sql.execute 'commit'

        def patReader = new PatientFlatReader(flatReader)
        NaaccrData naaccrData = patReader.getRootData()
        def dest = new Dest(sql, naaccrData, 0)

        sql.withBatch(sql_batch_size,
                      'insert into tumors (record_num, patients) values(?, ?)') {
            PS stmt ->

            while ((p1 = patReader.readPatient()) != null) {
                patQty += 1
                if (dest.addPatient(stmt, p1, patQty)) {
                    dest = new Dest(sql, naaccrData, dest.recordQty)
                }
            }
            dest.flush(stmt)
        }
        return patQty
    }

    class Dest {
        public final SQLXML xmlVal
        public final Writer stream
        public final PatientXmlWriter writer

        private int pending = 0
        int recordQty

        Dest(Sql sql, NaaccrData naaccrData, int qty) {
            xmlVal = sql.connection.createSQLXML()
            stream = xmlVal.setCharacterStream()
            writer = new PatientXmlWriter(stream, naaccrData)
            recordQty = qty
        }

        boolean addPatient(PS stmt, Patient p1, int patQty) {
            writer.writePatient(p1)
            pending += 1
            if (pending >= xml_batch_size) {
                if (patQty % (sql_batch_size * xml_batch_size) == 0) {
                    logger.info("${patQty} patients inserted (queued)...")
                }
                return flush(stmt)
            }
            return false
        }

        boolean flush(PS stmt) {
            if (pending == 0) {
                return false
            }
            writer.close()
            stream.close()
            stmt.addBatch([recordQty, xmlVal])
            recordQty += 1
            xmlVal.free()
            pending = 0
            true
        }
    }

    // help createSQLXML find XML parser
    // https://stackoverflow.com/a/12150962
    static List<Map<String, String>> xmlLinkage = [
        "javax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl",
        "javax.xml.parsers.SAXParserFactory=com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl",
        "javax.xml.transform.TransformerFactory=com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl"
    ].collect {
        String[] nv = it.split("=")
        ["name": nv[0], "value": nv[1]] as Map<String, String>
    }
}
