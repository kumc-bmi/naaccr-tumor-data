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
    static int batch_size = 1000
    static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)

    static void main(String[] args) {
        String flat_file_name = args[0]
        String database = args[1]
        String username = System.getenv('LOGNAME')
        String password = System.getenv("${username}_${database}".toUpperCase())
        String db_url = oraUrl + database

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
        int line = 0
        Patient p1

        sql.execute 'delete from tumors' // ISSUE: purge?
        sql.execute 'commit'

        sql.withBatch(batch_size,
                      'insert into tumors (line, tumor) values(?, ?)') {
            PS stmt ->
            def patReader = new PatientFlatReader(flatReader)
            NaaccrData naaccrData = patReader.getRootData()
            while ((p1 = patReader.readPatient()) != null) {
                def xmlVal = sql.connection.createSQLXML()
                xmlVal.setString(patientXML(p1, naaccrData))
                line += 1
                stmt.addBatch(line, xmlVal)
                if (line % batch_size == 0) {
                    logger.info("${line} lines inserted (queued)...")
                }
            }
        }
        return line - 1
    }

    /**
     * serailize Patient as XML format String.
     */
    static String patientXML(Patient p1, NaaccrData naaccrData) {
        def buf = new StringWriter()
        def writer = new PatientXmlWriter(buf, naaccrData)
        writer.writePatient(p1)
        writer.close()
        buf.toString()
    }
}
