import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.Sql
import groovy.transform.CompileStatic

import java.nio.file.Paths
import java.sql.Connection
import java.sql.SQLException
import java.util.logging.Logger

@CompileStatic
class Loader {
    static Logger logger = Logger.getLogger("")


    private final Sql _sql

    Loader(Sql sql) {
        _sql = sql
    }

    void runScript(URL input) {
        def edited = productSqlEdits(productName())

        input.withInputStream { stream ->
            new Scanner(stream).useDelimiter(";\n") each {
                String sql = edited(it)
                logger.fine("executing $input: $sql")
                try {
                    _sql.execute sql
                } catch (SQLException oops) {
                    if (sql.contains("drop table")) {
                        /* trundle on ... */
                    } else {
                        throw oops
                    }
                }
            }
        }
    }

    static Closure<String> productSqlEdits(productName) {
        if (productName == "Oracle") {
            return { String sql -> sql.replaceAll("drop table if exists", "drop table") }
        } else {
            return { String sql -> sql }
        }
    }

    void setSessionDateFormat() {
        if (productName() == "Oracle") {
            _sql.execute("alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD'")
        }
    }

    String productName() {
        String productName = '???'
        _sql.cacheConnection { Connection conn ->
            productName = conn.metaData.databaseProductName
        }
        productName
    }

    static int batchSize = 100

    int loadRaw(Reader input, String table) {
        String stmt = "insert into ${table} (record) values (?)"
        def qty = 0
        _sql.withBatch(batchSize, stmt) { BatchingPreparedStatementWrapper ps ->
            new Scanner(input).useDelimiter("\n") each { String it ->
                ps.addBatch([it as Object])
                qty += 1
            }
        }
        logger.info("inserted $qty records into $table")
        qty
    }

    JsonBuilder query(String sql) {
        def results = _sql.rows(sql)
        new JsonBuilder(results)
    }

    int load(Reader input) {
        def scanner = new Scanner(input)
        def parser = new JsonSlurper()
        def header = parser.parseText(scanner.nextLine())
        String stmt = header['sql']
        logger.info("batch insert: $stmt")

        setSessionDateFormat()

        def qty = 0
        _sql.withBatch(batchSize, stmt) { BatchingPreparedStatementWrapper ps ->
            List<Object> record
            while (scanner.hasNextLine()) {
                try {
                    record = parser.parseText(scanner.nextLine()) as List
                } catch (EOFException ignored) {
                    break
                }
                // logger.info("insert record: $record")
                ps.addBatch(record)
                qty += 1
            }
        }
        logger.info("inserted $qty records")
        qty
    }


    static void run_cli(DBConfig.CLI cli) {
        DBConfig account = cli.account()

        account.withSql { Sql sql ->
            def loader = new Loader(sql)

            String script = cli.arg("SCRIPT")
            String query = cli.arg("SQL")
            String table = cli.arg("TABLE")
            if (script) {
                def cwd = Paths.get(".").toAbsolutePath().normalize().toString()
                loader.runScript(new URL(new URL("file://$cwd/"), script))
            } else if (query) {
                def json = loader.query(query)
                System.out.withWriter {
                    json.writeTo(it)
                }
            } else if (table) {
                loader.loadRaw(new InputStreamReader(System.in), table)
            } else if (cli.flag("load")) {
                loader.load(new InputStreamReader(System.in))
            } else {
                throw new IllegalArgumentException(cli.opts.toString())
            }
            null
        }
    }
}
