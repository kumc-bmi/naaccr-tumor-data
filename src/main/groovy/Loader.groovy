import groovy.json.JsonBuilder
import groovy.json.JsonParserType
import groovy.json.JsonSlurper
import groovy.sql.BatchingPreparedStatementWrapper
import groovy.sql.GroovyResultSet
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.transform.Immutable
import java.util.logging.Logger
import java.io.Reader
import java.io.BufferedReader
import java.io.InputStreamReader

@Immutable
class Password {
    final String value

    @Override
    String toString() {
        return "...${value.length()}..."
    }
}


@CompileStatic
class Loader {
    static Logger logger = Logger.getLogger("")

    @Immutable
    public static class DBConfig {
        final String url
        final String driver
        final String username
        final Password password

        static Logger logger = Logger.getLogger("")

        static DBConfig fromEnv(String account, Closure<String> getenv) {
            logger.info("getting config for $account")
            Closure<String> config = {
                def name = "${account}_${it}"
                def val = getenv(name)
                if (val == null) {
                    throw new IllegalStateException(name)
                }
                val
            }
            def driver = config("DRIVER")
            Class.forName(driver)
            new DBConfig(url: config("URL"), driver: driver,
                    username: config("USER"), password: new Password(value: config("PASSWORD")))
        }
    }

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
                } catch (java.sql.SQLException oops) {
                    if (sql.contains("drop table")) {
                        /* trundle on ... */
                    } else {
                        throw oops
                    }
                }
            }
        }
    }

    Closure<String> productSqlEdits(productName) {
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
        String productName
        _sql.cacheConnection { java.sql.Connection conn ->
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
                } catch (EOFException) {
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

    static void main(String[] args) {
        def argIx = { String target ->
            args.findIndexOf({ it == target })
        }
        def arg = { String target ->
            int ix = argIx(target)
            if (ix < 0 || ix + 1 >= args.length) {
                return null
            }
            args[ix + 1]
        }
        def account = arg("--account")
        if (!account) {
            logger.warning("Usage: java -jar loader.jar --account A [--run abc.sql]")
            System.exit(1)
        }
        def config
        try {
            config = DBConfig.fromEnv(account, { String name -> System.getenv(name) })
        } catch (IllegalStateException oops) {
            logger.warning("Config missing from environment: $oops")
            System.exit(1)
        } catch (ClassNotFoundException oops) {
            logger.warning("driver not found (fix CLASSPATH?): $oops")
            System.exit(1)
        }
        logger.info("$account config: $config")
        Sql.withInstance(config.url, config.username, config.password.value, config.driver) { Sql sql ->
            def loader = new Loader(sql)

            def script = arg("--run")
            if (script) {
                def cwd = java.nio.file.Paths.get(".").toAbsolutePath().normalize().toString()
                loader.runScript(new URL(new URL("file://$cwd/"), script))
            }

            def query = arg("--query")
            if (query) {
                def json = loader.query(query)
                System.out << json
            }

            String table = arg("--loadRaw")
            if (table) {
                loader.loadRaw(new java.io.InputStreamReader(System.in), table)
            }

            if (argIx("--load") >= 0) {
                loader.load(new java.io.InputStreamReader(System.in))
            }
        }
    }
}
