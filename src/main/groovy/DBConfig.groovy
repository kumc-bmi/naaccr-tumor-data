import groovy.sql.Sql
import groovy.transform.CompileStatic

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.logging.Logger

@CompileStatic
class DBConfig {
    static Logger logger = Logger.getLogger("")

    final String url
    final Properties connectionProperties
    private final Closure<Connection> connect

    DBConfig(String url, Properties connectionProperties, Closure<Connection> connect) {
        this.url = url
        this.connectionProperties = connectionProperties
        this.connect = connect
    }

    def <V> V withSql(Closure<V> thunk) throws SQLException {
        Connection conn = connect(url, connectionProperties)
        Sql sql = new Sql(conn)
        try {
            thunk(sql)
        } finally {
            sql.close()
            conn.close()
        }
    }

    static Properties jdbcProperties(Properties dbProperties) {
        Closure<String> config = {
            def name = "db.${it}"
            def val = dbProperties.getProperty(name)
            if (val == null) {
                throw new IllegalStateException(name)
            }
            val
        }
        def driver = config("driver")
        try {
            Class.forName(driver)
        } catch (Exception noDriver) {
            logger.exiting("cannot load driver", driver, noDriver)
            throw noDriver
        }
        Properties properties = new Properties()
        properties.setProperty('url', config("url"))
        properties.setProperty('user', config("user"))
        properties.setProperty('password', config('password'))
        properties
    }

    static DBConfig inMemoryDB(String databaseName, boolean reset=false) {
        String url = "jdbc:h2:mem:${databaseName};create=true"
        DBConfig it = new DBConfig(url, new Properties(),
                { String ignored, Properties _ -> DriverManager.getConnection(url) })
        if (reset) {
            it.withSql({ Sql sql -> sql.execute('drop all objects') })
        }
        it
    }

    static class CLI {
        protected final Map opts
        private final Closure<Properties> getProperties
        private final Closure<Void> exit
        private final Closure<Connection> getConnection

        CLI(Map opts, Closure<Properties> getProperties, Closure exit, Closure<Connection> getConnection) {
            this.opts = opts
            this.getProperties = getProperties
            this.exit = exit
            this.getConnection = getConnection
        }

        boolean flag(String target) {
            opts[target] == true
        }

        String arg(String target, String fallback=null) {
            if (!opts.containsKey(target) || opts[target] == null) {
                return fallback
            }
            opts[target]
        }

        DBConfig account() {
            String db = arg("--db")
            if (!db) {
                logger.warning("expected --db=PROPS")
                exit(1)
            }
            Properties config = null
            logger.info("getting config from $db")
            try {
                config = getProperties(db)
            } catch (IllegalStateException oops) {
                logger.warning("Config missing property: $oops")
                exit(1)
            } catch (ClassNotFoundException oops) {
                logger.warning("driver not found (fix CLASSPATH?): $oops")
                exit(1)
            }
            String url = config.getProperty('db.url')
            logger.info("$db: $url")
            new DBConfig(url, jdbcProperties(config), getConnection)
        }
    }
}