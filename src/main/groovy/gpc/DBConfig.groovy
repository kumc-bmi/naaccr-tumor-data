package gpc

import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

@CompileStatic
@Slf4j
class DBConfig {
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
            log.info("looking up JDBC driver: $driver")
            Class.forName(driver)
        } catch (Exception noDriver) {
            log.error("cannot load driver", driver, noDriver)
            throw noDriver
        }
        Properties properties = new Properties()
        properties.setProperty('url', config("url"))
        properties.setProperty('user', config("username"))
        properties.setProperty('password', config('password'))
        properties
    }

    static DBConfig inMemoryDB(String databaseName, boolean reset = false) {
        String url = "jdbc:h2:mem:${databaseName};create=true"
        String driver = 'org.h2.Driver'
        log.debug("looking up in-memory JDBC driver: $driver")
        Class.forName(driver)
        DBConfig it = new DBConfig(url, new Properties(),
                { String ignored, Properties _ -> DriverManager.getConnection(url) })
        if (reset) {
            it.withSql({ Sql sql -> sql.execute('drop all objects') })
        }
        it
    }


    interface Task {
        boolean complete()

        void run()
    }

    static interface IO {
        Properties fetchProperties(String name)

        void exit(int status)

        Connection getConnection(String url, Properties properties)

        Path resolve(String other)
    }

    static class CLI {
        protected final Map opts
        private final IO io
        private Properties configCache = null

        CLI(Map opts, IO io) {
            this.opts = opts
            this.io = io
        }

        boolean flag(String target) {
            opts[target] == true
        }

        String arg(String target, String fallback = null) {
            if (!opts.containsKey(target) || opts[target] == null) {
                return fallback
            }
            opts[target]
        }

        List<Path> files(String target) {
            opts[target].collect { String fn -> io.resolve(fn) }
        }

        Path pathArg(String target) {
            io.resolve(arg(target))
        }

        int intArg(String target) {
            Integer.parseInt(arg(target))
        }

        String property(String target,
                        String fallback = 'throw') {
            String value = getConfig().getProperty(target)
            if (value == null) {
                if (fallback == 'throw') {
                    throw new IllegalArgumentException(target)
                }
                value = fallback
            }
            value
        }

        Path pathProperty(String target) {
            io.resolve(property(target))
        }

        DBConfig account() {
            Properties config = getConfig()
            try {
                config = jdbcProperties(config)
            } catch (IllegalStateException oops) {
                log.warn("Config missing property: $oops")
                io.exit(1)
            } catch (ClassNotFoundException oops) {
                log.warn("driver not found (fix CLASSPATH?): $oops")
                io.exit(1)
            }
            String url = config.getProperty('url')
            log.info("DB: $url")
            new DBConfig(url, config, io::getConnection)
        }

        // a bit of a kludge
        private Properties getConfig() {
            if (configCache != null) {
                return configCache
            }
            String db = arg("--db")
            if (!db) {
                log.warn("expected --db=PROPS")
                io.exit(1)
            }
            log.info("getting config from $db")
            try {
                configCache = io.fetchProperties(db)
            } catch (Exception oops) {
                log.warn("cannot load properties from ${db}: $oops")
            }
            configCache
        }

        static String mustGetEnv(Map<String, String> env, String key) {
            String value = env[key]
            if (value == null) {
                log.warn("getEnv($key) failed")
                throw new RuntimeException(key)
            }
            value
        }
    }
}