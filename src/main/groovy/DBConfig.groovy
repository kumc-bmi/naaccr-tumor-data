import groovy.transform.CompileStatic
import groovy.transform.Immutable

import java.util.logging.Logger

@CompileStatic
@Immutable
class DBConfig {
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
        try {
            Class.forName(driver)
        } catch (Exception noDriver) {
            logger.exiting("cannot load driver", driver, noDriver)
            throw noDriver
        }
        new DBConfig(url: config("URL"), driver: driver,
                username: config("USER"), password: new Password(value: config("PASSWORD")))
    }

    static DBConfig memdb() {
        final Map<String, String> env1 = [MEM_URL : 'jdbc:h2:mem:A1;create=true', MEM_DRIVER: 'org.h2.Driver',
                                          MEM_USER: 'SA', MEM_PASSWORD: '']

        DBConfig.fromEnv('MEM', { String it -> env1[it] })
    }

    static class CLI {
        protected final String[] args
        private final Closure<String> getenv
        private final Closure exit

        CLI(String[] _args, Closure<String> _getenv, Closure _exit) {
            args = _args
            getenv = _getenv
            exit = _exit
        }

        int argIx(String target) {
            args.findIndexOf({ it == target })
        }

        String arg(String target, String fallback=null) {
            int ix = argIx(target)
            if (ix < 0 || ix + 1 >= args.length) {
                return fallback
            }
            args[ix + 1]
        }

        DBConfig account() {
            String account = arg("--account")
            if (!account) {
                logger.warning("Usage: java -jar JAR --account A [--run abc.sql]")
                exit(1)
            }
            DBConfig config = null
            try {
                config = fromEnv(account, getenv)
            } catch (IllegalStateException oops) {
                logger.warning("Config missing from environment: $oops")
                exit(1)
            } catch (ClassNotFoundException oops) {
                logger.warning("driver not found (fix CLASSPATH?): $oops")
                exit(1)
            }
            logger.info("$account config: $config")
            config
        }
    }

    @Immutable
    static class Password {
        final String value

        @Override
        String toString() {
            return "...${value.length()}..."
        }
    }
}