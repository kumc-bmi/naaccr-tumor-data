class DBConfigTest extends GroovyTestCase {
    void 'test DBConfig happy path' () {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'java.sql.DriverManager', A1_USER: 'U1', A1_PASSWORD: 'sekret']
        def c = DBConfig.fromEnv('A1', { String it -> env1[it] })
        assert c.username == 'U1'
    }

    void 'test DBConfig misspelled env var' () {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'java.sql.DriverManager', A1_USERNAME: 'U1', A1_PASSWORD: 'sekret']
        shouldFail IllegalStateException, { -> DBConfig.fromEnv('A1', { String it -> env1[it] }) }
    }

    void 'test missing driver' () {
        def env1 = [A1_URL: 'URL1', A1_DRIVER: 'sqlserver.Driver.Thingy', A1_USERNAME: 'U1', A1_PASSWORD: 'sekret']
        shouldFail ClassNotFoundException, { -> DBConfig.fromEnv('A1', { String it -> env1[it] }) }
    }
}
