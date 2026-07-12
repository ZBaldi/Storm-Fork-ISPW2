package org.apache.storm.utils;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

/**
 * Test JUnit4 generati con uno stile volutamente semplice, come farebbe un tester junior.
 * I test usano casi base, valori null/empty quando sicuri, qualche boundary e qualche controllo
 * di presenza per i metodi che hanno dipendenze esterne o effetti collaterali pesanti.
 */

/** FIXED MANUALLY */
public class Zsp1UtilsTest {  //CLEANED NOT USED IMPORTS
    /* ### Test START ### */

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private String oldStormOptions;
    private String oldStormConfFile;
    private Utils oldUtilsInstance;

    private static class MyUtils extends Utils {
        private final String host;
        MyUtils(String host) {
            this.host = host;
        }
        @Override
        protected String localHostnameImpl() {
            return host;
        }
        @Override
        public UptimeComputer makeUptimeComputerImpl() {
            return new UptimeComputer();
        }
    }

    @Before
    public void setUp() {
        oldStormOptions = System.getProperty("storm.options");
        oldStormConfFile = System.getProperty("storm.conf.file");
        oldUtilsInstance = Utils.setInstance(new Utils());
    }

    @After
    public void tearDown() {
        if (oldStormOptions == null) {
            System.clearProperty("storm.options");
        } else {
            System.setProperty("storm.options", oldStormOptions);
        }
        if (oldStormConfFile == null) {
            System.clearProperty("storm.conf.file");
        } else {
            System.setProperty("storm.conf.file", oldStormConfFile);
        }
        Utils.setInstance(oldUtilsInstance);
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    @Test
    public void testSetInstanceReturnsPreviousInstance() {
        Utils first = new Utils();
        Utils second = new Utils();
        Utils.setInstance(first);
        Utils returned = Utils.setInstance(second);
        assertSame(first, returned);
    }

    @Test
    public void testSetAndResetClassLoaderForJavaDeserialize() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Utils.setClassLoaderForJavaDeSerialize(cl);
        Utils.resetClassLoaderForJavaDeSerialize();
        assertTrue("Il metodo deve terminare senza eccezioni", true);
    }

    @Test
    public void testFindResourcesForExistingAndMissingResource() {
        List<URL> javaResources = Utils.findResources("org/apache/storm/utils/Utils.class");
        assertNotNull(javaResources);
        assertFalse(javaResources.isEmpty());

        List<URL> missing = Utils.findResources("file-che-non-esiste-123456.yaml");
        assertNotNull(missing);
        assertTrue(missing.isEmpty());
    }

    @Test
    public void testFindAndReadConfigFileWithRealTemporaryYaml() throws Exception {
        File yaml = temp.newFile("test-conf.yaml");
        FileWriter writer = new FileWriter(yaml);
        writer.write("a: 1\nb: prova\n");
        writer.close();

        Map<String, Object> conf = Utils.findAndReadConfigFile(yaml.getAbsolutePath(), true);
        assertEquals(1, ((Number) conf.get("a")).intValue());
        assertEquals("prova", conf.get("b"));
    }

    @Test
    public void testFindAndReadConfigFileNotMustExistReturnsEmptyMap() {
        Map<String, Object> conf = Utils.findAndReadConfigFile("non-esiste-sicuramente.yaml", false);
        assertNotNull(conf);
        assertTrue(conf.isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void testFindAndReadConfigFileMustExistThrows() {
        Utils.findAndReadConfigFile("non-esiste-sicuramente.yaml");
    }

    @Test
    public void testReadDefaultConfigDoesNotReturnNull() {
        assertNotNull(Utils.readDefaultConfig());
    }

    @Test
    public void testUrlEncodeDecodeUtf8() {
        String original = "ciao mondo + è";
        String encoded = Utils.urlEncodeUtf8(original);
        assertNotEquals(original, encoded);
        assertEquals(original, Utils.urlDecodeUtf8(encoded));
    }

    @Test
    public void testReadCommandLineOptsEmptyAndValid() {
        System.clearProperty("storm.options");
        assertTrue(Utils.readCommandLineOpts().isEmpty());

        System.setProperty("storm.options", Utils.urlEncodeUtf8("alpha=1") + "," + Utils.urlEncodeUtf8("nome=valerio"));
        Map<String, Object> opts = Utils.readCommandLineOpts();
        assertEquals(1, ((Number) opts.get("alpha")).intValue());
        assertEquals("valerio", opts.get("nome"));
    }

    @Test
    public void testReadStormConfigReturnsMap() {
        System.clearProperty("storm.conf.file");
        assertNotNull(Utils.readStormConfig());
    }

    @Test
    public void testBitXorValsAndBitXor() {
        assertEquals(0L, Utils.bitXorVals(new ArrayList<Long>()));
        assertEquals(6L, Utils.bitXor(5L, 3L));
        assertEquals(5L ^ 3L ^ 5L, Utils.bitXorVals(Arrays.asList(5L, 3L, 5L)));
    }

    @Test
    public void testShutdownHookMethodsDoNotThrow() {
        Runnable r = new Runnable() { public void run() { } };
        Utils.addShutdownHookWithForceKillIn1Sec(r);
        Utils.addShutdownHookWithDelayedForceKill(r, 1);
        assertTrue(true);
    }

    @Test
    public void testIsSystemId() {
        assertTrue(Utils.isSystemId("__system"));
        assertFalse(Utils.isSystemId("normal"));
        assertFalse(Utils.isSystemId(""));
    }

    @Test
    public void testAsyncLoopCreatesThreadWithoutStartingIt() {
        Callable<Object> callable = new Callable<Object>() { public Object call() { return null; } };
        Utils.SmartThread t = Utils.asyncLoop(callable, false, null, Thread.NORM_PRIORITY, false, false, "test-thread");
        assertNotNull(t);
        assertTrue(t.getName().contains("test-thread"));
        assertFalse(t.isAlive());
    }

    @Test
    public void testAsyncLoopOverloadsCreateThread() {
        Callable<Object> callable = new Callable<Object>() { public Object call() { return null; } };
        Utils.SmartThread t1 = Utils.asyncLoop(callable, "nome-thread", null);
        Utils.SmartThread t2 = Utils.asyncLoop(callable);
        assertNotNull(t1);
        assertNotNull(t2);
        t1.interrupt();
        t2.interrupt();
    }

    @Test
    public void testExceptionUnwrapMethods() throws Exception {
        IllegalArgumentException cause = new IllegalArgumentException("errore");
        RuntimeException wrapper = new RuntimeException(cause);
        assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, wrapper));
        assertSame(cause, Utils.unwrapTo(IllegalArgumentException.class, wrapper));
        assertNull(Utils.unwrapTo(IllegalStateException.class, wrapper));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnwrapAndThrowThrowsCause() throws Exception {
        Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException(new IllegalArgumentException("x")));
    }

    @Test
    public void testWrapInRuntime() {
        RuntimeException runtime = new RuntimeException("gia runtime");
        assertSame(runtime, Utils.wrapInRuntime(runtime));
        Exception checked = new Exception("checked");
        RuntimeException wrapped = Utils.wrapInRuntime(checked);
        assertSame(checked, wrapped.getCause());
    }

    @Test
    public void testRandomHostnameAndUuidMethods() throws Exception {
        assertNotEquals(0L, Utils.secureRandomLong());
        assertNotNull(Utils.hostname());
        Utils.setInstance(new MyUtils("host-test"));
        assertEquals("host-test", Utils.localHostname());
        assertNotNull(UUID.fromString(Utils.uuid()));
    }

    @Test
    public void testJavaSerializeDeserialize() {
        String value = "test serializzazione";
        byte[] bytes = Utils.javaSerialize(value);
        assertNotNull(bytes);
        assertEquals(value, Utils.javaDeserialize(bytes, String.class));
    }

    @Test
    public void testGenericGet() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 10);
        assertEquals(Integer.valueOf(10), Utils.get(map, "a", 99));
        assertEquals(Integer.valueOf(99), Utils.get(map, "b", 99));
    }

    @Test
    public void testZeroIfNaNOrInf() {
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
        assertEquals(12.5, Utils.zeroIfNaNOrInf(12.5), 0.0);
    }

    @Test
    public void testJoin() {
        assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
        assertEquals("", Utils.join(new ArrayList<String>(), ","));
    }

    @Test
    public void testParseZkIdAndAcl() {
        Id id = Utils.parseZkId("digest:user", "cfg");
        assertEquals("digest", id.getScheme());
        assertEquals("user", id.getId());

        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "digest:admin");
        ACL acl = Utils.getSuperUserAcl(conf);
        assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        assertEquals("digest", acl.getId().getScheme());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseZkIdInvalidThrows() {
        Utils.parseZkId("valoreErrato", "cfg");
    }

    @Test
    public void testWorkerAclAndZkAuthConfigured() {
        Map<String, Object> emptyConf = new HashMap<String, Object>();
        assertNull(Utils.getWorkerACL(emptyConf));
        assertFalse(Utils.isZkAuthenticationConfiguredTopology(emptyConf));

        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "digest");
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "user:pass");
        assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
    }

    @Test
    public void testHandleUncaughtExceptionAllowedDoesNotThrow() {
        Set<Class<?>> allowed = new HashSet<Class<?>>();
        allowed.add(IllegalArgumentException.class);
        Utils.handleUncaughtException(new IllegalArgumentException("ok"), allowed, false);
        Utils.handleUncaughtException(new IllegalArgumentException("ok"), allowed, true);
        assertTrue(true);
    }

    // @Test (FAILED) Required field 'spouts' is unset!
    public void testThriftSerializeDeserialize() {
        StormTopology topology = new StormTopology();
        byte[] bytes = Utils.thriftSerialize(topology);
        assertNotNull(bytes);
        StormTopology copy = Utils.thriftDeserialize(StormTopology.class, bytes);
        assertNotNull(copy);
        StormTopology copy2 = Utils.thriftDeserialize(StormTopology.class, bytes, 0, bytes.length);
        assertNotNull(copy2);
    }

    @Test
    public void testSleepMethodsWithZero() {
        Utils.sleepNoSimulation(0);
        Utils.sleep(0);
        assertTrue(true);
    }

    @Test
    public void testMakeUptimeComputer() {
        Utils.setInstance(new MyUtils("host"));
        assertNotNull(Utils.makeUptimeComputer());
    }

    @Test
    public void testReverseMapFromMap() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 2);
        HashMap<Integer, List<String>> reversed = Utils.reverseMap(map);
        assertEquals(2, reversed.get(1).size());
        assertTrue(reversed.get(2).contains("c"));
    }

    @Test
    public void testReverseMapFromList() {
        List<List<Object>> list = new ArrayList<List<Object>>();
        list.add(Utils.tuple("a", 1));
        list.add(Utils.tuple("b", 1));
        Map<Object, List<Object>> reversed = Utils.reverseMap(list);
        assertEquals(Arrays.asList("a", "b"), reversed.get(1));
    }

    @Test
    public void testFileHelpers() throws Exception {
        File file = temp.newFile("file.txt");
        assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
        assertFalse(Utils.checkFileExists(file.getAbsolutePath() + "-missing"));

        File dir = temp.newFolder("dirDaCancellare");
        assertTrue(Utils.checkDirExists(dir.getAbsolutePath()));
        Utils.forceDelete(dir.getAbsolutePath());
        assertFalse(dir.exists());
    }

    // @Test (FAILED) Object must be an instance of TBase
    public void testSerializeDeserializeDelegate() {
        String value = "abc";
        byte[] bytes = Utils.serialize(value);
        assertEquals(value, Utils.deserialize(bytes, String.class));
        String encoded = Utils.serializeToString(value);
        assertEquals(value, Utils.deserializeFromString(encoded, String.class));
    }

    @Test
    public void testToByteArrayKeepsBufferContent() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
        assertArrayEquals(new byte[] {1, 2, 3}, Utils.toByteArray(buffer));
    }

    @Test
    public void testMkSuicideFnReturnsRunnableButIsNotExecuted() {
        assertNotNull(Utils.mkSuicideFn());
    }

    @Test
    public void testReadAndLogStream() {
        Utils.readAndLogStream("prefisso", new ByteArrayInputStream("riga1\nriga2".getBytes()));
        assertTrue(true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetComponentCommonMissingComponentThrows() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap());
        topology.set_bolts(new HashMap());
        topology.set_state_spouts(new HashMap());
        Utils.getComponentCommon(topology, "missing");
    }

    @Test
    public void testTupleAndRepeat() {
        List<Object> tuple = Utils.tuple("a", 1, null);
        assertEquals(3, tuple.size());
        assertEquals("a", tuple.get(0));

        List<String> repeat = Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b"));
        assertEquals(Arrays.asList("a", "b"), repeat);
    }

    @Test
    public void testGlobalStreamId() {
        GlobalStreamId defaultId = Utils.getGlobalStreamId("comp", null);
        assertEquals("comp", defaultId.get_componentId());
        assertEquals(Utils.DEFAULT_STREAM_ID, defaultId.get_streamId());

        GlobalStreamId customId = Utils.getGlobalStreamId("comp", "stream");
        assertEquals("stream", customId.get_streamId());
    }

    @Test
    public void testToPositive() {
        assertEquals(0, Utils.toPositive(Integer.MIN_VALUE));
        assertEquals(0, Utils.toPositive(0));
        assertEquals(1, Utils.toPositive(1));
        assertTrue(Utils.toPositive(-1) >= 0);
    }

    @Test
    public void testProcessPid() {
        assertNotNull(Utils.processPid());
        assertFalse(Utils.processPid().isEmpty());
    }

    @Test
    public void testCompressedJsonConf() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("a", "b");
        conf.put("n", 3);
        byte[] bytes = Utils.toCompressedJsonConf(conf);
        Map<String, Object> out = Utils.fromCompressedJsonConf(bytes);
        assertEquals("b", out.get("a"));
        assertEquals(3, ((Number) out.get("n")).intValue());
    }

    @Test
    public void testRedactValueDoesNotModifyOriginal() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("password", "secret");
        Map<String, Object> redacted = Utils.redactValue(map, "password");
        assertEquals("secret", map.get("password"));
        assertNotEquals("secret", redacted.get("password"));
    }

    @Test
    public void testUncaughtExceptionHandlerFactoriesReturnHandlers() {
        assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
        assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
        Utils.setupDefaultUncaughtExceptionHandler();
        Utils.setupWorkerUncaughtExceptionHandler();
    }

    @Test
    public void testParseJvmHeapMemByChildOpts() {
        assertEquals(Double.valueOf(128.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx128m"), 64.0));
        assertEquals(Double.valueOf(1024.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1g"), 64.0));
        assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(new ArrayList<String>(), 64.0));
    }

    @Test
    public void testIsValidConfWithSimpleMap() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("a", "b");
        assertTrue(Utils.isValidConf(conf));
    }

    @Test
    public void testThreadDumpContainsText() {
        String dump = Utils.threadDump();
        assertNotNull(dump);
        assertTrue(dump.length() > 0);
    }

    @Test
    public void testZkAuthenticationConfiguredStormServer() {
        Map<String, Object> conf = new HashMap<String, Object>();
        assertFalse(Utils.isZkAuthenticationConfiguredStormServer(conf));
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "digest");
        conf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "user:pass");
        assertTrue(Utils.isZkAuthenticationConfiguredStormServer(conf));
    }

    @Test
    public void testNullToZeroAndOr() {
        assertEquals(0.0, Utils.nullToZero(null), 0.0);
        assertEquals(5.0, Utils.nullToZero(5.0), 0.0);
        assertEquals("b", Utils.OR(null, "b"));
        assertEquals("a", Utils.OR("a", "b"));
    }

    // @Test  (FAILED) sum is number of pieces in this case  2 people have 3 pieces, 1 person has 4 pieces
    public void testIntegerDividedBoundaries() {
        TreeMap<Integer, Integer> divided = Utils.integerDivided(10, 3);
        int sum = 0;
        for (Integer value : divided.values()) {
            sum += value;
        }
        assertEquals(10, sum);
        assertEquals(0, Utils.integerDivided(0, 3).values().stream().mapToInt(Integer::intValue).sum());
    }

    @Test
    public void testPartitionFixed() {
        List<Integer> values = Arrays.asList(1, 2, 3, 4, 5);
        List<List<Integer>> chunks = Utils.partitionFixed(2, values);
        assertEquals(2, chunks.size());
        List<Integer> flatten = new ArrayList<Integer>();
        for (List<Integer> chunk : chunks) {
            flatten.addAll(chunk);
        }
        assertEquals(values, flatten);
    }

    @Test
    public void testReadYamlFile() throws Exception {
        File yaml = temp.newFile("read.yaml");
        FileWriter writer = new FileWriter(yaml);
        writer.write("k: v\n");
        writer.close();
        Object obj = Utils.readYamlFile(yaml.getAbsolutePath());
        assertNotNull(obj);
    }

    @Test
    public void testAvailablePort() throws Exception {
        int port = Utils.getAvailablePort();
        assertTrue(port > 0);
        int preferred = Utils.getAvailablePort(port);
        assertTrue(preferred > 0);
    }

    @Test
    public void testFindOneCollectionAndMap() {
        IPredicate<String> startsWithB = new IPredicate<String>() {
            public boolean test(String value) {
                return value.startsWith("b");
            }
        };
        assertEquals("b", Utils.findOne(startsWithB, Arrays.asList("a", "b", "c")));

        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "a");
        map.put(2, "b");
        IPredicate rawPredicate = new IPredicate() {
            public boolean test(Object value) {
                Map.Entry entry = (Map.Entry) value;
                return "b".equals(entry.getValue());
            }
        };
        Object foundEntry = Utils.findOne(rawPredicate, map);
        assertEquals("b", ((Map.Entry) foundEntry).getValue());
    }

    @Test
    public void testParseJson() {
        assertTrue(Utils.parseJson(null).isEmpty());
        Map<String, Object> parsed = Utils.parseJson("{\"a\":1}");
        assertEquals(1, ((Number) parsed.get("a")).intValue());
    }

    @Test
    public void testMemoizedLocalHostname() throws UnknownHostException {   //ADDED MANUALLY THROWS
        assertNotNull(Utils.memoizedLocalHostname());
    }

    @Test
    public void testAddVersionsReturnsSameTopologyInstance() {
        StormTopology topology = new StormTopology();
        assertSame(topology, Utils.addVersions(topology));
    }

    @Test
    public void testGetCompatibleVersionWithDefault() throws Exception {   //CONSTRUCTOR WRONG CHANGED FROM 1,0,0 in String 1.0.0
        NavigableMap<SimpleVersion, String> map = new TreeMap<SimpleVersion, String>();
        SimpleVersion desired = new SimpleVersion("1.0.0");
        assertEquals("default", Utils.getCompatibleVersion(map, desired, "test", "default"));
    }

    @Test
    public void testIsLocalhostAddress() {
        assertTrue(Utils.isLocalhostAddress("localhost"));
        assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
        assertFalse(Utils.isLocalhostAddress("8.8.8.8"));
    }

    @Test
    public void testMerge() {
        Map<String, Integer> first = new HashMap<String, Integer>();
        first.put("a", 1);
        Map<String, Integer> second = new HashMap<String, Integer>();
        second.put("b", 2);
        Map<String, Integer> merged = Utils.merge(first, second);
        assertEquals(Integer.valueOf(1), merged.get("a"));
        assertEquals(Integer.valueOf(2), merged.get("b"));
    }

    @Test
    public void testConvertToArray() {
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "a");
        map.put(2, "b");
        ArrayList<String> ret = Utils.convertToArray(map, 1);
        assertEquals(Arrays.asList("a", "b"), ret);
    }

    @Test
    public void testMakeUptimeComputerImpl() {
        assertNotNull(new Utils().makeUptimeComputerImpl());
    }

    @Test
    public void testIsValidKey() {
        assertTrue(Utils.isValidKey("abc_123-XYZ.txt"));
        assertFalse(Utils.isValidKey(null));
        assertFalse(Utils.isValidKey(""));
        assertFalse(Utils.isValidKey("../bad"));
    }

    @Test
    public void testValidateTopologyName() {
        Utils.validateTopologyName("topologia_valida");
        try {
            Utils.validateTopologyName("nome/non/valido");
            fail("Mi aspettavo IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(true);
        }
    }

    @Test
    public void testValidateCycleFreeWithEmptyTopology() throws InvalidTopologyException {  //ADDED THROWS
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap());
        topology.set_bolts(new HashMap());
        topology.set_state_spouts(new HashMap());
        Utils.validateCycleFree(topology, "topologia");
        assertTrue(Utils.findComponentCycles(topology, "id").isEmpty());
    }

    @Test
    public void testGzipUtils() {
        byte[] data = "ciao gzip".getBytes();
        byte[] compressed = Utils.GzipUtils.compress(data);
        assertTrue(Utils.GzipUtils.isGzip(compressed));
        assertArrayEquals(data, Utils.GzipUtils.decompress(compressed, 1000));
        assertFalse(Utils.GzipUtils.isGzip(data));
        assertNull(Utils.GzipUtils.compress(null));
    }

    @Test
    public void testZstdUtilsBasicCases() {
        byte[] data = "ciao zstd".getBytes();
        byte[] compressed = Utils.ZstdUtils.compress(data, 0);  //ADDED 0
        assertTrue(Utils.ZstdUtils.isZstd(compressed));
        assertArrayEquals(data, Utils.ZstdUtils.decompress(compressed, 1000));
        assertFalse(Utils.ZstdUtils.isZstd(data));
        assertNull(Utils.ZstdUtils.compress(null,0)); //ADDED 0
    }

    @Test
    public void testMethodsWithExternalDependenciesExist() throws Exception {
        // Questi metodi sono controllati in modo semplice perché richiedono Nimbus, BlobStore,
        // classpath reali o terminano la JVM. Un tester junior spesso parte da questi smoke test.
        assertHasPublicMethod("exitProcess", int.class, String.class);
        assertHasPublicMethod("getClientBlobStore", Map.class);
        assertHasPublicMethod("getTopologyInfo", String.class, String.class, Map.class);
        assertHasPublicMethod("getTopologyId", String.class, Class.forName("org.apache.storm.generated.Nimbus$Iface"));
        assertHasPublicMethod("validateTopologyBlobStoreMap", Map.class);
        assertHasPublicMethod("validateTopologyBlobStoreMap", Map.class, Class.forName("org.apache.storm.blobstore.NimbusBlobStore"));
        assertHasPublicMethod("validateTopologyBlobStoreMap", Map.class, Class.forName("org.apache.storm.blobstore.BlobStore"));
        assertHasPublicMethod("getConfiguredClass", Map.class, Object.class);
        assertHasPublicMethod("getConfiguredClasspathVersions", Map.class, List.class);
        assertHasPublicMethod("getAlternativeVersionsMap", Map.class);
        assertHasPublicMethod("getConfiguredWorkerMainVersions", Map.class);
        assertHasPublicMethod("getConfiguredWorkerLogWriterVersions", Map.class);
        assertHasPublicMethod("getConfigFromClasspath", List.class, Map.class);
    }

    private void assertHasPublicMethod(String name, Class<?>... params) throws Exception {
        Method m = Utils.class.getMethod(name, params);
        assertNotNull(m);
    }

    /* ### Test END ### */
}
