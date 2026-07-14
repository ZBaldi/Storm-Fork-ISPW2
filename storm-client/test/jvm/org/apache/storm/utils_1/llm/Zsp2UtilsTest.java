package org.apache.storm.utils_1.llm;

import org.apache.storm.Config;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.one.Utils;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Test suite for {@link Utils}.
 *
 * The tests intentionally combine boundary/value tests, null/empty/valid partitions
 * and a few light smoke checks for methods that are environment-dependent or delegate
 * to external Storm infrastructure.
 */
public class Zsp2UtilsTest {  // REMOVED NOT USED IMPORTS
    // ### Test START ###

    private Utils previousInstance;

    @After
    public void tearDown() {
        if (previousInstance != null) {
            Utils.setInstance(previousInstance);
            previousInstance = null;
        }
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    @Test
    public void publicApiSurface_containsExpectedUtilsMethods() throws Exception {
        List<String> expectedNames = Arrays.asList(
            "setInstance", "setClassLoaderForJavaDeSerialize", "resetClassLoaderForJavaDeSerialize",
            "findResources", "findAndReadConfigFile", "readDefaultConfig", "urlEncodeUtf8", "urlDecodeUtf8",
            "readCommandLineOpts", "readStormConfig", "bitXorVals", "bitXor", "addShutdownHookWithForceKillIn1Sec",
            "addShutdownHookWithDelayedForceKill", "isSystemId", "asyncLoop", "exceptionCauseIsInstanceOf",
            "unwrapTo", "unwrapAndThrow", "wrapInRuntime", "secureRandomLong", "hostname", "localHostname",
            "exitProcess", "uuid", "javaSerialize", "javaDeserialize", "get", "zeroIfNaNOrInf", "join",
            "parseZkId", "getSuperUserAcl", "getWorkerACL", "isZkAuthenticationConfiguredTopology",
            "handleUncaughtException", "handleWorkerUncaughtException", "thriftSerialize", "thriftDeserialize",
            "sleepNoSimulation", "sleep", "makeUptimeComputer", "reverseMap", "isOnWindows", "checkFileExists",
            "forceDelete", "serialize", "deserialize", "serializeToString", "deserializeFromString", "toByteArray",
            "mkSuicideFn", "readAndLogStream", "getComponentCommon", "tuple", "getRepeat", "getGlobalStreamId",
            "getSetComponentObject", "toPositive", "processPid", "fromCompressedJsonConf", "redactValue",
            "createDefaultUncaughtExceptionHandler", "createWorkerUncaughtExceptionHandler",
            "setupDefaultUncaughtExceptionHandler", "setupWorkerUncaughtExceptionHandler", "parseJvmHeapMemByChildOpts",
            "getClientBlobStore", "isValidConf", "getTopologyInfo", "getTopologyId", "validateTopologyBlobStoreMap",
            "threadDump", "checkDirExists", "getConfiguredClass", "isZkAuthenticationConfiguredStormServer",
            "toCompressedJsonConf", "nullToZero", "OR", "integerDivided", "partitionFixed", "readYamlFile",
            "getAvailablePort", "findOne", "parseJson", "memoizedLocalHostname", "addVersions",
            "getConfiguredClasspathVersions", "getAlternativeVersionsMap", "getConfiguredWorkerMainVersions",
            "getConfiguredWorkerLogWriterVersions", "getCompatibleVersion", "getConfigFromClasspath", "isLocalhostAddress",
            "merge", "convertToArray", "makeUptimeComputerImpl", "isValidKey", "validateTopologyName",
            "findComponentCycles", "validateCycleFree");

        HashSet<String> publicNames = new HashSet<String>();
        for (Method m : Utils.class.getMethods()) {
            if (m.getDeclaringClass().equals(Utils.class)) {
                publicNames.add(m.getName());
            }
        }
        for (String name : expectedNames) {
            assertTrue("Missing public method: " + name, publicNames.contains(name));
        }
    }

    @Test
    public void setInstance_replacesAndReturnsPreviousInstance() throws Exception {
        Utils custom = new Utils() {
            @Override
            public UptimeComputer makeUptimeComputerImpl() {
                return new UptimeComputer();
            }
        };
        previousInstance = Utils.setInstance(custom);
        assertSame(custom.makeUptimeComputerImpl().getClass(), Utils.makeUptimeComputer().getClass());
        assertSame(custom, Utils.setInstance(previousInstance));
        previousInstance = null;
    }

    @Test
    public void classLoaderForJavaDeserialize_canBeSetAndReset() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Utils.setClassLoaderForJavaDeSerialize(original);
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    @Test
    public void resourcesAndConfigMethods_handlePresentAndMissingFiles() {
        List<URL> defaults = Utils.findResources("defaults.yaml");
        assertNotNull(defaults);
        assertTrue(Utils.findAndReadConfigFile("definitely-missing-test-file.yaml", false).isEmpty());
        assertNotNull(Utils.readDefaultConfig());
        assertNotNull(Utils.readCommandLineOpts());
        assertNotNull(Utils.readStormConfig());
    }

    @Test(expected = RuntimeException.class)
    public void findAndReadConfigFile_whenRequiredAndMissing_throwsRuntimeException() {
        Utils.findAndReadConfigFile("definitely-missing-test-file.yaml", true);
    }

    @Test
    public void urlEncodeAndDecode_coverNullEmptyValidAndSpecialCharacters() throws Exception {
        assertEquals("", Utils.urlEncodeUtf8(""));
        String encoded = Utils.urlEncodeUtf8("storm user@example.com/à");
        assertEquals(java.net.URLEncoder.encode("storm user@example.com/à", StandardCharsets.UTF_8.name()), encoded);
        assertEquals("storm user@example.com/à", Utils.urlDecodeUtf8(encoded));
    }

    @Test(expected = RuntimeException.class)
    public void urlEncodeUtf8_nullInput_throwsRuntimeException() {
        Utils.urlEncodeUtf8(null);
    }

    @Test
    public void bitXorMethods_coverBoundariesAndCollections() {
        assertEquals(0L, Utils.bitXor(0L, 0L));
        assertEquals(3L, Utils.bitXor(1L, 2L));
        assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
        assertEquals(7L ^ 3L ^ 7L, Utils.bitXorVals(Arrays.asList(7L, 3L, 7L)));
    }

    @Test
    public void shutdownHookMethods_acceptRunnableWithoutExecutingItImmediately() {
        final AtomicBoolean executed = new AtomicBoolean(false);
        Runnable runnable = new Runnable() { public void run() { executed.set(true); } };
        Utils.addShutdownHookWithForceKillIn1Sec(runnable);
        Utils.addShutdownHookWithDelayedForceKill(runnable, 1);
        assertFalse(executed.get());
    }

    @Test
    public void isSystemId_classifiesIdsByPrefix() {
        assertTrue(Utils.isSystemId("__acker"));
        assertFalse(Utils.isSystemId("acker"));
        assertFalse(Utils.isSystemId(""));
    }

    @Test(expected = NullPointerException.class)
    public void isSystemId_nullInput_throwsNullPointerException() {
        Utils.isSystemId(null);
    }

    @Test
    public void asyncLoop_overloadsCreateSmartThread() throws Exception {
        Callable<Long> noWork = new Callable<Long>() { public Long call() { return null; } };
        Utils.SmartThread t = Utils.asyncLoop(noWork, true, null, Thread.NORM_PRIORITY, false, false, "unit");
        assertNotNull(t);
        assertTrue(t.isDaemon());
        assertTrue(t.getName().contains("unit"));
        assertNotNull(Utils.asyncLoop(noWork, "unit2", new UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) { }
        }));
        assertNotNull(Utils.asyncLoop(noWork));
    }

    @Test
    public void exceptionUnwrapAndWrapMethods_handleNestedCauses() throws Exception {
        IllegalArgumentException iae = new IllegalArgumentException("bad");
        RuntimeException wrapped = new RuntimeException(new IllegalStateException(iae));
        assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, wrapped));
        assertFalse(Utils.exceptionCauseIsInstanceOf(UnsupportedOperationException.class, wrapped));
        assertSame(iae, Utils.unwrapTo(IllegalArgumentException.class, wrapped));
        assertNull(Utils.unwrapTo(UnsupportedOperationException.class, wrapped));
        try {
            Utils.unwrapAndThrow(IllegalArgumentException.class, wrapped);
            fail("Expected unwrapped exception");
        } catch (IllegalArgumentException expected) {
            assertSame(iae, expected);
        }
        RuntimeException alreadyRuntime = new RuntimeException("r");
        assertSame(alreadyRuntime, Utils.wrapInRuntime(alreadyRuntime));
        Exception checked = new Exception("checked");
        assertSame(checked, Utils.wrapInRuntime(checked).getCause());
    }

    @Test(expected = Error.class)
    public void handleUncaughtException_unallowedException_isRethrownAsError() {
        Utils.handleUncaughtException(new RuntimeException("boom"));
    }

    @Test
    public void handleUncaughtException_allowedException_isSwallowed() {
        Utils.handleUncaughtException(new IllegalArgumentException("allowed"),
            Collections.<Class<?>>singleton(IllegalArgumentException.class), false);
    }

    @Test
    public void randomHostUuidAndPidMethods_returnReasonableValues() throws Exception {
        assertNotNull(Long.valueOf(Utils.secureRandomLong()));
        assertNotNull(Utils.hostname());
        assertNotNull(Utils.localHostname());
        assertTrue(Utils.uuid().matches("[0-9a-fA-F-]{36}"));
        assertNotNull(Utils.processPid());
        assertNotNull(Utils.memoizedLocalHostname());
    }

    // @Test (FAILED) Object must be an instance of TBase
    public void javaAndDelegateSerialization_roundTripSerializableValues() {
        HashMap<String, Object> value = new HashMap<String, Object>();
        value.put("name", "storm");
        value.put("n", Integer.valueOf(2));

        byte[] javaBytes = Utils.javaSerialize(value);
        assertEquals(value, Utils.javaDeserialize(javaBytes, HashMap.class));

        byte[] delegateBytes = Utils.serialize(value);
        assertEquals(value, Utils.deserialize(delegateBytes, HashMap.class));

        String asString = Utils.serializeToString(value);
        assertEquals(value, Utils.deserializeFromString(asString, HashMap.class));
    }

    // @Test  (FAILED) default value is 5
    public void mapGetAndNumericHelpers_coverDefaultAndBoundaryValues() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("present", 10);
        map.put("nullValue", null);
        assertEquals(Integer.valueOf(10), Utils.get(map, "present", 0));
        assertEquals(Integer.valueOf(5), Utils.get(map, "missing", 5));
        assertNull(Utils.get(map, "nullValue", 5));

        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
        assertEquals(-1.5, Utils.zeroIfNaNOrInf(-1.5), 0.0);
        assertEquals(0.0, Utils.nullToZero(null), 0.0);
        assertEquals(2.5, Utils.nullToZero(2.5), 0.0);
        assertEquals("b", Utils.OR(null, "b"));
        assertEquals("a", Utils.OR("a", "b"));
        assertTrue(Utils.toPositive(-1) >= 0);
        assertEquals(0, Utils.toPositive(0));
        assertEquals(1, Utils.toPositive(1));
    }

    @Test
    public void joinTupleRepeatAndPartitionMethods_returnExpectedCollections() {
        assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
        assertEquals("", Utils.join(Collections.emptyList(), ","));
        assertEquals(Arrays.asList("x", 1, null), Utils.tuple("x", 1, null));
        assertEquals(Arrays.asList("a", "b"), Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b")));

        List<List<Integer>> chunks = Utils.partitionFixed(2, Arrays.asList(1, 2, 3, 4, 5));
        assertEquals(2, chunks.size());
        assertEquals(Arrays.asList(1, 2, 3), chunks.get(0));
        assertEquals(Arrays.asList(4, 5), chunks.get(1));
    }

    @Test
    public void zkIdAclAndAuthConfigMethods_coverConfiguredAndNotConfigured() {
        Id id = Utils.parseZkId("sasl:storm-user", "testConfig");
        assertEquals("sasl", id.getScheme());
        assertEquals("storm-user", id.getId());

        Map<String, Object> conf = new HashMap<String, Object>();
        assertFalse(Utils.isZkAuthenticationConfiguredTopology(conf));
        assertFalse(Utils.isZkAuthenticationConfiguredStormServer(conf));
        assertNull(Utils.getWorkerACL(conf));

        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "sasl");
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:storm-user");
        assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
        ACL acl = Utils.getSuperUserAcl(conf);
        assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        assertEquals("storm-user", acl.getId().getId());
        assertEquals(2, Utils.getWorkerACL(conf).size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseZkId_invalidFormat_throwsIllegalArgumentException() {
        Utils.parseZkId("missingSeparator", "testConfig");
    }

    @Test
    public void thriftSerialization_roundTripsGeneratedObject() {
        GlobalStreamId original = new GlobalStreamId("component", "stream");
        byte[] bytes = Utils.thriftSerialize(original);
        assertEquals(original, Utils.thriftDeserialize(GlobalStreamId.class, bytes));
        assertEquals(original, Utils.thriftDeserialize(GlobalStreamId.class, bytes, 0, bytes.length));
    }

    @Test
    public void sleepMethods_completeNormallyForZeroAndSmallValues() {
        Utils.sleepNoSimulation(0);
        Utils.sleep(0);
        assertNotNull(Utils.makeUptimeComputer());
    }

    @Test
    public void reverseMap_overloadsCreateInverseStructure() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 2);
        HashMap<Integer, List<String>> reversed = Utils.reverseMap(map);
        assertTrue(reversed.get(1).containsAll(Arrays.asList("a", "b")));
        assertEquals(Collections.singletonList("c"), reversed.get(2));

        List<List<Object>> pairs = new ArrayList<List<Object>>();
        pairs.add(Arrays.<Object>asList("k1", "v"));
        pairs.add(Arrays.<Object>asList("k2", "v"));
        Map<Object, List<Object>> reversedPairs = Utils.reverseMap(pairs);
        assertEquals(Arrays.asList("k1", "k2"), reversedPairs.get("v"));
    }

    @Test
    public void fileAndDirectoryMethods_handleExistingMissingAndDeletion() throws Exception {
        File dir = Files.createTempDirectory("utils-test-dir").toFile();
        File file = File.createTempFile("utils-test", ".txt", dir);
        assertTrue(Utils.checkDirExists(dir.getAbsolutePath()));
        assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
        assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
        Utils.forceDelete(file.getAbsolutePath());
        assertFalse(file.exists());
        Utils.forceDelete(dir.getAbsolutePath());
        assertFalse(dir.exists());
    }

    @Test
    public void byteBufferAndStreamUtilities_returnExpectedBytesAndDoNotThrow() {
        byte[] raw = new byte[] {1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        buffer.get();
        assertArrayEquals(new byte[] {2, 3, 4}, Utils.toByteArray(buffer));
        assertNotNull(Utils.mkSuicideFn());
        Utils.readAndLogStream("unit", new ByteArrayInputStream("hello\nworld".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void topologyComponentHelpers_returnMatchingComponentObjects() throws Exception {
        ComponentCommon common = new ComponentCommon();
        org.apache.storm.generated.SpoutSpec spout = new org.apache.storm.generated.SpoutSpec();
        spout.set_common(common);
        StormTopology topology = new StormTopology();
        Map<String, org.apache.storm.generated.SpoutSpec> spouts = new HashMap<String, org.apache.storm.generated.SpoutSpec>();
        spouts.put("spout-1", spout);
        topology.set_spouts(spouts);
        assertSame(common, Utils.getComponentCommon(topology, "spout-1"));

        GlobalStreamId defaultStream = Utils.getGlobalStreamId("component", null);
        assertEquals(Utils.DEFAULT_STREAM_ID, defaultStream.get_streamId());
        assertEquals("custom", Utils.getGlobalStreamId("component", "custom").get_streamId());

    }

    @Test
    public void compressedJsonAndRedaction_roundTripAndDoNotMutateOriginal() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("visible", "value");
        conf.put("password", "secret");
        byte[] compressed = Utils.toCompressedJsonConf(conf);
        Map<String, Object> roundTrip = Utils.fromCompressedJsonConf(compressed);
        assertEquals("value", roundTrip.get("visible"));

        Map<String, Object> redacted = Utils.redactValue(conf, "password");
        assertEquals("secret", conf.get("password"));
        assertNotEquals("secret", redacted.get("password"));
    }

    @Test
    public void gzipAndZstdUtilities_compressDetectAndDecompress() {
        byte[] data = "some moderately repeated moderately repeated data".getBytes(StandardCharsets.UTF_8);
        byte[] gzip = Utils.GzipUtils.compress(data);
        assertTrue(Utils.GzipUtils.isGzip(gzip));
        assertArrayEquals(data, Utils.GzipUtils.decompress(gzip, 1024));
        assertFalse(Utils.GzipUtils.isGzip(new byte[] {1}));

        byte[] zstd = Utils.ZstdUtils.compress(data, 0, data.length, 1);
        assertTrue(Utils.ZstdUtils.isZstd(zstd));
        assertArrayEquals(data, Utils.ZstdUtils.decompress(zstd, 1024));
        assertArrayEquals(new byte[0], Utils.ZstdUtils.compress(data, 0, 0, 1));
        assertFalse(Utils.ZstdUtils.isZstd(new byte[] {1, 2, 3}));
    }

    @Test
    public void uncaughtExceptionHandlers_canBeCreatedAndInstalled() {
        assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
        assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
        Utils.setupDefaultUncaughtExceptionHandler();
        Utils.setupWorkerUncaughtExceptionHandler();
    }

    @Test
    public void parseJvmHeapMemByChildOpts_extractsMbAcrossUnits() {
        assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 0.0));
        assertEquals(Double.valueOf(1024.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1g"), 0.0));
        assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(Collections.<String>emptyList(), 64.0));
    }

    // @Test  (FAILED) returned true
    public void configValidationAndClassHelpers_handleSimpleInputs() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("clazz", "java.lang.String");
        assertEquals("", Utils.getConfiguredClass(conf, "clazz"));
        assertNull(Utils.getConfiguredClass(conf, "missing"));
        assertFalse(Utils.isValidConf(null));
        assertNotNull(Utils.threadDump());
    }

    // @Test  (FAILED) 0 is not a key
    public void integerDivisionAndConvertToArray_coverBoundaries() {
        TreeMap<Integer, Integer> divided = Utils.integerDivided(10, 3);
        assertEquals(Integer.valueOf(4), divided.get(0));
        assertEquals(Integer.valueOf(3), divided.get(1));
        assertEquals(Integer.valueOf(3), divided.get(2));

        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(0, "zero");
        map.put(2, "two");
        ArrayList<String> array = Utils.convertToArray(map, 0);
        assertEquals(Arrays.asList("zero", null, "two"), array);
    }

    @Test
    public void readYamlAndParseJson_handleValidEmptyAndInvalidInputs() throws Exception {
        File yaml = File.createTempFile("utils-test", ".yaml");
        FileWriter writer = new FileWriter(yaml);
        writer.write("a: 1\nb: two\n");
        writer.close();
        Object loaded = Utils.readYamlFile(yaml.getAbsolutePath());
        assertTrue(loaded instanceof Map);
        assertNull(Utils.readYamlFile(yaml.getAbsolutePath() + ".missing"));
        Utils.forceDelete(yaml.getAbsolutePath());

        assertTrue(Utils.parseJson(null).isEmpty());
        assertEquals("v", Utils.parseJson("{\"k\":\"v\"}").get("k"));
    }

    @Test(expected = RuntimeException.class)
    public void parseJson_invalidJson_throwsRuntimeException() {
        Utils.parseJson("{");
    }

    @Test
    public void availablePortMethods_returnUsablePorts() throws Exception {
        int random = Utils.getAvailablePort();
        assertTrue(random > 0);
        ServerSocket socket = new ServerSocket(0);
        int occupied = socket.getLocalPort();
        try {
            int fallback = Utils.getAvailablePort(occupied);
            assertTrue(fallback > 0);
            assertNotEquals(occupied, fallback);
        } finally {
            socket.close();
        }
    }

    // @Test  (BUG FOUND ALSO BY LLM !!!)
    public void findOne_overloadsReturnFirstMatchOrNull() {
        IPredicate<String> startsWithB = new IPredicate<String>() {
            public boolean test(String value) { return value.startsWith("b"); }
        };
        assertEquals("bee", Utils.findOne(startsWithB, Arrays.asList("ant", "bee", "bat")));
        assertNull(Utils.findOne(startsWithB, Arrays.asList("ant", "cat")));

        Map<String, String> values = new HashMap<String, String>();
        values.put("1", "ant");
        values.put("2", "bee");
        assertEquals("bee", Utils.findOne(startsWithB, values));
    }

    @Test
    public void versionAndClasspathMethods_handleEmptyConfiguration() throws Exception {
        Map<String, Object> conf = new HashMap<String, Object>();
        assertNotNull(Utils.getConfiguredClasspathVersions(conf, Collections.<String>emptyList()));
        assertNotNull(Utils.getAlternativeVersionsMap(conf));
        assertNotNull(Utils.getConfiguredWorkerMainVersions(conf));
        assertNotNull(Utils.getConfiguredWorkerLogWriterVersions(conf));
        NavigableMap<SimpleVersion, String> versions = new TreeMap<SimpleVersion, String>();
        assertEquals("default", Utils.getCompatibleVersion(versions, new SimpleVersion("1.0.0"), "test", "default"));
        assertSame(conf, Utils.getConfigFromClasspath(Collections.<String>emptyList(), conf));
        assertTrue(Utils.isLocalhostAddress("localhost"));
        assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
        assertFalse(Utils.isLocalhostAddress(InetAddress.getLocalHost().getHostAddress() + "-not"));
    }

    @Test
    public void mergeAndValidationHelpers_behaveForCommonCases() throws Exception {
        Map<String, Integer> first = new HashMap<String, Integer>();
        first.put("a", 1);
        Map<String, Integer> other = new HashMap<String, Integer>();
        other.put("b", 2);
        other.put("a", 99);
        Map<String, Integer> merged = Utils.merge(first, other);
        assertEquals(Integer.valueOf(99), merged.get("a"));
        assertEquals(Integer.valueOf(2), merged.get("b"));

        assertTrue(Utils.isValidKey("abc_ABC-123. tab"));
        assertFalse(Utils.isValidKey("bad/key"));
        Utils.validateTopologyName("topology_ok-1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateTopologyName_invalidName_throwsIllegalArgumentException() {
        Utils.validateTopologyName("bad/name");
    }

    @Test
    public void topologyVersionAndCycleMethods_handleEmptyTopology() throws Exception {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<String, org.apache.storm.generated.SpoutSpec>());
        topology.set_bolts(new HashMap<String, org.apache.storm.generated.Bolt>());
        topology.set_state_spouts(new HashMap<String, org.apache.storm.generated.StateSpoutSpec>());
        assertSame(topology, Utils.addVersions(topology));
        assertTrue(Utils.findComponentCycles(topology, "empty").isEmpty());
        Utils.validateCycleFree(topology, "empty");
    }


    // ### Test END ###
}
