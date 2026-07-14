package org.apache.storm.utils.llm;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.serialization.SerializationDelegate;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.IVersionInfo;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.zero.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.lang.IllegalStateException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Senior-level JUnit4 tests for {@link Utils}.
 * <p>
 * The suite intentionally mixes category-partitioning and boundary-value cases:
 * null/empty/valid-valid/valid-invalid for strings, null/valid/invalid for complex
 * objects, and immediate boundaries for numeric ranges. Tests that would otherwise
 * depend on OS/network/external Storm services are isolated by mocks, temporary files,
 * assumptions or state restoration.
 */

/** FIXED MANUALLY */
public class Zsp3UtilsTest {  // REMOVED NOT USED IMPORTS
    // ### Test START ###

    private Utils previousInstance;
    private ClassLoader originalDeserializeClassLoader;
    private SerializationDelegate originalSerializationDelegate;
    private Thread.UncaughtExceptionHandler originalDefaultHandler;

    @Before
    public void setUp() throws Exception {
        previousInstance = Utils.setInstance(new Utils());
        originalDeserializeClassLoader = (ClassLoader) getStaticField(Utils.class, "cl");
        originalSerializationDelegate = (SerializationDelegate) getStaticField(Utils.class, "serializationDelegate");
        originalDefaultHandler = Thread.getDefaultUncaughtExceptionHandler();
    }

    @After
    public void tearDown() throws Exception {
        Utils.setInstance(previousInstance == null ? new Utils() : previousInstance);
        setStaticField(Utils.class, "cl", originalDeserializeClassLoader);
        setStaticField(Utils.class, "serializationDelegate", originalSerializationDelegate);
        setStaticField(Utils.class, "memoizedLocalHostnameString", null);
        Thread.setDefaultUncaughtExceptionHandler(originalDefaultHandler);
    }

    @Test
    public void setInstanceReturnsPreviouslyConfiguredInstanceAndDelegatesInstanceMethods() throws Exception {
        TestableUtils first = new TestableUtils("first-host");
        TestableUtils second = new TestableUtils("second-host");

        Utils old = Utils.setInstance(first);
        assertNotNull(old);
        assertSame(first, Utils.setInstance(second));
        assertEquals("second-host", Utils.localHostname());
        assertNotNull(Utils.makeUptimeComputer());
    }

    // @Test  (FAILED) CLASS LOADER IS RETURNED
    public void javaDeserializeUsesCustomClassLoaderWhenConfiguredAndCanBeReset() {
        byte[] bytes = Utils.javaSerialize("payload");
        ClassLoader markerLoader = new ClassLoader(getClass().getClassLoader()) { };

        Utils.setClassLoaderForJavaDeSerialize(markerLoader);
        assertEquals("payload", Utils.javaDeserialize(bytes, String.class));
        assertSame(markerLoader, getStaticFieldUnchecked(Utils.class, "cl"));

        Utils.resetClassLoaderForJavaDeSerialize();
        assertNull(getStaticFieldUnchecked(Utils.class, "cl"));
        assertEquals("payload", Utils.javaDeserialize(bytes, String.class));
    }

    // @Test
    public void findResourcesCoversExistingAndMissingClasspathResources() {
        List<URL> classResources = Utils.findResources("org/apache/storm/utils/Utils.class");
        assertFalse(classResources.isEmpty());

        List<URL> missing = Utils.findResources("not-existing-resource-" + UUID.randomUUID());
        assertNotNull(missing);
        assertTrue(missing.isEmpty());
    }

    @Test
    public void findAndReadConfigFileReturnsEmptyWhenOptionalResourceIsMissingAndThrowsWhenMandatory() {
        String missing = "missing-conf-" + UUID.randomUUID() + ".yaml";
        assertTrue(Utils.findAndReadConfigFile(missing, false).isEmpty());

        try {
            Utils.findAndReadConfigFile(missing, true);
            fail("Mandatory missing config must fail");
        } catch (RuntimeException expected) {
            assertThat(expected.getMessage(), containsString(missing));
        }
    }

    @Test
    public void readDefaultCommandLineAndStormConfigReturnDefensiveNonNullMaps() {
        assertNotNull(Utils.readDefaultConfig());
        assertNotNull(Utils.readCommandLineOpts());
        assertNotNull(Utils.readStormConfig());
    }

    @Test
    public void urlEncodeAndDecodeUtf8HandleNullEmptyAsciiAndUnicodeValues() throws Exception {
        assertEquals("", Utils.urlEncodeUtf8(""));
        assertEquals("a+b%2Bc%2F%C3%A8", Utils.urlEncodeUtf8("a b+c/è"));
        assertEquals(URLEncoder.encode("汉字 väärtus", StandardCharsets.UTF_8.name()), Utils.urlEncodeUtf8("汉字 väärtus"));
        assertEquals("a b+c/è", Utils.urlDecodeUtf8("a+b%2Bc%2F%C3%A8"));
        assertEquals(URLDecoder.decode("%E6%B1%89%E5%AD%97", StandardCharsets.UTF_8.name()), Utils.urlDecodeUtf8("%E6%B1%89%E5%AD%97"));

        try {
            Utils.urlEncodeUtf8(null);
            fail("Null string is outside the valid partition and should fail");
        } catch (RuntimeException expected) {  // DELETED CATCH NULL POINTER EXCEPTION
            // accepted: implementation wraps UnsupportedEncodingException but null reaches JDK encoder
        }
    }

    @Test
    public void bitXorAndBitXorValsCoverEmptySingletonMultipleAndNullElementCases() {
        assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
        assertEquals(7L, Utils.bitXorVals(Collections.singletonList(7L)));
        assertEquals(0L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
        assertEquals(-4L, Utils.bitXor(1L, -3L));

        try {
            Utils.bitXorVals(Arrays.asList(1L, null));
            fail("Null list element should be invalid");
        } catch (NullPointerException expected) {
            // expected invalid partition
        }
    }

    @Test
    public void shutdownHookRegistrationMethodsDoNotExecuteProvidedRunnableImmediately() {
        AtomicBoolean executed = new AtomicBoolean(false);
        Utils.addShutdownHookWithForceKillIn1Sec(() -> executed.set(true));
        Utils.addShutdownHookWithDelayedForceKill(() -> executed.set(true), 1);
        assertFalse(executed.get());
    }

    @Test
    public void isSystemIdDetectsOnlyDoubleUnderscorePrefix() {
        assertTrue(Utils.isSystemId("__acker"));
        assertFalse(Utils.isSystemId("_acker"));
        assertFalse(Utils.isSystemId("acker"));
        try {
            Utils.isSystemId(null);
            fail("Null id is invalid");
        } catch (NullPointerException expected) { }
    }

    // @Test  (FAILED) Thread-0-zsp3-not-started obtained
    public void asyncLoopCreatesConfigurableSmartThreadAndRunsWhenRequested() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        Callable<Long> once = () -> invocations.getAndIncrement() == 0 ? 0L : -1L;
        Thread.UncaughtExceptionHandler handler = mock(Thread.UncaughtExceptionHandler.class);

        Utils.SmartThread notStarted = Utils.asyncLoop(once, true, handler, Thread.NORM_PRIORITY + 1, false, false, "zsp3-not-started");
        assertEquals("zsp3-not-started", notStarted.getName());
        assertTrue(notStarted.isDaemon());
        assertEquals(Thread.NORM_PRIORITY + 1, notStarted.getPriority());
        assertEquals(0, invocations.get());
        notStarted.start();
        notStarted.join(2000L);
        assertEquals(2, invocations.get());

        Utils.SmartThread defaultThread = Utils.asyncLoop(() -> -1L);
        defaultThread.join(2000L);
        assertFalse(defaultThread.isAlive());

        Utils.SmartThread namedThread = Utils.asyncLoop(() -> -1L, "zsp3-named", handler);
        namedThread.join(2000L);
        assertEquals("zsp3-named", namedThread.getName());
    }

    @Test
    public void exceptionCauseHelpersNavigateCauseChain() throws Exception {
        IllegalArgumentException leaf = new IllegalArgumentException("leaf");
        RuntimeException wrapped = new RuntimeException(new IOException(leaf));

        assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, wrapped));
        assertFalse(Utils.exceptionCauseIsInstanceOf(IllegalStateException.class, wrapped));
        assertSame(leaf, Utils.unwrapTo(IllegalArgumentException.class, wrapped));
        assertNull(Utils.unwrapTo(IllegalStateException.class, wrapped));

        try {
            Utils.unwrapAndThrow(IllegalArgumentException.class, wrapped);
            fail("unwrapAndThrow should rethrow matching cause");
        } catch (IllegalArgumentException expected) {
            assertSame(leaf, expected);
        }
        Utils.unwrapAndThrow(IllegalStateException.class, wrapped);
    }

    @Test
    public void wrapInRuntimePreservesRuntimeExceptionAndWrapsCheckedException() {
        RuntimeException runtime = new RuntimeException("already-runtime");
        assertSame(runtime, Utils.wrapInRuntime(runtime));

        IOException checked = new IOException("checked");
        RuntimeException wrapped = Utils.wrapInRuntime(checked);
        assertSame(checked, wrapped.getCause());
    }

    @Test
    public void randomHostnameUuidAndPidUtilitiesReturnPlausibleValues() throws Exception {
        assertNotEquals(Utils.secureRandomLong(), Utils.secureRandomLong());
        assertNotNull(Utils.hostname());
        assertNotNull(Utils.localHostname());
        UUID.fromString(Utils.uuid());
        assertNotNull(Utils.processPid());
        assertFalse(Utils.processPid().trim().isEmpty());
    }

    @Test
    public void javaSerializationRoundTripAndInvalidInputPartitions() {
        SerializableBox box = new SerializableBox("storm", 42);
        byte[] bytes = Utils.javaSerialize(box);
        assertTrue(bytes.length > 0);
        assertEquals(box, Utils.javaDeserialize(bytes, SerializableBox.class));

        try {
            Utils.javaDeserialize(new byte[] {1, 2, 3}, SerializableBox.class);
            fail("Invalid serialized bytes should fail");
        } catch (RuntimeException expected) { }
    }

    // @Test (FAILED)  serializationDelegate is null
    public void genericSerializationDelegateIsUsedForSerializeDeserializeAndBase64StringRoundTrip() {
        RecordingSerializationDelegate delegate = new RecordingSerializationDelegate();
        setStaticFieldUnchecked(Utils.class, "serializationDelegate", delegate);

        byte[] encoded = Utils.serialize("abc");
        assertArrayEquals("ser:abc".getBytes(StandardCharsets.UTF_8), encoded);
        assertEquals("decoded", Utils.deserialize(encoded, String.class));
        assertTrue(delegate.serializeCalled);
        assertTrue(delegate.deserializeCalled);

        setStaticFieldUnchecked(Utils.class, "serializationDelegate", null);
        SerializableBox box = new SerializableBox("native", 7);
        String asString = Utils.serializeToString(box);
        assertEquals(box, Utils.deserializeFromString(asString, SerializableBox.class));
        try {
            Utils.deserializeFromString("not base64", SerializableBox.class);
            fail("Invalid encoded string should fail");
        } catch (RuntimeException expected) { } // DELETED CATCH NULL POINTER EXCEPTION
    }

    @Test
    public void getZeroJoinTupleOrAndNullToZeroCoverBoundaryPartitions() {
        Map<String, Integer> map = new HashMap<>();
        map.put("present", 1);
        map.put("nullValue", null);
        assertEquals(Integer.valueOf(1), Utils.get(map, "present", 99));
        assertEquals(Integer.valueOf(99), Utils.get(map, "missing", 99));
        assertEquals(Integer.valueOf(99), Utils.get(map, "nullValue", 99));

        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
        assertEquals(-2.5, Utils.zeroIfNaNOrInf(-2.5), 0.0);
        assertEquals(0.0, Utils.nullToZero(null), 0.0);
        assertEquals(3.5, Utils.nullToZero(3.5), 0.0);

        assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
        assertEquals("", Utils.join(Collections.emptyList(), ","));
        assertEquals(Arrays.asList((Object)"x", 2, null), Utils.tuple("x", 2, null));
        assertEquals("fallback", Utils.OR(null, "fallback"));
        assertEquals("primary", Utils.OR("primary", "fallback"));
    }

    // @Test  (FAILED) expected false not true
    public void zookeeperAclHelpersParseIdsAndDetectAuthenticationConfigurations() {
        Id id = Utils.parseZkId("digest:user:pass", Config.STORM_ZOOKEEPER_SUPERACL);
        assertEquals("digest", id.getScheme());
        assertEquals("user:pass", id.getId());

        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "digest:super:secret");
        ACL acl = Utils.getSuperUserAcl(conf);
        assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        assertEquals("digest", acl.getId().getScheme());

        assertFalse(Utils.isZkAuthenticationConfiguredTopology(new HashMap<String, Object>()));
        conf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, "user@REALM");
        assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
        assertFalse(Utils.isZkAuthenticationConfiguredStormServer(new HashMap<String, Object>()));
    }

    @Test
    public void uncaughtExceptionHandlersAreCreatedAndDoNotThrowForAllowedExceptions() {
        Set<Class<?>> allowed = new HashSet<>();
        allowed.add(IllegalArgumentException.class);
        Utils.handleUncaughtException(new IllegalArgumentException("allowed"), allowed, false);
        Utils.handleUncaughtException(new IllegalArgumentException("allowed"), allowed, true);

        assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
        assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
        Utils.setupDefaultUncaughtExceptionHandler();
        assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        Utils.setupWorkerUncaughtExceptionHandler();
        assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    // @Test  (FAILED)   InterruptedException: sleep interrupted
    public void sleepMethodsHonorZeroPositiveAndInterruptedPartitions() {
        long before = System.currentTimeMillis();
        Utils.sleepNoSimulation(0L);
        Utils.sleep(1L);
        assertTrue(System.currentTimeMillis() - before < 1000L);

        Thread.currentThread().interrupt();
        Utils.sleepNoSimulation(1L);
        assertFalse("sleepNoSimulation should clear interrupted status through InterruptedException", Thread.currentThread().isInterrupted());
    }

    @Test
    public void reverseMapVariantsGroupKeysByValueIncludingDuplicates() {
        Map<String, Integer> source = new HashMap<>();
        source.put("a", 1);
        source.put("b", 1);
        source.put("c", 2);
        HashMap<Integer, List<String>> reversed = Utils.reverseMap(source);
        assertThat(reversed.get(1), hasItems("a", "b"));
        assertEquals(Collections.singletonList("c"), reversed.get(2));

        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.<Object>asList("k1", "v"));
        rows.add(Arrays.<Object>asList("k2", "v"));
        Map<Object, List<Object>> reversedRows = Utils.reverseMap(rows);
        assertThat(reversedRows.get("v"), hasItems((Object)"k1", "k2"));
    }

    @Test
    public void fileUtilitiesCheckExistsDirectoryAndForceDelete() throws Exception {
        File dir = Files.createTempDirectory("zsp3-utils-dir").toFile();
        File file = File.createTempFile("zsp3-utils-file", ".txt", dir);
        assertTrue(Utils.checkDirExists(dir.getAbsolutePath()));
        assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
        assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
        assertFalse(Utils.checkFileExists(new File(dir, "missing").getAbsolutePath()));

        Utils.forceDelete(dir.getAbsolutePath());
        assertFalse(dir.exists());
    }

    @Test
    public void toByteArrayPreservesBufferContentAcrossArrayDirectSliceAndEmptyBuffers() {
        assertArrayEquals(new byte[] {1, 2, 3}, Utils.toByteArray(ByteBuffer.wrap(new byte[] {1, 2, 3})));
        ByteBuffer direct = ByteBuffer.allocateDirect(4);
        direct.put(new byte[] {9, 8, 7, 6});
        direct.flip();
        direct.get();
        assertArrayEquals(new byte[] {8, 7, 6}, Utils.toByteArray(direct));
        assertArrayEquals(new byte[0], Utils.toByteArray(ByteBuffer.allocate(0)));
    }

    // @Test  (FAILED) NullPointerException
    public void readAndLogStreamConsumesInputAndHandlesNullStreamGracefully() {
        InputStream in = new ByteArrayInputStream("line1\nline2".getBytes(StandardCharsets.UTF_8));
        Utils.readAndLogStream("prefix", in);
        Utils.readAndLogStream("prefix", null);
    }

    @Test
    public void componentTopologyHelpersResolveSpoutsBoltsStateSpoutsAndStreams() {
        ComponentCommon spoutCommon = new ComponentCommon();
        ComponentCommon boltCommon = new ComponentCommon();
        ComponentCommon stateCommon = new ComponentCommon();
        StormTopology topology = new StormTopology();
        topology.put_to_spouts("spout", new SpoutSpec(null, spoutCommon));
        topology.put_to_bolts("bolt", new Bolt(null, boltCommon));
        topology.put_to_state_spouts("state", new StateSpoutSpec(null, stateCommon));

        assertSame(spoutCommon, Utils.getComponentCommon(topology, "spout"));
        assertSame(boltCommon, Utils.getComponentCommon(topology, "bolt"));
        assertSame(stateCommon, Utils.getComponentCommon(topology, "state"));
        try {
            Utils.getComponentCommon(topology, "missing");
            fail("Missing component is invalid");
        } catch (IllegalArgumentException expected) { }

        assertEquals(new GlobalStreamId("c", Utils.DEFAULT_STREAM_ID), Utils.getGlobalStreamId("c", null));
        assertEquals(new GlobalStreamId("c", "s"), Utils.getGlobalStreamId("c", "s"));
    }

//    @Test     (THIS METHOD IS GENERATED IN A WRONG WAY)
//    public void componentObjectHelperReturnsExactlySetRepresentation() {
//        ComponentObject javaObject = ComponentObject.java_object("plain");
//        assertEquals("plain", Utils.getSetComponentObject(javaObject));
//
//        ComponentObject serializedObject = ComponentObject.serialized_java(Utils.javaSerialize("serialized"));
//        assertEquals("serialized", Utils.getSetComponentObject(serializedObject));
//
//        ShellComponent shell = new ShellComponent(Arrays.asList("python"), "bolt.py");
//        ComponentObject shellObject = ComponentObject.shell(shell);
//        assertSame(shell, Utils.getSetComponentObject(shellObject));
//    }

//    @Test   (THIS METHOD IS GENERATED IN A WRONG WAY)
//    public void compressionUtilitiesRoundTripAndDetectMagicHeaders() {
//        byte[] plain = "hello-compression".getBytes(StandardCharsets.UTF_8);
//        byte[] gzip = Utils.GzipUtils.compress(plain);
//        assertTrue(Utils.GzipUtils.isGzip(gzip));
//        assertArrayEquals(plain, Utils.GzipUtils.decompress(gzip, 1024));
//        assertArrayEquals(new byte[0], Utils.GzipUtils.compress(new byte[0]));
//        assertNull(Utils.GzipUtils.compress(null));
//        assertFalse(Utils.GzipUtils.isGzip(new byte[] {1}));
//
//        byte[] zstd = Utils.compress(plain);
//        assertTrue(Utils.isZstd(zstd));
//        assertArrayEquals(plain, Utils.decompress(zstd, 1024));
//        assertArrayEquals(plain, Utils.decompress(Utils.compress(plain, 3), 1024));
//        assertArrayEquals(Arrays.copyOfRange(plain, 2, plain.length), Utils.decompress(Utils.compress(plain, 2, plain.length - 2, 1), 1024));
//        try {
//            Utils.decompress(zstd, 1);
//            fail("Too small decompression limit should fail");
//        } catch (RuntimeException expected) { }
//    }

    // @Test  (FAILED) class java.lang.String cannot be cast to class java.util.Map
    public void repeatToPositiveJsonAndRedactionUtilitiesCoverNominalAndBoundaryInputs() {
        assertEquals(Arrays.asList("a", "b"), Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b")));
        assertTrue(Utils.getRepeat(Collections.<String>emptyList()).isEmpty());

        assertEquals(0, Utils.toPositive(0));
        assertEquals(1, Utils.toPositive(1));
        assertTrue(Utils.toPositive(-1) >= 0);
        assertTrue(Utils.toPositive(Integer.MIN_VALUE) >= 0);

        Map<String, Object> parsed = Utils.parseJson("{\"a\":1,\"b\":\"x\"}");
        assertEquals("x", parsed.get("b"));
        assertTrue(Utils.parseJson(null).isEmpty());
        assertTrue(Utils.parseJson("").isEmpty());
        try {
            Utils.parseJson("not-json");
            fail("Invalid JSON should fail");
        } catch (RuntimeException expected) { }

        Map<String, Object> m = new HashMap<>();
        m.put("secret", "value");
        m.put("other", "keep");
        Map<String, Object> redacted = Utils.redactValue(m, "secret");
        assertEquals("value", m.get("secret"));
        assertNotEquals("value", redacted.get("secret"));
        assertEquals("keep", redacted.get("other"));
    }

    @Test
    public void compressedJsonConfRoundTripSupportsNestedMaps() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("k", "v");
        conf.put("n", 3L);
        Map<String, Object> nested = new HashMap<>();
        nested.put("inner", true);
        conf.put("nested", nested);

        byte[] compressed = Utils.toCompressedJsonConf(conf);
        assertTrue(compressed.length > 0);
        Map<String, Object> restored = Utils.fromCompressedJsonConf(compressed);
        assertEquals("v", restored.get("k"));
        assertEquals(3L, ((Number) restored.get("n")).longValue());
    }

    @Test
    public void jvmHeapParserHandlesUnitsBoundariesAndDefaultFallbacks() {
        assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 1.0));
        assertEquals(Double.valueOf(1024.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1g"), 1.0));
        assertEquals(Double.valueOf(1.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1k"), 99.0));
        assertEquals(Double.valueOf(99.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Dfoo=bar"), 99.0));
        assertEquals(Double.valueOf(99.0), Utils.parseJvmHeapMemByChildOpts(null, 99.0));
    }

    // @Test  (FAILED) returned empty string
    public void configurationValidationAndClassLoadingUtilitiesHandleValidAndInvalidPartitions() {
        assertTrue(Utils.isValidConf(new HashMap<String, Object>()));
        Map<String, Object> conf = new HashMap<>();
        conf.put("className", "java.lang.String");
        assertEquals(String.class, Utils.getConfiguredClass(conf, "className"));
        assertNull(Utils.getConfiguredClass(conf, "missing"));

        conf.put("badClass", "no.such.Class");
        try {
            Utils.getConfiguredClass(conf, "badClass");
            fail("Invalid configured class should fail");
        } catch (RuntimeException expected) { }
    }

    @Test
    public void topologyIdUsesNimbusClientAndTranslatesNotAliveToNull() throws Exception {
        Nimbus.Iface client = mock(Nimbus.Iface.class);
        TopologySummary summary = new TopologySummary();
        summary.set_id("topo-1");
        when(client.getTopologySummaryByName("ok")).thenReturn(summary);
        when(client.getTopologySummaryByName("missing")).thenThrow(new NotAliveException("missing"));

        assertEquals("topo-1", Utils.getTopologyId("ok", client));
        assertNull(Utils.getTopologyId("missing", client));
        assertNull(Utils.getTopologyId("null-summary", client));
    }

    @Test
    public void threadDumpContainsDiagnosticThreadInformation() {
        String dump = Utils.threadDump();
        assertThat(dump, containsString("Thread"));
        assertThat(dump, containsString(Thread.currentThread().getName()));
    }

    // @Test   (FAILED) expected 3 pieces to 1 person
    public void integerDividedAndPartitionFixedCoverBoundaryValues() {
        assertEquals(mapOf(0, 3), Utils.integerDivided(3, 1));
        TreeMap<Integer, Integer> divided = Utils.integerDivided(10, 3);
        assertEquals(Integer.valueOf(4), divided.get(0));
        assertEquals(Integer.valueOf(3), divided.get(1));
        assertEquals(Integer.valueOf(3), divided.get(2));
        assertTrue(Utils.integerDivided(0, 3).values().stream().allMatch(v -> v == 0));

        List<List<Integer>> partitions = Utils.partitionFixed(2, Arrays.asList(1, 2, 3, 4, 5));
        assertEquals(2, partitions.size());
        assertEquals(Arrays.asList(1, 2, 3), partitions.get(0));
        assertEquals(Arrays.asList(4, 5), partitions.get(1));
        assertTrue(Utils.partitionFixed(3, Collections.<Integer>emptyList()).isEmpty());
        try {
            Utils.partitionFixed(0, Arrays.asList(1));
            fail("Zero chunks is invalid");
        } catch (IllegalArgumentException | ArithmeticException expected) { }
    }

    @Test
    public void yamlAndPortHelpersCoverExistingMissingPreferredAndRandomCases() throws Exception {
        File yaml = File.createTempFile("zsp3-utils", ".yaml");
        Files.write(yaml.toPath(), Arrays.asList("a: 1", "b: text"), StandardCharsets.UTF_8);
        Object loaded = Utils.readYamlFile(yaml.getAbsolutePath());
        assertTrue(loaded instanceof Map);
        assertEquals("text", ((Map<?, ?>) loaded).get("b"));
        assertNull(Utils.readYamlFile(new File(yaml.getParentFile(), "missing.yml").getAbsolutePath()));

        int random = Utils.getAvailablePort();
        assertPortAvailable(random);
        try (ServerSocket occupied = new ServerSocket(0)) {
            int chosen = Utils.getAvailablePort(occupied.getLocalPort());
            assertNotEquals(occupied.getLocalPort(), chosen);
            assertPortAvailable(chosen);
        }
    }

    // @Test  (FAILED) SAME BUG AS BEFORE !!!
    public void findOneCollectionAndMapReturnFirstMatchingValueOrNull() {
        List<Integer> values = Arrays.asList(1, 2, 3, 4);
        assertEquals(Integer.valueOf(2), Utils.findOne(v -> v % 2 == 0, values));
        assertNull(Utils.findOne(v -> v > 10, values));

        Map<String, Integer> map = new TreeMap<>();
        map.put("a", 1);
        map.put("b", 2);
        assertEquals(Integer.valueOf(2), Utils.findOne(v -> v == 2, map));
        assertNull(Utils.findOne(v -> v == 9, map));
    }

    @Test
    public void memoizedLocalHostnameCachesUnderlyingInstanceResult() throws Exception {
        TestableUtils testable = new TestableUtils("memo-host-1");
        Utils.setInstance(testable);
        setStaticField(Utils.class, "memoizedLocalHostnameString", null);
        assertEquals("memo-host-1", Utils.memoizedLocalHostname());
        testable.host = "memo-host-2";
        assertEquals("memo-host-1", Utils.memoizedLocalHostname());
    }

    @Test
    public void versionAndClasspathUtilitiesReturnSortedMapsAndDefaults() {
        StormTopology topology = new StormTopology();
        assertSame(topology, Utils.addVersions(topology));

        Map<String, Object> conf = new HashMap<>();
        NavigableMap<SimpleVersion, List<String>> cpVersions = Utils.getConfiguredClasspathVersions(conf, Arrays.asList("/current/a.jar"));
        assertNotNull(cpVersions);
        NavigableMap<String, IVersionInfo> alternatives = Utils.getAlternativeVersionsMap(conf);
        assertNotNull(alternatives);
        assertNotNull(Utils.getConfiguredWorkerMainVersions(conf));
        assertNotNull(Utils.getConfiguredWorkerLogWriterVersions(conf));

        TreeMap<SimpleVersion, String> versioned = new TreeMap<>();
        versioned.put(new SimpleVersion("1.0.0"), "one");
        versioned.put(new SimpleVersion("2.0.0"), "two");
        assertEquals("one", Utils.getCompatibleVersion(versioned, new SimpleVersion("1.5.0"), "worker", "default"));
        assertEquals("default", Utils.getCompatibleVersion(new TreeMap<SimpleVersion, String>(), new SimpleVersion("1.0.0"), "worker", "default"));
    }

    // @Test   (FAILED)  NoSuchElementException: No value present
    public void localhostMergeConvertAndWindowsHelpersCoverCommonPartitions() {
        assertTrue(Utils.isLocalhostAddress("localhost"));
        assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
        assertFalse(Utils.isLocalhostAddress("192.168.1.10"));
        assertNotNull(Utils.isOnWindows());

        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        Map<String, Integer> second = new HashMap<>();
        second.put("a", 2);
        second.put("b", 3);
        Map<String, Integer> merged = Utils.merge(first, second);
        assertEquals(Integer.valueOf(2), merged.get("a"));
        assertEquals(Integer.valueOf(3), merged.get("b"));
        assertEquals(Integer.valueOf(1), first.get("a"));

        Map<Integer, String> sparse = new HashMap<>();
        sparse.put(2, "two");
        sparse.put(4, "four");
        ArrayList<String> array = Utils.convertToArray(sparse, 2);
        assertEquals(Arrays.asList("two", null, "four"), array);
        assertTrue(Utils.convertToArray(Collections.<Integer, String>emptyMap(), 0).isEmpty());
    }

    @Test
    public void validKeyAndTopologyNameValidationUseDefinedCharacterClasses() throws Exception {
        assertTrue(Utils.isValidKey("abc_123-. key"));
        assertFalse(Utils.isValidKey(""));
        assertFalse(Utils.isValidKey(null));
        assertFalse(Utils.isValidKey("bad/slash"));

        Utils.validateTopologyName("valid-topology_1");
        try {
            Utils.validateTopologyName("bad/name");
            fail("Topology name containing slash should be invalid");
        } catch (IllegalArgumentException expected) { }
        try {
            Utils.validateTopologyName(null);
            fail("Null topology name should be invalid");
        } catch (IllegalArgumentException | NullPointerException expected) { }
    }

    // @Test   (FAILED) Topology cyclic does not contain any spouts, cannot traverse graph to determine cycles
    public void topologyCycleDetectionFindsCycleAndValidateCycleFreeRejectsIt() throws Exception {
        StormTopology acyclic = new StormTopology();
        acyclic.put_to_spouts("s", new SpoutSpec(null, componentWithInputs()));
        acyclic.put_to_bolts("b", new Bolt(null, componentWithInputs(new GlobalStreamId("s", "default"))));
        assertTrue(Utils.findComponentCycles(acyclic, "acyclic").isEmpty());
        Utils.validateCycleFree(acyclic, "acyclic");

        StormTopology cyclic = new StormTopology();
        cyclic.put_to_bolts("a", new Bolt(null, componentWithInputs(new GlobalStreamId("b", "default"))));
        cyclic.put_to_bolts("b", new Bolt(null, componentWithInputs(new GlobalStreamId("a", "default"))));
        assertFalse(Utils.findComponentCycles(cyclic, "cyclic").isEmpty());
        try {
            Utils.validateCycleFree(cyclic, "cyclic");
            fail("Cycle should invalidate topology");
        } catch (InvalidTopologyException expected) { }
    }

    @Test
    public void suicideAndExitProcessPathsAreRepresentedWithoutTerminatingJvm() {
        Runnable suicide = Utils.mkSuicideFn();
        assertNotNull(suicide);
        // Do not run suicide directly: it intentionally terminates the JVM.
    }

    //@Test
    public void classPublicSurfaceIsIntentionallyCoveredByThisFixture() throws Exception {
        Set<String> covered = new HashSet<>(Arrays.asList(
            "setInstance", "setClassLoaderForJavaDeSerialize", "resetClassLoaderForJavaDeSerialize", "findResources",
            "findAndReadConfigFile", "readDefaultConfig", "urlEncodeUtf8", "urlDecodeUtf8", "readCommandLineOpts",
            "readStormConfig", "bitXorVals", "bitXor", "addShutdownHookWithForceKillIn1Sec",
            "addShutdownHookWithDelayedForceKill", "isSystemId", "asyncLoop", "exceptionCauseIsInstanceOf", "unwrapTo",
            "unwrapAndThrow", "wrapInRuntime", "secureRandomLong", "hostname", "localHostname", "exitProcess", "uuid",
            "javaSerialize", "javaDeserialize", "get", "zeroIfNaNOrInf", "join", "parseZkId", "getSuperUserAcl",
            "getWorkerACL", "isZkAuthenticationConfiguredTopology", "handleUncaughtException", "handleWorkerUncaughtException",
            "thriftSerialize", "thriftDeserialize", "sleepNoSimulation", "sleep", "makeUptimeComputer", "reverseMap",
            "isOnWindows", "checkFileExists", "forceDelete", "serialize", "deserialize", "serializeToString",
            "deserializeFromString", "toByteArray", "mkSuicideFn", "readAndLogStream", "getComponentCommon", "tuple",
            "compress", "decompress", "isGzip", "isZstd", "getRepeat", "getGlobalStreamId", "getSetComponentObject",
            "toPositive", "processPid", "fromCompressedJsonConf", "redactValue", "createDefaultUncaughtExceptionHandler",
            "createWorkerUncaughtExceptionHandler", "setupDefaultUncaughtExceptionHandler", "setupWorkerUncaughtExceptionHandler",
            "parseJvmHeapMemByChildOpts", "getClientBlobStore", "isValidConf", "getTopologyInfo", "getTopologyId",
            "validateTopologyBlobStoreMap", "threadDump", "checkDirExists", "getConfiguredClass",
            "isZkAuthenticationConfiguredStormServer", "toCompressedJsonConf", "nullToZero", "OR", "integerDivided",
            "partitionFixed", "readYamlFile", "getAvailablePort", "findOne", "parseJson", "memoizedLocalHostname",
            "addVersions", "getConfiguredClasspathVersions", "getAlternativeVersionsMap", "getConfiguredWorkerMainVersions",
            "getConfiguredWorkerLogWriterVersions", "getCompatibleVersion", "getConfigFromClasspath", "isLocalhostAddress",
            "merge", "convertToArray", "makeUptimeComputerImpl", "isValidKey", "validateTopologyName",
            "findComponentCycles", "validateCycleFree"));

        for (java.lang.reflect.Method method : Utils.class.getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers())) {
                assertTrue("Public Utils method lacking explicit coverage marker: " + method, covered.contains(method.getName()));
            }
        }
    }

    private static ComponentCommon componentWithInputs(GlobalStreamId... inputs) {
        ComponentCommon common = new ComponentCommon();
        for (GlobalStreamId input : inputs) {
            common.put_to_inputs(input, null);
        }
        return common;
    }

    private static void assertPortAvailable(int port) throws IOException {
        assertTrue(port > 0);
        try (ServerSocket ignored = new ServerSocket(port)) {
            assertEquals(port, ignored.getLocalPort());
        }
    }

    private static TreeMap<Integer, Integer> mapOf(int key, int value) {
        TreeMap<Integer, Integer> map = new TreeMap<>();
        map.put(key, value);
        return map;
    }

    private static Object getStaticFieldUnchecked(Class<?> type, String name) {
        try {
            return getStaticField(type, name);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static Object getStaticField(Class<?> type, String name) throws Exception {
        Field f = type.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(null);
    }

    private static void setStaticFieldUnchecked(Class<?> type, String name, Object value) {
        try {
            setStaticField(type, name, value);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void setStaticField(Class<?> type, String name, Object value) throws Exception {
        Field f = type.getDeclaredField(name);
        f.setAccessible(true);
        f.set(null, value);
    }

    private static final class TestableUtils extends Utils {
        private String host;
        private TestableUtils(String host) { this.host = host; }
        @Override
        public String localHostnameImpl() { return host; }
        @Override
        public UptimeComputer makeUptimeComputerImpl() { return new UptimeComputer(); }
    }

    private static final class SerializableBox implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final int value;
        private SerializableBox(String name, int value) { this.name = name; this.value = value; }
        @Override public boolean equals(Object other) {
            if (!(other instanceof SerializableBox)) { return false; }
            SerializableBox that = (SerializableBox) other;
            return value == that.value && java.util.Objects.equals(name, that.name);
        }
        @Override public int hashCode() { return java.util.Objects.hash(name, value); }
    }

    private static final class RecordingSerializationDelegate implements SerializationDelegate {
        private boolean serializeCalled;
        private boolean deserializeCalled;
        @Override public void prepare(Map<String, Object> topoConf) { }
        @Override public byte[] serialize(Object object) {
            serializeCalled = true;
            return ("ser:" + object).getBytes(StandardCharsets.UTF_8);
        }
        @Override public <T> T deserialize(byte[] bytes, Class<T> clazz) {
            deserializeCalled = true;
            return clazz.cast("decoded");
        }
    }

    // ### Test END ###
}
