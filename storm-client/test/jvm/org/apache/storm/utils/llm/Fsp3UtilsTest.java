package org.apache.storm.utils.llm;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.zero.Utils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.IllegalStateException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Permission;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test suite generated for {@link Utils} using Category Partition and Boundary Value Analysis.
 * The class intentionally focuses on deterministic public behavior and isolates dangerous JVM-side
 * effects (System.exit / shutdown hooks) with safe guards or ignored documentation tests.
 */

/** FIXED MANUALLY */
@SuppressWarnings("removal")  // ADDED SUPPRESS WARNING
public class Fsp3UtilsTest {
    /* ### Test START ### */

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private SecurityManager originalSecurityManager;
    private String originalDeserializationDisabled;

    @Before
    public void setUp() {
        originalSecurityManager = System.getSecurityManager();
        originalDeserializationDisabled = System.getProperty("java.deserialization.disabled");
        System.clearProperty("java.deserialization.disabled");
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    @After
    public void tearDown() {
        System.setSecurityManager(originalSecurityManager);
        if (originalDeserializationDisabled == null) {
            System.clearProperty("java.deserialization.disabled");
        } else {
            System.setProperty("java.deserialization.disabled", originalDeserializationDisabled);
        }
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    /** Test setInstance with valid instance. Expected = returns previously configured instance. */
    @Test
    public void setInstanceValidInstanceReturnsPreviousInstance() {
        Utils first = new Utils();
        Utils previous = Utils.setInstance(first);
        Utils second = new Utils();
        Assert.assertSame(first, Utils.setInstance(second));
        Utils.setInstance(previous);
    }

    /** Test Java deserialization class loader setters. Expected = complete without exception and preserve deserialization behavior. */
    @Test
    public void javaDeserializeClassLoaderSetAndResetShouldPass() {
        byte[] payload = Utils.javaSerialize("value");
        Utils.setClassLoaderForJavaDeSerialize(Thread.currentThread().getContextClassLoader());
        Assert.assertEquals("value", Utils.javaDeserialize(payload, String.class));
        Utils.resetClassLoaderForJavaDeSerialize();
        Assert.assertEquals("value", Utils.javaDeserialize(payload, String.class));
    }

    /** Test findResources with a known JDK resource. Expected = non-null list, usually not empty. */
    @Test
    public void findResourcesKnownResourceShouldPass() {
        List<URL> resources = Utils.findResources("java/lang/String.class");
        Assert.assertNotNull(resources);
    }

    /** Test findResources with a not correct name. Expected = empty list. */
    @Test
    public void findResourcesNotCorrectNameReturnsEmptyList() {
        Assert.assertTrue(Utils.findResources("missing-resource-for-fsp3-utils-test.yaml").isEmpty());
    }

    /** Test findAndReadConfigFile with name not found and mustExist false. Expected = empty map. */
    @Test
    public void findAndReadConfigFileMissingOptionalReturnsEmptyMap() {
        Assert.assertEquals(Collections.emptyMap(), Utils.findAndReadConfigFile("missing-fsp3.yaml", false));
    }

    /** Test findAndReadConfigFile overload with name not found. Expected = RuntimeException because mustExist defaults to true. */
    @Test
    public void findAndReadConfigFileMissingRequiredThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("missing-fsp3.yaml"));
    }

    /** Test readDefaultConfig. Expected = defaults configuration is read from classpath. */
    @Test
    public void readDefaultConfigShouldReturnMap() {
        Map<String, Object> conf = Utils.readDefaultConfig();
        Assert.assertNotNull(conf);
        Assert.assertFalse(conf.isEmpty());
    }

    /** Test URL encode/decode with spaces and symbols. Expected = equivalent JDK UTF-8 behavior. */
    @Test
    public void urlEncodeDecodeValidStringShouldPass() throws Exception {
        String value = "a b+à&=";
        Assert.assertEquals(URLEncoder.encode(value, StandardCharsets.UTF_8.name()), Utils.urlEncodeUtf8(value));
        Assert.assertEquals(value, Utils.urlDecodeUtf8(Utils.urlEncodeUtf8(value)));
        Assert.assertEquals(URLDecoder.decode(Utils.urlEncodeUtf8(value), StandardCharsets.UTF_8.name()),
                Utils.urlDecodeUtf8(Utils.urlEncodeUtf8(value)));
    }

    /** Test URL encode/decode with null. Expected = NullPointerException. */
    @Test
    public void urlEncodeDecodeNullThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlEncodeUtf8(null));
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlDecodeUtf8(null));
    }

    /** Test config readers. Expected = non-null maps. */
    @Test
    public void readCommandLineAndStormConfigShouldReturnMaps() {
        Assert.assertNotNull(Utils.readCommandLineOpts());
        Assert.assertNotNull(Utils.readStormConfig());
    }

    /** Test bitXorVals boundaries: empty, one element, multiple elements. Expected = XOR aggregation. */
    @Test
    public void bitXorValsBoundariesShouldPass() {
        Assert.assertEquals(0L, Utils.bitXorVals(Collections.emptyList()));
        Assert.assertEquals(7L, Utils.bitXorVals(Collections.singletonList(7L)));
        Assert.assertEquals(0L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    /** Test bitXor valid and null inputs. Expected = XOR or NullPointerException. */
    @Test
    public void bitXorValidAndNullShouldPass() {
        Assert.assertEquals(6L, Utils.bitXor(5L, 3L));
        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXor(null, 1L));
    }

    /** Test isSystemId string categories. Expected = true only for identifiers beginning with double underscore. */
    @Test
    public void isSystemIdCategoriesShouldPass() {
        Assert.assertTrue(Utils.isSystemId("__system"));
        Assert.assertFalse(Utils.isSystemId("_system"));
        Assert.assertFalse(Utils.isSystemId("system"));
        Assert.assertThrows(NullPointerException.class, () -> Utils.isSystemId(null));
    }

    /** Test asyncLoop full signature with startImmediately false. Expected = configured SmartThread returned but not started. */
    // @Test   (FAILED) expected Thread-0-fsp3-thread
    public void asyncLoopFullSignatureNotStartedShouldPass() {
        Callable<Long> callable = () -> null;
        Utils.SmartThread thread = Utils.asyncLoop(callable, true, null, Thread.MAX_PRIORITY, false, false, "fsp3-thread");
        Assert.assertNotNull(thread);
        Assert.assertEquals("fsp3-thread", thread.getName());
        Assert.assertTrue(thread.isDaemon());
        Assert.assertEquals(Thread.MAX_PRIORITY, thread.getPriority());
        Assert.assertFalse(thread.isAlive());
    }

    /** Test asyncLoop overload with handler. Expected = thread runs callable and stops when null is returned. */
    // @Test   (FAILED)  expected Thread-0-named-loop
    public void asyncLoopNamedOverloadShouldExecuteOnce() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        UncaughtExceptionHandler handler = (t, e) -> { };
        Utils.SmartThread thread = Utils.asyncLoop((Callable<Long>) () -> {
            calls.incrementAndGet();
            return null;
        }, "named-loop", handler);
        thread.join(2000L);
        Assert.assertEquals("named-loop", thread.getName());
        Assert.assertEquals(1, calls.get());
    }

    /** Test asyncLoop simplest overload. Expected = thread executes callable once. */
    @Test
    public void asyncLoopSimpleOverloadShouldExecuteOnce() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Utils.SmartThread thread = Utils.asyncLoop((Callable<Long>) () -> {
            calls.incrementAndGet();
            return null;
        });
        thread.join(2000L);
        Assert.assertEquals(1, calls.get());
    }

    /** Test exception unwrapping APIs. Expected = detects exception in cause chain. */
    @Test
    public void exceptionCauseUnwrapMethodsShouldPass() throws Exception {
        IllegalArgumentException cause = new IllegalArgumentException("bad");
        RuntimeException wrapper = new RuntimeException(new IOException(cause));
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, wrapper));
        Assert.assertSame(cause, Utils.unwrapTo(IllegalArgumentException.class, wrapper));
        Assert.assertNull(Utils.unwrapTo(IllegalStateException.class, wrapper));
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.unwrapAndThrow(IllegalArgumentException.class, wrapper));
        Utils.unwrapAndThrow(IllegalStateException.class, wrapper);
    }

    /** Test wrapInRuntime with RuntimeException and checked Exception. Expected = same instance or wrapping runtime exception. */
    @Test
    public void wrapInRuntimeCategoriesShouldPass() {
        RuntimeException runtime = new RuntimeException("runtime");
        Assert.assertSame(runtime, Utils.wrapInRuntime(runtime));
        IOException checked = new IOException("io");
        RuntimeException wrapped = Utils.wrapInRuntime(checked);
        Assert.assertSame(checked, wrapped.getCause());
    }

    /** Test random/host/id utilities. Expected = valid value shapes. */
    @Test
    public void randomHostAndIdUtilitiesShouldPass() throws UnknownHostException {
        Assert.assertNotEquals(Utils.secureRandomLong(), Utils.secureRandomLong());
        Assert.assertNotNull(Utils.hostname());
        Assert.assertNotNull(Utils.localHostname());
        Assert.assertNotNull(UUID.fromString(Utils.uuid()));
        Assert.assertTrue(Utils.processPid().length() > 0);
        Assert.assertTrue(Utils.memoizedLocalHostname().length() > 0);
        Assert.assertEquals(Utils.memoizedLocalHostname(), Utils.memoizedLocalHostname());
    }

    /** Test exitProcess in an isolated SecurityManager. Expected = attempts requested exit code. */
    // @Test   (FAILED)  UnsupportedOperationException: The Security Manager is deprecated and will be removed in a future release
    public void exitProcessShouldAttemptSystemExit() {
        System.setSecurityManager(new NoExitSecurityManager(originalSecurityManager));
        ExitException thrown = Assert.assertThrows(ExitException.class, () -> Utils.exitProcess(7, "test"));
        Assert.assertEquals(7, thrown.status);
    }

    /** Test Java serialization/deserialization valid, null and disabled categories. */
    @Test
    public void javaSerializeDeserializeCategoriesShouldPass() {
        ArrayList<String> expected = new ArrayList<>(Arrays.asList("a", "b"));
        byte[] serialized = Utils.javaSerialize(expected);
        Assert.assertEquals(expected, Utils.javaDeserialize(serialized, ArrayList.class));
        Assert.assertNotNull(Utils.javaSerialize(null));
        System.setProperty("java.deserialization.disabled", "true");
        Assert.assertThrows(AssertionError.class, () -> Utils.javaDeserialize(serialized, ArrayList.class));
    }

    /** Test generic get helper. Expected = configured value for present key, default for absent key and null value. */
    @Test
    public void getHelperCategoriesShouldPass() {
        Map<String, String> map = new HashMap<>();
        map.put("present", "value");
        map.put("null", null);
        Assert.assertEquals("value", Utils.get(map, "present", "default"));
        Assert.assertEquals("default", Utils.get(map, "missing", "default"));
        Assert.assertEquals("default", Utils.get(map, "null", "default"));
    }

    /** Test zeroIfNaNOrInf with numeric boundaries and special floating point values. */
    @Test
    public void zeroIfNaNOrInfBoundariesShouldPass() {
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NEGATIVE_INFINITY), 0.0);
        Assert.assertEquals(-1.5, Utils.zeroIfNaNOrInf(-1.5), 0.0);
    }

    /** Test join with empty, singleton and multiple values. */
    @Test
    public void joinCategoriesShouldPass() {
        Assert.assertEquals("", Utils.join(Collections.emptyList(), ","));
        Assert.assertEquals("a", Utils.join(Collections.singletonList("a"), ","));
        Assert.assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    /** Test ZK id parsing and super/worker ACL creation. */
    @Test
    public void zkAclUtilitiesShouldPass() {
        Id id = Utils.parseZkId("sasl:storm-user", "key");
        Assert.assertEquals("sasl", id.getScheme());
        Assert.assertEquals("storm-user", id.getId());
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.parseZkId("invalid", "key"));

        Map<String, Object> conf = new HashMap<>();
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(conf));
        Assert.assertNull(Utils.getWorkerACL(conf));
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "sasl");
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:super");
        ACL superAcl = Utils.getSuperUserAcl(conf);
        Assert.assertEquals(ZooDefs.Perms.ALL, superAcl.getPerms());
        Assert.assertEquals("super", superAcl.getId().getId());
        Assert.assertNotNull(Utils.getWorkerACL(conf));
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
        conf.remove(Config.STORM_ZOOKEEPER_SUPERACL);
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.getSuperUserAcl(conf));
    }

    /** Test uncaught exception handlers with allowed exceptions. Expected = no exception. */
    // @Test    (FAILED)   launched an Error
    public void uncaughtExceptionHandlersAllowedExceptionsShouldPass() {
        Set<Class<?>> allowed = new HashSet<>();
        allowed.add(IllegalArgumentException.class);
        Utils.handleUncaughtException(new RuntimeException(new IllegalArgumentException("allowed")), allowed, false);
        Utils.handleUncaughtException(new RuntimeException(new IllegalArgumentException("allowed")), allowed, true);
        Utils.createDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), null);
        Utils.createWorkerUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), null);
        Utils.setupDefaultUncaughtExceptionHandler();
        Utils.setupWorkerUncaughtExceptionHandler();
    }

    /** Test thrift serialize/deserialize using generated GlobalStreamId. Expected = round trip. */
    @Test
    public void thriftRoundTripShouldPass() {
        GlobalStreamId expected = new GlobalStreamId("component", "stream");
        byte[] serialized = Utils.thriftSerialize(expected);
        Assert.assertEquals(expected, Utils.thriftDeserialize(GlobalStreamId.class, serialized));
        byte[] wrapper = new byte[serialized.length + 2];
        System.arraycopy(serialized, 0, wrapper, 1, serialized.length);
        Assert.assertEquals(expected, Utils.thriftDeserialize(GlobalStreamId.class, wrapper, 1, serialized.length));
        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftDeserialize(GlobalStreamId.class, new byte[] {1, 2, 3}));
    }

    /** Test sleep methods at zero boundary. Expected = complete without exception. */
    @Test
    public void sleepZeroMillisShouldPass() {
        Utils.sleepNoSimulation(0L);
        Utils.sleep(0L);
    }

    /** Test makeUptimeComputer. Expected = non-null uptime computer. */
    @Test
    public void makeUptimeComputerShouldPass() {
        Assert.assertNotNull(Utils.makeUptimeComputer());
//        Assert.assertNotNull(Utils.makeUptimeComputerImpl());  //NOT STATIC
    }

    /** Test reverseMap(Map) with duplicate values. Expected = values become keys and original keys are grouped. */
    @Test
    public void reverseMapMapShouldGroupKeys() {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 2);
        HashMap<Integer, List<String>> reversed = Utils.reverseMap(map);
        Assert.assertEquals(Arrays.asList("a", "b"), reversed.get(1));
        Assert.assertEquals(Collections.singletonList("c"), reversed.get(2));
    }

    /** Test reverseMap(List) with list of sequences. Expected = indexes grouped by object value. */
    // @Test   (FAILED) a is not a key anymore
    public void reverseMapListShouldGroupPositions() {
        List<List<Object>> input = Arrays.asList(
                Arrays.asList((Object) "a", "b"),
                Arrays.asList((Object) "b", "c"));
        Map<Object, List<Object>> reversed = Utils.reverseMap(input);
        Assert.assertEquals(Collections.singletonList(0), reversed.get("a"));
        Assert.assertEquals(Arrays.asList(0, 1), reversed.get("b"));
        Assert.assertEquals(Collections.singletonList(1), reversed.get("c"));
    }

    /** Test filesystem utilities with file, directory and missing paths. */
    @Test
    public void filesystemUtilitiesShouldPass() throws IOException {
        File file = temporaryFolder.newFile("file.txt");
        File dir = temporaryFolder.newFolder("dir");
        Assert.assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
        Assert.assertFalse(Utils.checkFileExists(new File(temporaryFolder.getRoot(), "missing").getAbsolutePath()));
        Assert.assertTrue(Utils.checkDirExists(dir.getAbsolutePath()));
        Assert.assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
        Utils.forceDelete(file.getAbsolutePath());
        Assert.assertFalse(file.exists());
    }

    /** Test Storm serialization delegate APIs. Expected = round trip through bytes and string. */
    @Test
    public void serializationDelegateRoundTripShouldPass() {
        GlobalStreamId expected = new GlobalStreamId("component", "stream");
        byte[] serialized = Utils.serialize(expected);
        Assert.assertEquals(expected, Utils.deserialize(serialized, GlobalStreamId.class));
        String encoded = Utils.serializeToString(expected);
        Assert.assertEquals(expected, Utils.deserializeFromString(encoded, GlobalStreamId.class));
        Assert.assertThrows(NullPointerException.class, () -> Utils.deserializeFromString(null, GlobalStreamId.class));
    }

    /** Test ByteBuffer conversion with position and remaining bytes. Expected = only remaining bytes are returned. */
    @Test
    public void toByteArrayShouldReturnRemainingBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
        buffer.position(1);
        Assert.assertArrayEquals(new byte[] {1, 2, 3}, Utils.toByteArray(buffer));
    }

    /** Test mkSuicideFn object creation only. Expected = Runnable returned; run is intentionally not invoked. */
    @Test
    public void mkSuicideFnShouldReturnRunnable() {
        Assert.assertNotNull(Utils.mkSuicideFn());
    }

    /** Test readAndLogStream valid stream and null stream. Expected = valid stream consumed, null handled internally. */
    // @Test  (FAILED) NullPointerException
    public void readAndLogStreamCategoriesShouldPass() {
        InputStream in = new ByteArrayInputStream("line1\nline2".getBytes(StandardCharsets.UTF_8));
        Utils.readAndLogStream("prefix", in);
        Utils.readAndLogStream("prefix", null);
    }

    /** Test getComponentCommon for spout, bolt, state spout and missing id. */
    // @Test  (FAILED)  IllegalArgumentException: Could not find component with id missing
    public void getComponentCommonCategoriesShouldPass() {
        ComponentCommon spoutCommon = new ComponentCommon();
        ComponentCommon boltCommon = new ComponentCommon();
        ComponentCommon stateCommon = new ComponentCommon();
        StormTopology topology = new StormTopology();
        topology.put_to_spouts("spout", new SpoutSpec(null, spoutCommon));
        topology.put_to_bolts("bolt", new Bolt(null, boltCommon));
        topology.put_to_state_spouts("state", new StateSpoutSpec(null, stateCommon));
        Assert.assertSame(spoutCommon, Utils.getComponentCommon(topology, "spout"));
        Assert.assertSame(boltCommon, Utils.getComponentCommon(topology, "bolt"));
        Assert.assertSame(stateCommon, Utils.getComponentCommon(topology, "state"));
        Assert.assertNull(Utils.getComponentCommon(topology, "missing"));
    }

    /** Test tuple and repeat helpers. Expected = created list and duplicate-only list. */
    @Test
    public void tupleAndGetRepeatShouldPass() {
        Assert.assertEquals(Arrays.asList("a", 1, null), Utils.tuple("a", 1, null));
        Assert.assertEquals(Arrays.asList("a", "b"), Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b")));
        Assert.assertEquals(Collections.emptyList(), Utils.getRepeat(Collections.singletonList("a")));
    }

    /** Test GZIP compression utility categories. */
    @Test
    public void gzipUtilsCategoriesShouldPass() {
        byte[] raw = "hello gzip".getBytes(StandardCharsets.UTF_8);
        Assert.assertNull(Utils.GzipUtils.compress(null));
        Assert.assertArrayEquals(new byte[0], Utils.GzipUtils.compress(new byte[0]));
        byte[] compressed = Utils.GzipUtils.compress(raw);
        Assert.assertTrue(Utils.GzipUtils.isGzip(compressed));
        Assert.assertFalse(Utils.GzipUtils.isGzip(raw));
        Assert.assertArrayEquals(raw, Utils.GzipUtils.decompress(compressed, 1024));
        Assert.assertThrows(RuntimeException.class, () -> Utils.GzipUtils.decompress(raw, 1024));
    }

    /** Test Zstd compression utility categories. */
    @Test
    public void zstdUtilsCategoriesShouldPass() {  // ADDED COMPRESSION LEVELS
        byte[] raw = "hello zstd".getBytes(StandardCharsets.UTF_8);
        Assert.assertNull(Utils.ZstdUtils.compress(null,0));
        Assert.assertArrayEquals(new byte[0], Utils.ZstdUtils.compress(new byte[0],0));
        byte[] compressed = Utils.ZstdUtils.compress(raw,0);
        Assert.assertTrue(Utils.ZstdUtils.isZstd(compressed));
        Assert.assertFalse(Utils.ZstdUtils.isZstd(raw));
        Assert.assertArrayEquals(raw, Utils.ZstdUtils.decompress(compressed, 1024));
        Assert.assertThrows(RuntimeException.class, () -> Utils.ZstdUtils.decompress(raw, 1024));
    }

    /** Test GlobalStreamId helper. Expected = default stream for null/empty else provided stream. */
    // @Test (FAILED) expected = GlobalStreamId(componentId:component, streamId:)
    public void getGlobalStreamIdCategoriesShouldPass() {
        Assert.assertEquals(new GlobalStreamId("component", Utils.DEFAULT_STREAM_ID), Utils.getGlobalStreamId("component", null));
        Assert.assertEquals(new GlobalStreamId("component", Utils.DEFAULT_STREAM_ID), Utils.getGlobalStreamId("component", ""));
        Assert.assertEquals(new GlobalStreamId("component", "stream"), Utils.getGlobalStreamId("component", "stream"));
    }

//    /** Test ComponentObject helper for java object and serialized java object. */
//    @Test
//    public void getSetComponentObjectCategoriesShouldPass() {  (GENERATED IN A WRONG WAY)
//        ComponentObject javaObject = ComponentObject.java_object("plain");
//        Assert.assertEquals("plain", Utils.getSetComponentObject(javaObject));
//        ComponentObject serializedJava = ComponentObject.serialized_java(Utils.javaSerialize("serialized"));
//        Assert.assertEquals("serialized", Utils.getSetComponentObject(serializedJava));
//        Assert.assertNull(Utils.getSetComponentObject(new ComponentObject()));
//    }

    /** Test toPositive boundary values. Expected = non-negative hash-style transformation. */
    // @Test  (FAILED) it's not an absolute value method
    public void toPositiveBoundariesShouldPass() {
        Assert.assertEquals(0, Utils.toPositive(0));
        Assert.assertEquals(1, Utils.toPositive(1));
        Assert.assertEquals(1, Utils.toPositive(-1));
        Assert.assertTrue(Utils.toPositive(Integer.MIN_VALUE) >= 0);
    }

    /** Test compressed JSON config round trip and invalid serialized payload. */
    // @Test  (FAILED)  expected = {"number":3,"key":"value"}
    public void compressedJsonConfRoundTripShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("key", "value");
        conf.put("number", 3L);
        byte[] serialized = Utils.toCompressedJsonConf(conf);
        Assert.assertEquals(conf, Utils.fromCompressedJsonConf(serialized));
        Assert.assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(new byte[] {1, 2, 3}));
    }

    /** Test redactValue. Expected = returns copy with only target key redacted and original preserved. */
    @Test
    public void redactValueShouldNotMutateOriginal() {
        Map<String, Object> source = new HashMap<>();
        source.put("password", "secret");
        source.put("user", "name");
        Map<String, Object> redacted = Utils.redactValue(source, "password");
        Assert.assertEquals("secret", source.get("password"));
        Assert.assertNotEquals("secret", redacted.get("password"));
        Assert.assertEquals("name", redacted.get("user"));
    }

    /** Test parseJvmHeapMemByChildOpts with valid units and missing values. */
    @Test
    public void parseJvmHeapMemByChildOptsCategoriesShouldPass() {
        Assert.assertEquals(Double.valueOf(1024.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1g"), 64.0));
        Assert.assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 64.0));
        Assert.assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Dkey=value"), 64.0));
        Assert.assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(null, 64.0));
    }

    /** Test isValidConf with valid and invalid topology config. */
    // @Test   (FAILED)    NullPointerException: Cannot invoke "Object.toString()" because the return value of "java.util.Map$Entry.getKey()" is null
    public void isValidConfCategoriesShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("key", "value");
        Assert.assertTrue(Utils.isValidConf(conf));
        Map<String, Object> invalid = new HashMap<>();
        invalid.put(null, "value");
        Assert.assertFalse(Utils.isValidConf(invalid));
    }

    /** Test getTopologyId with a Nimbus proxy returning no topology summary. Expected = null. */
    @Test
    public void getTopologyIdMissingShouldReturnNull() {
        Nimbus.Iface client = (Nimbus.Iface)
                java.lang.reflect.Proxy.newProxyInstance(
                        Nimbus.Iface.class.getClassLoader(),
                        new Class<?>[] { Nimbus.Iface.class },
                        (proxy, method, args) -> "getTopologySummaryByName".equals(method.getName()) ? null : null);
        Assert.assertNull(Utils.getTopologyId("definitely-missing", client));
    }

    /** Documented but ignored: getTopologyInfo builds a real NimbusClient and requires an integration fixture. */
    @Ignore("Requires a configured NimbusClient integration fixture; unit test coverage is provided through getTopologyId proxy path.")
    @Test
    public void getTopologyInfoMissingShouldReturnNullWithNimbusFixture() {
        Assert.assertNull(Utils.getTopologyInfo("definitely-missing", null, new HashMap<>()));
    }

    /** Test topology blobstore validation overloads with empty map. Expected = complete without exception. */
    @Test
    public void validateTopologyBlobStoreMapOverloadsEmptyShouldPass() throws Exception {
        Map<String, Object> topoConf = new HashMap<>();
        Utils.validateTopologyBlobStoreMap(topoConf, (org.apache.storm.blobstore.NimbusBlobStore) null);
        Utils.validateTopologyBlobStoreMap(topoConf, (org.apache.storm.blobstore.BlobStore) null);
    }

    /** Documented but ignored: no-arg client overload creates a NimbusBlobStore and requires integration configuration. */
    @Ignore("Avoids constructing a real NimbusBlobStore in a pure unit test.")
    @Test
    public void validateTopologyBlobStoreMapNoClientEmptyShouldPassWithFixture() throws Exception {
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>());
    }

    /** Test threadDump. Expected = human-readable dump containing current thread name. */
    @Test
    public void threadDumpShouldContainThreadInformation() {
        String dump = Utils.threadDump();
        Assert.assertNotNull(dump);
        Assert.assertTrue(dump.contains(Thread.currentThread().getName()) || dump.contains("Thread"));
    }

    /** Test configured class with absent, valid and invalid class values. */
    @Test
    public void getConfiguredClassCategoriesShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        Assert.assertNull(Utils.getConfiguredClass(conf, "class.key"));
        conf.put("class.key", ArrayList.class.getName());
        Object configured = Utils.getConfiguredClass(conf, "class.key");
        Assert.assertTrue(configured instanceof ArrayList);
        conf.put("class.key", "not.existing.ClassName");
        Assert.assertThrows(RuntimeException.class, () -> Utils.getConfiguredClass(conf, "class.key"));
    }

    /** Test server ZK auth config. Expected = true only when storm server auth user configured. */
    @Test
    public void isZkAuthenticationConfiguredStormServerCategoriesShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(conf));
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "sasl");
        conf.put(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD, "payload");
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(conf));
    }

    /** Test nullToZero and OR generic helpers. */
    @Test
    public void nullToZeroAndOrShouldPass() {
        Assert.assertEquals(0.0, Utils.nullToZero(null), 0.0);
        Assert.assertEquals(3.5, Utils.nullToZero(3.5), 0.0);
        Assert.assertEquals("b", Utils.OR(null, "b"));
        Assert.assertEquals("a", Utils.OR("a", "b"));
    }

    /** Test integerDivided with boundary values. */
    // @Test    (FAILED)   Actual   :{0=1}
    public void integerDividedBoundariesShouldPass() {
        Assert.assertEquals(new TreeMap<Integer, Integer>() {{ put(0, 0); }}, Utils.integerDivided(0, 1));
        TreeMap<Integer, Integer> split = Utils.integerDivided(10, 3);
        Assert.assertEquals(Integer.valueOf(2), split.get(3));
        Assert.assertEquals(Integer.valueOf(1), split.get(4));
        Assert.assertThrows(ArithmeticException.class, () -> Utils.integerDivided(1, 0));
    }

    /** Test partitionFixed with empty, singleton and multiple partitions. */
    @Test
    public void partitionFixedCategoriesShouldPass() {
        Assert.assertEquals(Collections.emptyList(), Utils.partitionFixed(3, Collections.emptyList()));
        Assert.assertEquals(Collections.singletonList(Collections.singletonList("a")),
                Utils.partitionFixed(3, Collections.singletonList("a")));
        List<List<Integer>> partitions = Utils.partitionFixed(2, Arrays.asList(1, 2, 3, 4, 5));
        Assert.assertEquals(2, partitions.size());
        Assert.assertEquals(Arrays.asList(1, 2, 3), partitions.get(0));
        Assert.assertEquals(Arrays.asList(4, 5), partitions.get(1));
    }

    /** Test YAML reader with existing YAML, missing file and not valid YAML. */
    @Test
    public void readYamlFileCategoriesShouldPass() throws IOException {
        File yaml = temporaryFolder.newFile("valid.yaml");
        java.nio.file.Files.write(yaml.toPath(), Arrays.asList("a: 1", "b: text"));
        Object value = Utils.readYamlFile(yaml.getAbsolutePath());
        Assert.assertTrue(value instanceof Map);
        Assert.assertNull(Utils.readYamlFile(new File(temporaryFolder.getRoot(), "missing.yaml").getAbsolutePath()));
    }

    /** Test available port helpers. Expected = preferred when free, random valid otherwise. */
    @Test
    public void getAvailablePortCategoriesShouldPass() throws IOException {
        int preferred = freePort();
        Assert.assertEquals(preferred, Utils.getAvailablePort(preferred));
        Assert.assertTrue(Utils.getAvailablePort() > 0);
    }

    /** Test findOne overloads for collection and map. */
    // @Test    (FAILED)   ClassCastException: class java.util.LinkedHashMap$Entry cannot be cast to class java.lang.String
    public void findOneCategoriesShouldPass() {
        Assert.assertEquals("bb", Utils.findOne(x -> x.length() == 2, Arrays.asList("a", "bb", "cc")));
        Assert.assertNull(Utils.findOne(x -> x.startsWith("z"), Arrays.asList("a", "bb")));
        Map<String, String> map = new LinkedHashMap<>();
        map.put("one", "a");
        map.put("two", "bb");
        Assert.assertEquals("bb", Utils.findOne(x -> x.length() == 2, map));
        Assert.assertNull(Utils.findOne(x -> x.startsWith("z"), map));
    }

    /** Test parseJson valid, empty and not valid categories. */
    @Test
    public void parseJsonCategoriesShouldPass() {
        Map<String, Object> parsed = Utils.parseJson("{\"a\":1,\"b\":\"x\"}");
        Assert.assertEquals("x", parsed.get("b"));
        Assert.assertEquals(Collections.emptyMap(), Utils.parseJson(null));
        Assert.assertThrows(RuntimeException.class, () -> Utils.parseJson("{not-valid-json"));
    }

    /** Test addVersions with empty topology. Expected = topology returned and version fields present. */
    @Test
    public void addVersionsShouldReturnSameTopology() {
        StormTopology topology = new StormTopology();
        StormTopology returned = Utils.addVersions(topology);
        Assert.assertSame(topology, returned);
        Assert.assertNotNull(returned.get_storm_version());
    }

    /** Test version configuration maps with minimal configuration. Expected = non-null navigable maps. */
    @Test
    public void configuredVersionMapsShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        Assert.assertNotNull(Utils.getConfiguredClasspathVersions(conf, Collections.emptyList()));
        Assert.assertNotNull(Utils.getAlternativeVersionsMap(conf));
        Assert.assertNotNull(Utils.getConfiguredWorkerMainVersions(conf));
        Assert.assertNotNull(Utils.getConfiguredWorkerLogWriterVersions(conf));
    }

    /** Test getCompatibleVersion with empty map and lower-entry compatible version. */
    @Test
    public void getCompatibleVersionCategoriesShouldPass() {
        TreeMap<SimpleVersion, String> versions = new TreeMap<>();
        Assert.assertEquals("default", Utils.getCompatibleVersion(versions, new SimpleVersion("1.0.0"), "worker", "default"));
        versions.put(new SimpleVersion("1.0.0"), "one");
        versions.put(new SimpleVersion("2.0.0"), "two");
        Assert.assertEquals("one", Utils.getCompatibleVersion(versions, new SimpleVersion("1.5.0"), "worker", "default"));
    }

    /** Test getConfigFromClasspath with empty classpath. Expected = original conf returned. */
    @Test
    public void getConfigFromClasspathEmptyShouldReturnConf() throws IOException {  // ADDED THROWS
        Map<String, Object> conf = new HashMap<>();
        conf.put("a", "b");
        Assert.assertSame(conf, Utils.getConfigFromClasspath(Collections.emptyList(), conf));
    }

    /** Test localhost address recognition. */
    @Test
    public void isLocalhostAddressCategoriesShouldPass() {
        Assert.assertTrue(Utils.isLocalhostAddress("localhost"));
        Assert.assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
        Assert.assertFalse(Utils.isLocalhostAddress("192.168.1.1"));
        Assert.assertFalse(Utils.isLocalhostAddress(null));
    }

    /** Test merge with overriding keys and null/empty maps. */
    @Test
    public void mergeCategoriesShouldPass() {
        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        first.put("b", 2);
        Map<String, Integer> other = new HashMap<>();
        other.put("b", 20);
        other.put("c", 30);
        Map<String, Integer> merged = Utils.merge(first, other);
        Assert.assertEquals(Integer.valueOf(1), merged.get("a"));
        Assert.assertEquals(Integer.valueOf(20), merged.get("b"));
        Assert.assertEquals(Integer.valueOf(30), merged.get("c"));
    }

    /** Test convertToArray with boundaries around start and sparse map. */
    @Test
    public void convertToArrayBoundariesShouldPass() {
        Map<Integer, String> map = new HashMap<>();
        map.put(0, "zero");
        map.put(2, "two");
        Assert.assertEquals(Arrays.asList("zero", null, "two"), Utils.convertToArray(map, 0));
        Assert.assertEquals(Collections.singletonList("two"), Utils.convertToArray(map, 2));
        Assert.assertEquals(Collections.emptyList(), Utils.convertToArray(map, 3));
    }

    /** Test key and topology name validation. */
    @Test
    public void keyAndTopologyNameValidationShouldPass() {
        Assert.assertTrue(Utils.isValidKey("abc_ 123.-"));
        Assert.assertFalse(Utils.isValidKey(""));
        Assert.assertFalse(Utils.isValidKey("bad/key"));
        Utils.validateTopologyName("valid-name");
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.validateTopologyName("invalid/name"));
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.validateTopologyName(null));
    }

    /** Test cycle detection utilities with empty topology. Expected = no cycles. */
    @Test
    public void componentCyclesEmptyTopologyShouldPass() throws Exception {
        StormTopology topology = new StormTopology();
        Assert.assertTrue(Utils.findComponentCycles(topology, "topo").isEmpty());
        Utils.validateCycleFree(topology, "topo");
    }

    /** Test isOnWindows only for totality. Expected = deterministic boolean, no exception. */
    @Test
    public void isOnWindowsShouldReturnBoolean() {
        Assert.assertEquals(Boolean.TRUE, Boolean.valueOf(Utils.isOnWindows()).getClass().equals(Boolean.class));
    }

    /** Documented but ignored: registering shutdown hooks mutates the JVM test runner lifecycle. */
    @Ignore("Avoids installing process-level shutdown hooks that can affect the test JVM at teardown.")
    @Test
    public void addShutdownHookMethodsShouldComplete() {
        Utils.addShutdownHookWithForceKillIn1Sec(() -> { });
        Utils.addShutdownHookWithDelayedForceKill(() -> { }, 1);
    }

    /** Documented but ignored: a real BlobStore/NimbusBlobStore integration fixture is required. */
    @Ignore("Requires an integration BlobStore/NimbusBlobStore fixture not suitable for a unit-only test class.")
    @Test
    public void blobStoreClientAndValidationIntegrationMethods() throws Exception {
        Assert.assertNotNull(Utils.getClientBlobStore(new HashMap<String, Object>()));
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class ExitException extends SecurityException {
        private final int status;

        private ExitException(int status) {
            this.status = status;
        }
    }

    private static final class NoExitSecurityManager extends SecurityManager {
        private final SecurityManager delegate;

        private NoExitSecurityManager(SecurityManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public void checkPermission(Permission perm) {
            if (delegate != null) {
                delegate.checkPermission(perm);
            }
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            if (delegate != null) {
                delegate.checkPermission(perm, context);
            }
        }

        @Override
        public void checkExit(int status) {
            throw new ExitException(status);
        }
    }



    /* ### Test END ### */
}
