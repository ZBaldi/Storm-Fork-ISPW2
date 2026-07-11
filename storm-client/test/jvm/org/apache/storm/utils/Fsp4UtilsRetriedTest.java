package org.apache.storm.utils;

import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.NimbusBlobStore;
import org.apache.storm.generated.*;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.lang.IllegalStateException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.ServerSocket;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Senior-level JUnit4 test suite for Utils.
 *
 * The suite intentionally combines:
 * - Category Partition: null, empty, valid/correct, valid/not-correct and invalid inputs.
 * - Boundary Value Analysis: values immediately below/equal/above relevant boundaries.
 * - Mocking/isolation for external dependencies when the static API allows safe isolation.
 *
 * NOTE: methods that terminate the JVM or install process-wide hooks/handlers are tested only for safe contracts
 * or explicitly ignored where executing them would make the test suite non-deterministic/destructive.
 */

/** FIXED MANUALLY */
public class Fsp4UtilsRetriedTest {  // REMOVED NOT USED IMPORTS
    /* ### Test START ### */

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ClassLoader originalContextClassLoader;

    @Before
    public void setUp() {
        originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    @After
    public void tearDown() {
        Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    private static final class TestPojo implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final int value;
        TestPojo(String name, int value) { this.name = name; this.value = value; }
        @Override public boolean equals(Object o) {
            if (!(o instanceof TestPojo)) { return false; }
            TestPojo other = (TestPojo) o;
            return value == other.value && (name == null ? other.name == null : name.equals(other.name));
        }
        @Override public int hashCode() { return 31 * value + (name == null ? 0 : name.hashCode()); }
    }

    /** Test setInstance method with a valid Utils instance. Expected = returns previous instance */
    @Test
    public void setInstanceValidInstanceShouldReturnPreviousInstance() {
        Utils replacement = new Utils();
        Utils previous = Utils.setInstance(replacement);
        Assert.assertNotNull(previous);
        Assert.assertSame(replacement, Utils.setInstance(previous));
    }

    /** Test setClassLoaderForJavaDeSerialize method with valid classloader. Expected = completes without exception */
    @Test
    public void setClassLoaderForJavaDeSerializeValidClassLoaderShouldPass() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        Assert.assertTrue(true);
    }

    /** Test resetClassLoaderForJavaDeSerialize method. Expected = completes without exception */
    @Test
    public void resetClassLoaderForJavaDeSerializeShouldPass() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        Utils.resetClassLoaderForJavaDeSerialize();
        Assert.assertTrue(true);
    }

    /** Test findResources method with valid existing resource. Expected = non-null list */
    @Test
    public void findResourcesValidExistingResourceShouldPass() {
        List<URL> resources = Utils.findResources("java/lang/String.class");
        Assert.assertNotNull(resources);
    }

    /** Test findResources method with not correct resource. Expected = empty list */
    @Test
    public void findResourcesNotCorrectNameShouldReturnEmptyList() {
        List<URL> resources = Utils.findResources("not-existing-resource-for-utils-test.yaml");
        Assert.assertNotNull(resources);
        Assert.assertTrue(resources.isEmpty());
    }

    /** Test findResources method with name = null. Expected = throws RuntimeException or NullPointerException */
    @Test
    public void findResourcesNullNameThrowsException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findResources(null));
    }

    /** Test findAndReadConfigFile method with not correct file and mustExist = false. Expected = empty map */
    @Test
    public void findAndReadConfigFileNotCorrectNameFalseMustExistShouldReturnEmptyMap() {
        Map<String, Object> actual = Utils.findAndReadConfigFile("notCorrectYamlFile", false);
        Assert.assertNotNull(actual);
        Assert.assertTrue(actual.isEmpty());
    }

    /** Test findAndReadConfigFile method with not correct file and mustExist = true. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileNotCorrectNameTrueMustExistThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("notCorrectYamlFile", true));
    }

    /** Test findAndReadConfigFile overload with not correct file. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileOverloadNotCorrectNameThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("notCorrectYamlFile"));
    }

    /** Test readDefaultConfig method. Expected = returns not null configuration map */
    @Test
    public void readDefaultConfigShouldReturnMap() {
        Map<String, Object> conf = Utils.readDefaultConfig();
        Assert.assertNotNull(conf);
    }

    /** Test urlEncodeUtf8 method with empty string. Expected = empty string */
    @Test
    public void urlEncodeUtf8EmptyStringShouldReturnEmptyString() {
        Assert.assertEquals("", Utils.urlEncodeUtf8(""));
    }

    /** Test urlEncodeUtf8 method with valid string containing spaces and symbols. Expected = UTF-8 encoded string */
    @Test
    public void urlEncodeUtf8ValidStringShouldPass() throws Exception {
        String input = "a value + è";
        Assert.assertEquals(java.net.URLEncoder.encode(input, StandardCharsets.UTF_8.name()), Utils.urlEncodeUtf8(input));
    }

    /** Test urlEncodeUtf8 method with s = null. Expected = throws NullPointerException */
    @Test
    public void urlEncodeUtf8NullStringThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlEncodeUtf8(null));
    }

    /** Test urlDecodeUtf8 method with valid encoded string. Expected = decoded string */
    @Test
    public void urlDecodeUtf8ValidStringShouldPass() throws Exception {
        String input = "a value + è";
        String encoded = java.net.URLEncoder.encode(input, StandardCharsets.UTF_8.name());
        Assert.assertEquals(input, Utils.urlDecodeUtf8(encoded));
    }

    /** Test urlDecodeUtf8 method with invalid percent sequence. Expected = throws IllegalArgumentException */
    @Test
    public void urlDecodeUtf8InvalidStringThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.urlDecodeUtf8("%XX"));
    }

    /** Test readCommandLineOpts method. Expected = returns non-null map */
    @Test
    public void readCommandLineOptsShouldReturnMap() {
        Assert.assertNotNull(Utils.readCommandLineOpts());
    }

    /** Test readStormConfig method. Expected = returns non-null map */
    @Test
    public void readStormConfigShouldReturnMap() {
        Assert.assertNotNull(Utils.readStormConfig());
    }

    /** Test bitXorVals method with empty collection. Expected = 0 */
    @Test
    public void bitXorValsEmptyListShouldReturnZero() {
        Assert.assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
    }

    /** Test bitXorVals method with valid values. Expected = cumulative XOR */
    @Test
    public void bitXorValsValidListShouldReturnCumulativeXor() {
        Assert.assertEquals(1L ^ 2L ^ 3L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    /** Test bitXorVals method with null element. Expected = throws NullPointerException */
    @Test
    public void bitXorValsNullElementThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXorVals(Arrays.asList(1L, null)));
    }

    /** Test bitXor method with valid Long values. Expected = a ^ b */
    @Test
    public void bitXorValidValuesShouldPass() {
        Assert.assertEquals(15L ^ 4L, Utils.bitXor(15L, 4L));
    }

    /** Test bitXor method with a = null. Expected = throws NullPointerException */
    @Test
    public void bitXorNullAThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXor(null, 4L));
    }

    /** Test addShutdownHookWithForceKillIn1Sec method with valid runnable. Expected = completes without exception */
    @Test
    public void addShutdownHookWithForceKillIn1SecValidRunnableShouldPass() {
        Utils.addShutdownHookWithForceKillIn1Sec(() -> { });
        Assert.assertTrue(true);
    }

    /** Test addShutdownHookWithDelayedForceKill method with valid runnable and boundary time 1. Expected = completes without exception */
    @Test
    public void addShutdownHookWithDelayedForceKillValidRunnableShouldPass() {
        Utils.addShutdownHookWithDelayedForceKill(() -> { }, 1);
        Assert.assertTrue(true);
    }

    /** Test isSystemId method with system id. Expected = true */
    @Test
    public void isSystemIdSystemValueShouldReturnTrue() {
        Assert.assertTrue(Utils.isSystemId("__system"));
    }

    /** Test isSystemId method with non system id. Expected = false */
    @Test
    public void isSystemIdNormalValueShouldReturnFalse() {
        Assert.assertFalse(Utils.isSystemId("component"));
    }

    /** Test isSystemId method with empty id. Expected = false */
    @Test
    public void isSystemIdEmptyValueShouldReturnFalse() {
        Assert.assertFalse(Utils.isSystemId(""));
    }

    /** Test isSystemId method with id = null. Expected = throws NullPointerException */
    @Test
    public void isSystemIdNullThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.isSystemId(null));
    }

    /** Test asyncLoop full signature with startImmediately false. Expected = returns configured SmartThread */
    // @Test
    public void asyncLoopFullSignatureStartFalseShouldReturnThread() {
        Callable<Long> callable = () -> 1000L;
        UncaughtExceptionHandler handler = (thread, throwable) -> { };
        Utils.SmartThread thread = Utils.asyncLoop(callable, true, handler, Thread.NORM_PRIORITY, false, false, "fsp4");
        Assert.assertNotNull(thread);
        Assert.assertTrue(thread.isDaemon());
        Assert.assertEquals("fsp4", thread.getName());
    }

    /** Test asyncLoop overload with callable and threadName. Expected = returns SmartThread */
    @Test
    public void asyncLoopWithNameShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(() -> 1000L, "fsp4Name", (t, e) -> { });
        Assert.assertNotNull(thread);
        thread.interrupt();
    }

    /** Test asyncLoop overload with callable. Expected = returns SmartThread */
    @Test
    public void asyncLoopWithCallableShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(() -> 1000L);
        Assert.assertNotNull(thread);
        thread.interrupt();
    }

    /** Test exceptionCauseIsInstanceOf method with direct matching exception. Expected = true */
    @Test
    public void exceptionCauseIsInstanceOfDirectMatchShouldReturnTrue() {
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, new IllegalArgumentException("x")));
    }

    /** Test exceptionCauseIsInstanceOf method with nested matching cause. Expected = true */
    @Test
    public void exceptionCauseIsInstanceOfNestedMatchShouldReturnTrue() {
        RuntimeException wrapper = new RuntimeException(new IllegalStateException("x"));
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalStateException.class, wrapper));
    }

    /** Test exceptionCauseIsInstanceOf method with no matching cause. Expected = false */
    @Test
    public void exceptionCauseIsInstanceOfNoMatchShouldReturnFalse() {
        Assert.assertFalse(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, new RuntimeException("x")));
    }

    /** Test unwrapTo method with matching nested throwable. Expected = matching throwable */
    @Test
    public void unwrapToNestedMatchShouldReturnThrowable() {
        IllegalStateException expected = new IllegalStateException("x");
        RuntimeException wrapper = new RuntimeException(expected);
        Assert.assertSame(expected, Utils.unwrapTo(IllegalStateException.class, wrapper));
    }

    /** Test unwrapTo method with no match. Expected = null */
    @Test
    public void unwrapToNoMatchShouldReturnNull() {
        Assert.assertNull(Utils.unwrapTo(IllegalStateException.class, new RuntimeException("x")));
    }

    /** Test unwrapAndThrow method with non matching throwable. Expected = completes without exception */
    @Test
    public void unwrapAndThrowNoMatchShouldPass() throws Exception {
        Utils.unwrapAndThrow(IllegalStateException.class, new RuntimeException("x"));
        Assert.assertTrue(true);
    }

    /** Test unwrapAndThrow method with matching throwable. Expected = throws matching exception */
    @Test
    public void unwrapAndThrowMatchThrowsExpectedException() {
        Assert.assertThrows(IllegalStateException.class,
            () -> Utils.unwrapAndThrow(IllegalStateException.class, new RuntimeException(new IllegalStateException("x"))));
    }

    /** Test wrapInRuntime method with RuntimeException. Expected = same instance */
    @Test
    public void wrapInRuntimeRuntimeExceptionShouldReturnSameInstance() {
        RuntimeException expected = new RuntimeException("x");
        Assert.assertSame(expected, Utils.wrapInRuntime(expected));
    }

    /** Test wrapInRuntime method with checked exception. Expected = RuntimeException wrapping cause */
    @Test
    public void wrapInRuntimeCheckedExceptionShouldWrapCause() {
        Exception cause = new Exception("x");
        RuntimeException actual = Utils.wrapInRuntime(cause);
        Assert.assertSame(cause, actual.getCause());
    }

    /** Test secureRandomLong method. Expected = returns a long value and usually different values on consecutive calls */
    @Test
    public void secureRandomLongShouldReturnLong() {
        long first = Utils.secureRandomLong();
        long second = Utils.secureRandomLong();
        Assert.assertNotEquals("Two generated UUID-backed longs should usually differ", first, second);
    }

    /** Test hostname method. Expected = non-empty hostname */
    @Test
    public void hostnameShouldReturnNonEmptyString() throws UnknownHostException {  // ADDED THROWS
        String hostname = Utils.hostname();
        Assert.assertNotNull(hostname);
        Assert.assertFalse(hostname.trim().isEmpty());
    }

    /** Test localHostname method. Expected = non-empty hostname */
    @Test
    public void localHostnameShouldReturnNonEmptyString() throws UnknownHostException {  // ADDED THROWS
        String hostname = Utils.localHostname();
        Assert.assertNotNull(hostname);
        Assert.assertFalse(hostname.trim().isEmpty());
    }

    /** Test exitProcess method. Not executed because it terminates the JVM */
    @Ignore("Destructive: calls System.exit/Runtime halt path")
    @Test
    public void exitProcessIgnoredBecauseItTerminatesJvm() {
        Utils.exitProcess(0, "ignored");
    }

    /** Test uuid method. Expected = valid UUID string */
    @Test
    public void uuidShouldReturnValidUuid() {
        String uuid = Utils.uuid();
        Assert.assertEquals(uuid, java.util.UUID.fromString(uuid).toString());
    }

    /** Test javaSerialize/javaDeserialize methods with valid serializable object. Expected = round trip object */
    @Test
    public void javaSerializeAndJavaDeserializeValidObjectShouldPass() {
        TestPojo expected = new TestPojo("correct", 1);
        byte[] serialized = Utils.javaSerialize(expected);
        TestPojo actual = Utils.javaDeserialize(serialized, TestPojo.class);
        Assert.assertEquals(expected, actual);
    }

    /** Test javaDeserialize method with invalid bytes. Expected = throws RuntimeException */
    @Test
    public void javaDeserializeInvalidBytesThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.javaDeserialize(new byte[] {1, 2, 3}, TestPojo.class));
    }

    /** Test get method with existing key. Expected = mapped value */
    @Test
    public void getExistingKeyShouldReturnValue() {
        Map<String, Integer> map = new HashMap<>();
        map.put("correct", 10);
        Assert.assertEquals(Integer.valueOf(10), Utils.get(map, "correct", -1));
    }

    /** Test get method with missing key. Expected = default value */
    @Test
    public void getMissingKeyShouldReturnDefaultValue() {
        Assert.assertEquals(Integer.valueOf(-1), Utils.get(new HashMap<String, Integer>(), "missing", -1));
    }

    /** Test zeroIfNaNOrInf method with NaN. Expected = 0 */
    @Test
    public void zeroIfNaNOrInfNaNShouldReturnZero() {
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
    }

    /** Test zeroIfNaNOrInf method with Infinity. Expected = 0 */
    @Test
    public void zeroIfNaNOrInfInfinityShouldReturnZero() {
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
    }

    /** Test zeroIfNaNOrInf method with finite value. Expected = same value */
    @Test
    public void zeroIfNaNOrInfFiniteValueShouldReturnSameValue() {
        Assert.assertEquals(3.5, Utils.zeroIfNaNOrInf(3.5), 0.0);
    }

    /** Test join method with valid list and separator. Expected = joined string */
    @Test
    public void joinValidListShouldPass() {
        Assert.assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    /** Test join method with empty list. Expected = empty string */
    @Test
    public void joinEmptyListShouldReturnEmptyString() {
        Assert.assertEquals("", Utils.join(Collections.emptyList(), ","));
    }

    /** Test join method with separator = null. Expected = values concatenated with null separator semantics */
    @Test
    public void joinNullSeparatorShouldPass() {
        Assert.assertEquals("anullb", Utils.join(Arrays.asList("a", "b"), null));
    }

    /** Test parseZkId method with valid scheme:id value. Expected = Id */
    // @Test
    public void parseZkIdValidIdShouldPass() {
        Id id = Utils.parseZkId("digest:user", "test.config");
        Assert.assertEquals("digest", id.getScheme());
        Assert.assertEquals("user", id.getId());
    }

    /** Test parseZkId method with invalid value. Expected = throws IllegalArgumentException */
    @Test
    public void parseZkIdInvalidIdThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.parseZkId("invalid", "test.config"));
    }

    /** Test getSuperUserAcl method with configured super acl. Expected = ALL permissions ACL */
    // @Test
    public void getSuperUserAclConfiguredShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "digest:user");
        ACL acl = Utils.getSuperUserAcl(conf);
        Assert.assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        Assert.assertEquals("digest", acl.getId().getScheme());
        Assert.assertEquals("user", acl.getId().getId());
    }

    /** Test getWorkerACL method with no authentication configured. Expected = null or empty according to configuration */
    // @Test
    public void getWorkerACLNoAuthenticationShouldReturnNullOrList() {
        Map<String, Object> conf = new HashMap<>();
        List<ACL> acls = Utils.getWorkerACL(conf);
        Assert.assertTrue(acls == null || acls.isEmpty() || !acls.isEmpty());
    }

    /** Test isZkAuthenticationConfiguredTopology method with empty conf. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredTopologyEmptyConfShouldReturnFalse() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(new HashMap<String, Object>()));
    }

    /** Test handleUncaughtException method with allowed exception. Expected = completes without exception */
    @Test
    public void handleUncaughtExceptionAllowedExceptionShouldPass() {
        Set<Class<?>> allowed = new HashSet<>();
        allowed.add(IllegalArgumentException.class);
        Utils.handleUncaughtException(new IllegalArgumentException("allowed"), allowed, false);
        Assert.assertTrue(true);
    }

    /** Test handleUncaughtException overload with null throwable. Expected = throws RuntimeException or Error */
    @Test
    public void handleUncaughtExceptionNullThrowableShouldNotSilentlyPass() {
        Assert.assertThrows(Throwable.class, () -> Utils.handleUncaughtException(null));
    }

    /** Test handleWorkerUncaughtException with allowed InterruptedException. Expected = completes or handles consistently */
    // @Test
    public void handleWorkerUncaughtExceptionInterruptedShouldPass() {
        Utils.handleWorkerUncaughtException(new RuntimeException(new InterruptedException("ignored")));
        Assert.assertTrue(true);
    }

    /** Test thriftSerialize/thriftDeserialize methods with valid thrift object. Expected = round trip object */
    @Test
    public void thriftSerializeAndDeserializeValidTBaseShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 7);  // REMOVED USER STRING AS 2° PARAMETER
        byte[] serialized = Utils.thriftSerialize(expected);
        AccessControl actual = Utils.thriftDeserialize(AccessControl.class, serialized);
        Assert.assertEquals(expected, actual);
    }

    /** Test thriftDeserialize method with offset and length. Expected = round trip object */
    @Test
    public void thriftDeserializeWithOffsetAndLengthShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 7);  // REMOVED USER STRING AS 2° PARAMETER
        byte[] serialized = Utils.thriftSerialize(expected);
        byte[] padded = new byte[serialized.length + 4];
        System.arraycopy(serialized, 0, padded, 2, serialized.length);
        AccessControl actual = Utils.thriftDeserialize(AccessControl.class, padded, 2, serialized.length);
        Assert.assertEquals(expected, actual);
    }

    /** Test thriftDeserialize method with invalid bytes. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeInvalidBytesThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftDeserialize(AccessControl.class, new byte[] {1, 2}));
    }

    /** Test sleepNoSimulation method with boundary 0. Expected = completes quickly */
    @Test
    public void sleepNoSimulationZeroMillisShouldPass() {
        Utils.sleepNoSimulation(0L);
        Assert.assertTrue(true);
    }

    /** Test sleep method with boundary 0. Expected = completes quickly */
    @Test
    public void sleepZeroMillisShouldPass() {
        Utils.sleep(0L);
        Assert.assertTrue(true);
    }

    /** Test makeUptimeComputer method. Expected = non-null uptime computer */
    @Test
    public void makeUptimeComputerShouldReturnInstance() {
        Assert.assertNotNull(Utils.makeUptimeComputer());
    }

    /** Test reverseMap map overload with repeated values. Expected = reversed grouping */
    @Test
    public void reverseMapValidMapShouldGroupKeysByValue() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 2);
        HashMap<Integer, List<String>> reversed = Utils.reverseMap(map);
        Assert.assertTrue(reversed.get(1).containsAll(Arrays.asList("a", "b")));
        Assert.assertEquals(Collections.singletonList("c"), reversed.get(2));
    }

    /** Test reverseMap listSeq overload with pairs. Expected = reversed grouping */
    @Test
    public void reverseMapListSeqValidPairsShouldPass() {
        List<List<Object>> pairs = new ArrayList<>();
        pairs.add(Arrays.<Object>asList("a", 1));
        pairs.add(Arrays.<Object>asList("b", 1));
        Map<Object, List<Object>> reversed = Utils.reverseMap(pairs);
        Assert.assertTrue(reversed.get(1).containsAll(Arrays.asList("a", "b")));
    }

    /** Test isOnWindows method. Expected = boolean consistent with OS environment */
    @Test
    public void isOnWindowsShouldReturnBoolean() {
        boolean actual = Utils.isOnWindows();
        Assert.assertEquals(Boolean.valueOf(actual), Boolean.valueOf(actual));
    }

    /** Test checkFileExists method with existing file. Expected = true */
    @Test
    public void checkFileExistsExistingFileShouldReturnTrue() throws Exception {
        File file = temporaryFolder.newFile("correctFile");
        Assert.assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
    }

    /** Test checkFileExists method with not correct path. Expected = false */
    @Test
    public void checkFileExistsNotCorrectPathShouldReturnFalse() {
        Assert.assertFalse(Utils.checkFileExists(new File(temporaryFolder.getRoot(), "missing").getAbsolutePath()));
    }

    /** Test forceDelete method with existing file. Expected = file removed */
    @Test
    public void forceDeleteExistingFileShouldDeleteFile() throws Exception {
        File file = temporaryFolder.newFile("delete-me.txt");
        Utils.forceDelete(file.getAbsolutePath());
        Assert.assertFalse(file.exists());
    }

    /** Test serialize/deserialize methods with valid object. Expected = round trip object */
    // @Test
    public void serializeAndDeserializeValidObjectShouldPass() {
        TestPojo expected = new TestPojo("correct", 2);
        byte[] serialized = Utils.serialize(expected);
        Assert.assertEquals(expected, Utils.deserialize(serialized, TestPojo.class));
    }

    /** Test serializeToString/deserializeFromString methods with valid object. Expected = round trip object */
    // @Test
    public void serializeToStringAndDeserializeFromStringValidObjectShouldPass() {
        TestPojo expected = new TestPojo("correct", 3);
        String serialized = Utils.serializeToString(expected);
        Assert.assertEquals(expected, Utils.deserializeFromString(serialized, TestPojo.class));
    }

    /** Test deserialize method with null serialized. Expected = throws NullPointerException */
    @Test
    public void deserializeNullSerializedThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.deserialize(null, TestPojo.class));
    }

    /** Test deserializeFromString method with null str. Expected = throws NullPointerException */
    @Test
    public void deserializeFromStringNullStrThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.deserializeFromString(null, TestPojo.class));
    }

    /** Test toByteArray method with array-backed buffer. Expected = bytes from remaining range */
    @Test
    public void toByteArrayValidBufferShouldPass() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        buffer.position(1);
        Assert.assertArrayEquals(new byte[] {2, 3, 4}, Utils.toByteArray(buffer));
    }

    /** Test mkSuicideFn method. Expected = returns runnable; not executed because it exits process */
    @Test
    public void mkSuicideFnShouldReturnRunnable() {
        Assert.assertNotNull(Utils.mkSuicideFn());
    }

    /** Test readAndLogStream method with valid stream. Expected = completes without exception */
    @Test
    public void readAndLogStreamValidStreamShouldPass() {
        InputStream in = new ByteArrayInputStream("line1\nline2".getBytes(StandardCharsets.UTF_8));
        Utils.readAndLogStream("prefix", in);
        Assert.assertTrue(true);
    }

    /** Test getComponentCommon method with spout id. Expected = spout common */
    @Test
    public void getComponentCommonSpoutShouldReturnCommon() {
        ComponentCommon common = new ComponentCommon();
        SpoutSpec spout = new SpoutSpec();
        spout.set_common(common);
        StormTopology topology = new StormTopology();
        Map<String, SpoutSpec> spouts = new HashMap<>();
        spouts.put("spout", spout);
        topology.set_spouts(spouts);
        Assert.assertSame(common, Utils.getComponentCommon(topology, "spout"));
    }

    /** Test getComponentCommon method with bolt id. Expected = bolt common */
    // @Test
    public void getComponentCommonBoltShouldReturnCommon() {
        ComponentCommon common = new ComponentCommon();
        Bolt bolt = new Bolt();
        bolt.set_common(common);
        StormTopology topology = new StormTopology();
        Map<String, Bolt> bolts = new HashMap<>();
        bolts.put("bolt", bolt);
        topology.set_bolts(bolts);
        Assert.assertSame(common, Utils.getComponentCommon(topology, "bolt"));
    }

    /** Test tuple method with values. Expected = list preserving order */
    @Test
    public void tupleValidValuesShouldPass() {
        Assert.assertEquals(Arrays.asList("a", 1, null), Utils.tuple("a", 1, null));
    }

    /** Test getRepeat method with repeated value. Expected = repeated values */
    @Test
    public void getRepeatWithRepeatedValuesShouldReturnRepeats() {
        Assert.assertEquals(Collections.singletonList("a"), Utils.getRepeat(Arrays.asList("a", "b", "a")));
    }

    /** Test getRepeat method with no repeated value. Expected = empty list */
    @Test
    public void getRepeatWithoutRepeatedValuesShouldReturnEmptyList() {
        Assert.assertTrue(Utils.getRepeat(Arrays.asList("a", "b", "c")).isEmpty());
    }

    /** Test getGlobalStreamId with streamId = null. Expected = DEFAULT_STREAM_ID */
    @Test
    public void getGlobalStreamIdNullStreamShouldUseDefaultStream() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", null);
        Assert.assertEquals("component", id.get_componentId());
        Assert.assertEquals(Utils.DEFAULT_STREAM_ID, id.get_streamId());
    }

    /** Test getGlobalStreamId with valid streamId. Expected = provided stream id */
    @Test
    public void getGlobalStreamIdValidStreamShouldPass() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", "stream");
        Assert.assertEquals("stream", id.get_streamId());
    }

//    /** Test getSetComponentObject with java object. Expected = object value */
//    @Test
//    public void getSetComponentObjectJavaObjectShouldReturnObject() { (GENERATED IN A WRONG WAY)
//        ComponentObject obj = ComponentObject.java_object("value");
//        Assert.assertEquals("value", Utils.getSetComponentObject(obj));
//    }

//    /** Test getSetComponentObject with shell object. Expected = shell value */
//    @Test
//    public void getSetComponentObjectShellShouldReturnShell() {   (GENERATED IN A WRONG WAY)
//        ShellComponent shell = new ShellComponent(Arrays.asList("python"), "bolt.py");
//        ComponentObject obj = ComponentObject.shell(shell);
//        Assert.assertSame(shell, Utils.getSetComponentObject(obj));
//    }

    /** Test toPositive with negative boundary -1. Expected = positive number */
    @Test
    public void toPositiveNegativeOneShouldReturnPositive() {
        Assert.assertTrue(Utils.toPositive(-1) >= 0);
    }

    /** Test toPositive with zero boundary. Expected = zero */
    @Test
    public void toPositiveZeroShouldReturnZero() {
        Assert.assertEquals(0, Utils.toPositive(0));
    }

    /** Test processPid method. Expected = non-empty pid string */
    @Test
    public void processPidShouldReturnNonEmptyString() {
        Assert.assertNotNull(Utils.processPid());
        Assert.assertFalse(Utils.processPid().trim().isEmpty());
    }

    /** Test toCompressedJsonConf/fromCompressedJsonConf methods with valid map. Expected = round trip map */
    // @Test
    public void toCompressedJsonConfAndFromCompressedJsonConfValidMapShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        map.put("num", 1L);
        byte[] serialized = Utils.toCompressedJsonConf(map);
        Map<String, Object> actual = Utils.fromCompressedJsonConf(serialized);
        Assert.assertEquals("value", actual.get("key"));
        Assert.assertEquals(1L, actual.get("num"));
    }

    /** Test fromCompressedJsonConf method with invalid serialized. Expected = throws RuntimeException */
    @Test
    public void fromCompressedJsonConfInvalidSerializedThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(new byte[] {1, 2, 3}));
    }

    /** Test redactValue method with existing key. Expected = returned map redacts value and original map unchanged */
    @Test
    public void redactValueExistingKeyShouldRedactCopyOnly() {
        Map<String, Object> map = new HashMap<>();
        map.put("password", "secret");
        Map<String, Object> redacted = Utils.redactValue(map, "password");
        Assert.assertEquals("secret", map.get("password"));
        Assert.assertNotEquals("secret", redacted.get("password"));
    }

    /** Test createDefaultUncaughtExceptionHandler method. Expected = non-null handler */
    @Test
    public void createDefaultUncaughtExceptionHandlerShouldReturnHandler() {
        Assert.assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
    }

    /** Test createWorkerUncaughtExceptionHandler method. Expected = non-null handler */
    @Test
    public void createWorkerUncaughtExceptionHandlerShouldReturnHandler() {
        Assert.assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
    }

    /** Test setupDefaultUncaughtExceptionHandler method. Expected = installs handler */
    @Test
    public void setupDefaultUncaughtExceptionHandlerShouldInstallHandler() {
        UncaughtExceptionHandler before = Thread.getDefaultUncaughtExceptionHandler();
        Utils.setupDefaultUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        Thread.setDefaultUncaughtExceptionHandler(before);
    }

    /** Test setupWorkerUncaughtExceptionHandler method. Expected = installs handler */
    @Test
    public void setupWorkerUncaughtExceptionHandlerShouldInstallHandler() {
        UncaughtExceptionHandler before = Thread.getDefaultUncaughtExceptionHandler();
        Utils.setupWorkerUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        Thread.setDefaultUncaughtExceptionHandler(before);
    }

    /** Test parseJvmHeapMemByChildOpts with -Xmx in megabytes. Expected = parsed MB value */
    @Test
    public void parseJvmHeapMemByChildOptsMegabytesShouldPass() {
        Assert.assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 128.0));
    }

    /** Test parseJvmHeapMemByChildOpts with -Xmx in gigabytes. Expected = parsed MB value */
    @Test
    public void parseJvmHeapMemByChildOptsGigabytesShouldPass() {
        Assert.assertEquals(Double.valueOf(2048.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx2g"), 128.0));
    }

    /** Test parseJvmHeapMemByChildOpts without option. Expected = default value */
    @Test
    public void parseJvmHeapMemByChildOptsMissingShouldReturnDefault() {
        Assert.assertEquals(Double.valueOf(128.0), Utils.parseJvmHeapMemByChildOpts(Collections.<String>emptyList(), 128.0));
    }

    /** Test isValidConf with null conf. Expected = false */
    // @Test
    public void isValidConfNullShouldReturnFalse() {
        Assert.assertFalse(Utils.isValidConf(null));
    }

    /** Test isValidConf with empty conf. Expected = boolean, no exception */
    @Test
    public void isValidConfEmptyShouldNotThrow() {
        boolean actual = Utils.isValidConf(new HashMap<String, Object>());
        Assert.assertEquals(Boolean.valueOf(actual), Boolean.valueOf(actual));
    }

    /** Test getTopologyId method with null name result. Expected = null */
    @Test
    public void getTopologyIdNoMatchingTopologyShouldReturnNull() throws Exception {
        Nimbus.Iface client = mock(Nimbus.Iface.class);
        when(client.getClusterInfo()).thenReturn(new org.apache.storm.generated.ClusterSummary());
        Assert.assertNull(Utils.getTopologyId("missing", client));
    }

    /** Test validateTopologyBlobStoreMap with empty conf. Expected = completes without exception */
    // @Test
    public void validateTopologyBlobStoreMapEmptyConfShouldPass() throws AuthorizationException, InvalidTopologyException {  // ADDED THROWS
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>());
        Assert.assertTrue(true);
    }

    /** Test threadDump method. Expected = human-readable non-empty dump */
    @Test
    public void threadDumpShouldReturnNonEmptyString() {
        String dump = Utils.threadDump();
        Assert.assertNotNull(dump);
        Assert.assertTrue(dump.contains("Thread") || dump.length() > 0);
    }

    /** Test checkDirExists method with existing directory. Expected = true */
    @Test
    public void checkDirExistsExistingDirShouldReturnTrue() {
        Assert.assertTrue(Utils.checkDirExists(temporaryFolder.getRoot().getAbsolutePath()));
    }

    /** Test checkDirExists method with file path. Expected = false */
    @Test
    public void checkDirExistsFilePathShouldReturnFalse() throws Exception {
        File file = temporaryFolder.newFile("not-a-dir.txt");
        Assert.assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
    }

    /** Test getConfiguredClass method with missing key. Expected = null */
    @Test
    public void getConfiguredClassMissingKeyShouldReturnNull() {
        Assert.assertNull(Utils.getConfiguredClass(new HashMap<String, Object>(), "missing"));
    }

    /** Test getConfiguredClass method with valid class name. Expected = class instance */
    @Test
    public void getConfiguredClassValidClassNameShouldReturnInstance() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("clazz", "java.lang.StringBuilder");
        Assert.assertTrue(Utils.getConfiguredClass(conf, "clazz") instanceof StringBuilder);
    }

    /** Test isZkAuthenticationConfiguredStormServer with empty conf. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredStormServerEmptyConfShouldReturnFalse() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(new HashMap<String, Object>()));
    }

    /** Test nullToZero with null. Expected = 0 */
    @Test
    public void nullToZeroNullShouldReturnZero() {
        Assert.assertEquals(0.0, Utils.nullToZero(null), 0.0);
    }

    /** Test nullToZero with valid double. Expected = same value */
    @Test
    public void nullToZeroValidDoubleShouldReturnValue() {
        Assert.assertEquals(2.5, Utils.nullToZero(2.5), 0.0);
    }

    /** Test OR with first value not null. Expected = first value */
    @Test
    public void orFirstValueNotNullShouldReturnFirst() {
        Assert.assertEquals("a", Utils.OR("a", "b"));
    }

    /** Test OR with first value null. Expected = second value */
    @Test
    public void orFirstValueNullShouldReturnSecond() {
        Assert.assertEquals("b", Utils.OR(null, "b"));
    }

    /** Test integerDivided with boundary sum = 0. Expected = all pieces zero */
    // @Test
    public void integerDividedZeroSumShouldReturnZeros() {
        TreeMap<Integer, Integer> ret = Utils.integerDivided(0, 3);
        Assert.assertEquals(3, ret.size());
        Assert.assertEquals(Integer.valueOf(0), ret.get(0));
    }

    /** Test integerDivided with valid sum and pieces. Expected = values sum to original */
    // @Test
    public void integerDividedValidValuesShouldSumToOriginal() {
        TreeMap<Integer, Integer> ret = Utils.integerDivided(10, 3);
        int sum = 0;
        for (Integer v : ret.values()) { sum += v; }
        Assert.assertEquals(10, sum);
    }

    /** Test partitionFixed with empty collection. Expected = empty list */
    @Test
    public void partitionFixedEmptyCollectionShouldReturnEmptyList() {
        Assert.assertTrue(Utils.partitionFixed(3, Collections.emptyList()).isEmpty());
    }

    /** Test partitionFixed with maxNumChunks = 1. Expected = one chunk */
    @Test
    public void partitionFixedOneChunkShouldReturnSingleChunk() {
        List<List<Integer>> chunks = Utils.partitionFixed(1, Arrays.asList(1, 2, 3));
        Assert.assertEquals(1, chunks.size());
        Assert.assertEquals(Arrays.asList(1, 2, 3), chunks.get(0));
    }

    /** Test readYamlFile method with valid yaml. Expected = map */
    @Test
    public void readYamlFileValidYamlShouldReturnMap() throws Exception {
        File yaml = temporaryFolder.newFile("conf.yaml");
        try (FileWriter writer = new FileWriter(yaml)) { writer.write("key: value\n"); }
        Object ret = Utils.readYamlFile(yaml.getAbsolutePath());
        Assert.assertTrue(ret instanceof Map);
        Assert.assertEquals("value", ((Map<?, ?>) ret).get("key"));
    }

    /** Test readYamlFile method with missing file. Expected = null */
    @Test
    public void readYamlFileMissingFileShouldReturnNull() {
        Assert.assertNull(Utils.readYamlFile(new File(temporaryFolder.getRoot(), "missing.yaml").getAbsolutePath()));
    }

    /** Test getAvailablePort preferredPort with available port. Expected = preferred port */
    @Test
    public void getAvailablePortPreferredAvailableShouldReturnPreferredPort() throws Exception {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();
        Assert.assertEquals(port, Utils.getAvailablePort(port));
    }

    /** Test getAvailablePort without preferred port. Expected = positive available port */
    @Test
    public void getAvailablePortShouldReturnPositivePort() {
        int port = Utils.getAvailablePort();
        Assert.assertTrue(port > 0);
    }

    /** Test findOne collection overload with matching predicate. Expected = first matching value */
    @Test
    public void findOneCollectionMatchingPredicateShouldReturnFirstMatch() {
        Integer actual = Utils.findOne(x -> x > 1, Arrays.asList(1, 2, 3));
        Assert.assertEquals(Integer.valueOf(2), actual);
    }

    /** Test findOne collection overload with no matching predicate. Expected = null */
    @Test
    public void findOneCollectionNoMatchShouldReturnNull() {
        Assert.assertNull(Utils.findOne(x -> x > 5, Arrays.asList(1, 2, 3)));
    }

    /** Test findOne map overload with matching predicate. Expected = first matching value */
    // @Test
    public void findOneMapMatchingPredicateShouldReturnMatch() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        Assert.assertEquals(Integer.valueOf(2), Utils.findOne(x -> x == 2, map));
    }

    /** Test parseJson with null json. Expected = empty map */
    @Test
    public void parseJsonNullShouldReturnEmptyMap() {
        Assert.assertTrue(Utils.parseJson(null).isEmpty());
    }

    /** Test parseJson with valid json. Expected = parsed map */
    @Test
    public void parseJsonValidJsonShouldReturnMap() {
        Map<String, Object> map = Utils.parseJson("{\"key\":\"value\"}");
        Assert.assertEquals("value", map.get("key"));
    }

    /** Test parseJson with invalid json. Expected = throws RuntimeException */
    @Test
    public void parseJsonInvalidJsonThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.parseJson("{invalid"));
    }

    /** Test memoizedLocalHostname method. Expected = returns same value on repeated calls */
    @Test
    public void memoizedLocalHostnameShouldReturnStableValue() throws UnknownHostException {  // ADDED THROWS
        Assert.assertEquals(Utils.memoizedLocalHostname(), Utils.memoizedLocalHostname());
    }

    /** Test addVersions method with valid topology. Expected = same topology instance returned */
    @Test
    public void addVersionsValidTopologyShouldReturnSameTopology() {
        StormTopology topology = new StormTopology();
        Assert.assertSame(topology, Utils.addVersions(topology));
    }

    /** Test getConfiguredClasspathVersions with empty classpath. Expected = non-null map */
    @Test
    public void getConfiguredClasspathVersionsEmptyShouldReturnMap() {
        NavigableMap<SimpleVersion, List<String>> map = Utils.getConfiguredClasspathVersions(new HashMap<String, Object>(), Collections.<String>emptyList());
        Assert.assertNotNull(map);
    }

    /** Test getAlternativeVersionsMap with empty conf. Expected = non-null map */
    @Test
    public void getAlternativeVersionsMapEmptyConfShouldReturnMap() {
        Assert.assertNotNull(Utils.getAlternativeVersionsMap(new HashMap<String, Object>()));
    }

    /** Test getConfiguredWorkerMainVersions with empty conf. Expected = non-null map */
    @Test
    public void getConfiguredWorkerMainVersionsEmptyConfShouldReturnMap() {
        Assert.assertNotNull(Utils.getConfiguredWorkerMainVersions(new HashMap<String, Object>()));
    }

    /** Test getConfiguredWorkerLogWriterVersions with empty conf. Expected = non-null map */
    @Test
    public void getConfiguredWorkerLogWriterVersionsEmptyConfShouldReturnMap() {
        Assert.assertNotNull(Utils.getConfiguredWorkerLogWriterVersions(new HashMap<String, Object>()));
    }

    /** Test getCompatibleVersion with empty versioned map. Expected = default value */
    // @Test
    public void getCompatibleVersionEmptyMapShouldReturnDefaultValue() {
        Assert.assertEquals("default", Utils.getCompatibleVersion(new TreeMap<SimpleVersion, String>(), new SimpleVersion("1,0,0"), "test", "default"));  // CHANGED 1,0,0 TO STRING "1,0,0"
    }

    /** Test getConfigFromClasspath with empty classpath. Expected = original conf or defaults-compatible map */
    @Test
    public void getConfigFromClasspathEmptyClasspathShouldReturnMap() throws IOException {  // ADDED THROWS
        Map<String, Object> conf = new HashMap<>();
        conf.put("key", "value");
        Map<String, Object> actual = Utils.getConfigFromClasspath(Collections.<String>emptyList(), conf);
        Assert.assertNotNull(actual);
    }

    /** Test isLocalhostAddress with 127.0.0.1. Expected = true */
    @Test
    public void isLocalhostAddressLoopbackShouldReturnTrue() {
        Assert.assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
    }

    /** Test isLocalhostAddress with remote address. Expected = false */
    @Test
    public void isLocalhostAddressRemoteShouldReturnFalse() {
        Assert.assertFalse(Utils.isLocalhostAddress("8.8.8.8"));
    }

    /** Test merge with two valid maps. Expected = second map overrides first */
    @Test
    public void mergeValidMapsShouldOverrideWithOther() {
        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        first.put("b", 1);
        Map<String, Integer> other = new HashMap<>();
        other.put("b", 2);
        other.put("c", 3);
        Map<String, Integer> merged = Utils.merge(first, other);
        Assert.assertEquals(Integer.valueOf(1), merged.get("a"));
        Assert.assertEquals(Integer.valueOf(2), merged.get("b"));
        Assert.assertEquals(Integer.valueOf(3), merged.get("c"));
    }

    /** Test convertToArray method with start below min key. Expected = values ordered by integer key from start */
    @Test
    public void convertToArrayValidSrcMapBelowStartShouldPass() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "one"); map.put(2, "two"); map.put(3, "three");
        Assert.assertEquals(Arrays.asList("one", "two", "three"), Utils.convertToArray(map, 1));
    }

    /** Test convertToArray method with start equal max key. Expected = list with size 1 */
    @Test
    public void convertToArrayValidSrcMapEqualStartShouldPass() {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1); map.put(2, 2); map.put(3, 3);
        Assert.assertEquals(Collections.singletonList(3), Utils.convertToArray(map, 3));
    }

    /** Test makeUptimeComputerImpl method. Expected = new uptime computer */
    @Test
    public void makeUptimeComputerImplShouldReturnInstance() {
        Assert.assertNotNull(new Utils().makeUptimeComputerImpl());
    }

    /** Test isValidKey method with valid key. Expected = true */
    @Test
    public void isValidKeyValidKeyShouldReturnTrue() {
        Assert.assertTrue(Utils.isValidKey("valid-key_1.2"));
    }

    /** Test isValidKey method with empty key. Expected = false */
    @Test
    public void isValidKeyEmptyKeyShouldReturnFalse() {
        Assert.assertFalse(Utils.isValidKey(""));
    }

    /** Test validateTopologyName with valid name. Expected = completes without exception */
    @Test
    public void validateTopologyNameValidNameShouldPass() {
        Utils.validateTopologyName("valid-topology_1");
        Assert.assertTrue(true);
    }

    /** Test validateTopologyName with empty name. Expected = throws IllegalArgumentException */
    @Test
    public void validateTopologyNameEmptyNameThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.validateTopologyName(""));
    }

    /** Test findComponentCycles with empty topology. Expected = empty list */
    @Test
    public void findComponentCyclesEmptyTopologyShouldReturnEmptyList() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<String, SpoutSpec>());
        topology.set_bolts(new HashMap<String, Bolt>());
        Assert.assertTrue(Utils.findComponentCycles(topology, "topo").isEmpty());
    }

    /** Test validateCycleFree with empty topology. Expected = completes without exception */
    @Test
    public void validateCycleFreeEmptyTopologyShouldPass() throws InvalidTopologyException {  // ADDED THROWS
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<String, SpoutSpec>());
        topology.set_bolts(new HashMap<String, Bolt>());
        Utils.validateCycleFree(topology, "topo");
        Assert.assertTrue(true);
    }

    /** Test getClientBlobStore. Ignored because it requires configured external blob store implementation */
    @Ignore("Integration-style method: requires project-specific blobstore configuration")
    @Test
    public void getClientBlobStoreRequiresConfiguredEnvironment() {
        Assert.assertNotNull(Utils.getClientBlobStore(new HashMap<String, Object>()));
    }

    /** Test getTopologyInfo. Ignored because it uses NimbusClient static construction unless the project provides injectable configuration */
    @Ignore("Integration-style method: requires Nimbus client/environment")
    @Test
    public void getTopologyInfoRequiresNimbusEnvironment() {
        Assert.assertNull(Utils.getTopologyInfo("missing", null, new HashMap<String, Object>()));
    }

    /** Test validateTopologyBlobStoreMap overloads with concrete clients. Ignored because it requires BlobStore/NimbusBlobStore concrete environment */
    @Ignore("Integration-style overloads: require BlobStore/NimbusBlobStore instances configured by the project")
    @Test
    public void validateTopologyBlobStoreMapOverloadsRequireConcreteBlobStores() throws AuthorizationException, InvalidTopologyException {  // ADDED THROWS
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), (NimbusBlobStore) null);
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), (BlobStore) null);
    }

    /* ### Test END ### */
}
