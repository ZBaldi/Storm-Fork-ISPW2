package org.apache.storm.utils_4.another.llm;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.four.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.lang.IllegalStateException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class TotUtilsTest {

    // ### Test START ###

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Utils oldInstance;
    private String oldStormOptions;
    private String oldStormConfFile;
    private String oldJaap;
    private UncaughtExceptionHandler oldDefaultHandler;

    private static class TestableUtils extends Utils {
        private final String host;
        TestableUtils(String host) {
            this.host = host;
        }
        @Override
        protected String localHostnameImpl() {
            return host;
        }
        @Override
        protected String hostnameImpl() {
            return host;
        }
    }

    @Before
    public void setUp() {
        oldInstance = Utils.setInstance(new TestableUtils("test-host.local"));
        oldStormOptions = System.getProperty("storm.options");
        oldStormConfFile = System.getProperty("storm.conf.file");
        oldJaap = System.getProperty("java.security.auth.login.config");
        oldDefaultHandler = Thread.getDefaultUncaughtExceptionHandler();
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    @After
    public void tearDown() {
        Utils.setInstance(oldInstance);
        restoreProperty("storm.options", oldStormOptions);
        restoreProperty("storm.conf.file", oldStormConfFile);
        restoreProperty("java.security.auth.login.config", oldJaap);
        Thread.setDefaultUncaughtExceptionHandler(oldDefaultHandler);
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    /** Test setInstance method with valid Utils instance. Expected = returns previous instance */
    @Test
    public void setInstanceValidInstanceShouldReturnPreviousInstance() {
        Utils previous = Utils.setInstance(new TestableUtils("other-host"));
        assertNotNull(previous);
    }

    /** Test setClassLoaderForJavaDeSerialize and resetClassLoaderForJavaDeSerialize with valid classloader. Expected = serialize/deserialize still works */
    @Test
    public void setAndResetClassLoaderForJavaDeserializeShouldPass() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        byte[] serialized = Utils.javaSerialize("value");
        assertEquals("value", Utils.javaDeserialize(serialized, String.class));
        Utils.resetClassLoaderForJavaDeSerialize();
        assertEquals("value", Utils.javaDeserialize(serialized, String.class));
    }

    /** Test findResources method with existing resource name. Expected = returns a non-null list */
    @Test
    public void findResourcesExistingResourceShouldPass() {
        List<java.net.URL> resources = Utils.findResources("defaults.yaml");
        assertNotNull(resources);
    }

    /** Test findAndReadConfigFile method with missing name and mustExist false. Expected = empty map */
    @Test
    public void findAndReadConfigFileMissingOptionalShouldReturnEmptyMap() {
        assertEquals(Collections.emptyMap(), Utils.findAndReadConfigFile("missing-file-for-utils-test.yaml", false));
    }

    /** Test findAndReadConfigFile method with missing name and mustExist true. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileMissingRequiredShouldThrowRuntimeException() {
        assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("missing-file-for-utils-test.yaml", true));
    }

    /** Test findAndReadConfigFile overload with missing name. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileOverloadMissingRequiredShouldThrowRuntimeException() {
        assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("missing-file-for-utils-test.yaml"));
    }

    /** Test readDefaultConfig method. Expected = returns a map loaded from defaults.yaml */
    @Test
    public void readDefaultConfigShouldReturnMap() {
        assertNotNull(Utils.readDefaultConfig());
    }

    /** Test urlEncodeUtf8 method with valid string. Expected = same result as URLEncoder UTF-8 */
    @Test
    public void urlEncodeUtf8ValidStringShouldPass() throws Exception {
        String value = "name with spaces+accentà";
        assertEquals(URLEncoder.encode(value, StandardCharsets.UTF_8.name()), Utils.urlEncodeUtf8(value));
    }

    /** Test urlDecodeUtf8 method with valid encoded string. Expected = same result as URLDecoder UTF-8 */
    @Test
    public void urlDecodeUtf8ValidStringShouldPass() throws Exception {
        String encoded = Utils.urlEncodeUtf8("name with spaces+accentà");
        assertEquals(URLDecoder.decode(encoded, StandardCharsets.UTF_8.name()), Utils.urlDecodeUtf8(encoded));
    }

    /** Test readCommandLineOpts method with valid encoded options. Expected = parsed map */
    // @Test
    public void readCommandLineOptsValidOptionsShouldPass() {
        System.setProperty("storm.options", Utils.urlEncodeUtf8("a=1") + "," + Utils.urlEncodeUtf8("b=text"));
        Map<String, Object> opts = Utils.readCommandLineOpts();
        assertEquals(1L, opts.get("a"));
        assertEquals("text", opts.get("b"));
    }

    /** Test readCommandLineOpts method with no property. Expected = empty map */
    @Test
    public void readCommandLineOptsMissingPropertyShouldReturnEmptyMap() {
        System.clearProperty("storm.options");
        assertTrue(Utils.readCommandLineOpts().isEmpty());
    }

    /** Test readStormConfig method. Expected = returns merged configuration map */
    @Test
    public void readStormConfigShouldReturnMap() {
        System.clearProperty("storm.conf.file");
        assertNotNull(Utils.readStormConfig());
    }

    /** Test bitXorVals method with valid list. Expected = xor of all values */
    @Test
    public void bitXorValsValidListShouldPass() {
        assertEquals(0L ^ 1L ^ 2L ^ 3L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    /** Test bitXorVals method with empty list. Expected = 0 */
    @Test
    public void bitXorValsEmptyListShouldReturnZero() {
        assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
    }

    /** Test bitXor method with valid values. Expected = a xor b */
    @Test
    public void bitXorValidValuesShouldPass() {
        assertEquals(6L ^ 3L, Utils.bitXor(6L, 3L));
    }

    /** Test bitXor method with null operand. Expected = NullPointerException */
    @Test
    public void bitXorNullOperandShouldThrowNullPointerException() {
        assertThrows(NullPointerException.class, () -> Utils.bitXor(null, 1L));
    }

    /** Test isSystemId method with system id. Expected = true */
    @Test
    public void isSystemIdSystemValueShouldReturnTrue() {
        assertTrue(Utils.isSystemId("__system"));
    }

    /** Test isSystemId method with normal id. Expected = false */
    @Test
    public void isSystemIdNormalValueShouldReturnFalse() {
        assertFalse(Utils.isSystemId("component"));
    }

    /** Test asyncLoop full overload with startImmediately false. Expected = returns configured thread */
    @Test
    public void asyncLoopFullOverloadNotStartedShouldReturnThread() {
        Callable<Long> callable = () -> null;
        Utils.SmartThread thread = Utils.asyncLoop(callable, true, (t, e) -> { }, Thread.NORM_PRIORITY + 1, false, false, "unit");
        assertTrue(thread.isDaemon());
        assertEquals(Thread.NORM_PRIORITY + 1, thread.getPriority());
        assertTrue(thread.getName().contains("unit"));
        assertFalse(thread.isAlive());
    }

    /** Test asyncLoop named overload with callable returning null. Expected = thread starts and terminates */
    @Test
    public void asyncLoopNamedOverloadShouldReturnThread() throws Exception {
        Utils.SmartThread thread = Utils.asyncLoop((Callable<Long>) () -> null, "named", (t, e) -> fail("unexpected exception"));
        thread.join(2000);
        assertFalse(thread.isAlive());
    }

    /** Test asyncLoop simple overload with callable returning null. Expected = thread starts and terminates */
    @Test
    public void asyncLoopSimpleOverloadShouldReturnThread() throws Exception {
        Utils.SmartThread thread = Utils.asyncLoop((Callable<Long>) () -> null);
        thread.join(2000);
        assertFalse(thread.isAlive());
    }

    /** Test exceptionCauseIsInstanceOf with nested exception. Expected = true */
    @Test
    public void exceptionCauseIsInstanceOfNestedCauseShouldReturnTrue() {
        Throwable t = new RuntimeException(new IllegalArgumentException("bad"));
        assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, t));
    }

    /** Test exceptionCauseIsInstanceOf with unmatched exception. Expected = false */
    @Test
    public void exceptionCauseIsInstanceOfUnmatchedCauseShouldReturnFalse() {
        assertFalse(Utils.exceptionCauseIsInstanceOf(IllegalStateException.class, new RuntimeException("bad")));
    }

    /** Test unwrapTo with matching nested exception. Expected = matching cause */
    @Test
    public void unwrapToMatchingNestedCauseShouldPass() {
        IllegalArgumentException expected = new IllegalArgumentException("bad");
        assertSame(expected, Utils.unwrapTo(IllegalArgumentException.class, new RuntimeException(expected)));
    }

    /** Test unwrapTo with no matching exception. Expected = null */
    @Test
    public void unwrapToNoMatchingCauseShouldReturnNull() {
        assertNull(Utils.unwrapTo(IllegalStateException.class, new RuntimeException("bad")));
    }

    /** Test unwrapAndThrow with matching cause. Expected = throws matching exception */
    @Test
    public void unwrapAndThrowMatchingCauseShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException(new IllegalArgumentException("bad"))));
    }

    /** Test unwrapAndThrow with no matching cause. Expected = completes */
    @Test
    public void unwrapAndThrowNoMatchingCauseShouldPass() throws Exception {
        Utils.unwrapAndThrow(IllegalStateException.class, new RuntimeException("bad"));
    }

    /** Test wrapInRuntime with RuntimeException. Expected = same instance */
    @Test
    public void wrapInRuntimeRuntimeExceptionShouldReturnSameInstance() {
        RuntimeException expected = new RuntimeException("bad");
        assertSame(expected, Utils.wrapInRuntime(expected));
    }

    /** Test wrapInRuntime with checked exception. Expected = new RuntimeException wrapping cause */
    @Test
    public void wrapInRuntimeCheckedExceptionShouldWrapCause() {
        Exception expected = new Exception("bad");
        RuntimeException actual = Utils.wrapInRuntime(expected);
        assertSame(expected, actual.getCause());
    }

    /** Test secureRandomLong method. Expected = returns a long value */
    @Test
    public void secureRandomLongShouldReturnLong() {
        long value = Utils.secureRandomLong();
        assertEquals(value, value);
    }

    /** Test hostname method with mocked Utils instance. Expected = mocked hostname */
    @Test
    public void hostnameMockedInstanceShouldPass() throws Exception {
        assertEquals("test-host.local", Utils.hostname());
    }

    /** Test localHostname method with mocked Utils instance. Expected = mocked hostname */
    @Test
    public void localHostnameMockedInstanceShouldPass() throws Exception {
        assertEquals("test-host.local", Utils.localHostname());
    }

    /** Test uuid method. Expected = valid UUID string */
    @Test
    public void uuidShouldReturnValidUuid() {
        assertNotNull(UUID.fromString(Utils.uuid()));
    }

    /** Test javaSerialize/javaDeserialize methods with serializable object. Expected = original value restored */
    @Test
    public void javaSerializeDeserializeValidObjectShouldPass() {
        byte[] serialized = Utils.javaSerialize("payload");
        assertEquals("payload", Utils.javaDeserialize(serialized, String.class));
    }

    /** Test javaDeserialize with invalid bytes. Expected = RuntimeException */
    @Test
    public void javaDeserializeInvalidBytesShouldThrowRuntimeException() {
        assertThrows(RuntimeException.class, () -> Utils.javaDeserialize(new byte[] {1, 2, 3}, String.class));
    }

    /** Test get method with existing key. Expected = map value */
    @Test
    public void getExistingKeyShouldReturnValue() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        assertEquals(Integer.valueOf(1), Utils.get(map, "a", 9));
    }

    /** Test get method with missing key. Expected = default value */
    @Test
    public void getMissingKeyShouldReturnDefault() {
        assertEquals(Integer.valueOf(9), Utils.get(new HashMap<String, Integer>(), "a", 9));
    }

    /** Test zeroIfNaNOrInf method with NaN, infinity and finite value. Expected = zero for invalid numeric values */
    @Test
    public void zeroIfNaNOrInfBoundaryValuesShouldPass() {
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
        assertEquals(1.5, Utils.zeroIfNaNOrInf(1.5), 0.0);
    }

    /** Test join method with valid collection and separator. Expected = joined string */
    @Test
    public void joinValidCollectionShouldPass() {
        assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    /** Test join method with empty collection. Expected = empty string */
    @Test
    public void joinEmptyCollectionShouldReturnEmptyString() {
        assertEquals("", Utils.join(Collections.emptyList(), ","));
    }

    /** Test parseZkId method with valid id. Expected = Id with scheme and id */
    @Test
    public void parseZkIdValidIdShouldPass() {
        Id id = Utils.parseZkId("sasl:storm", "conf");
        assertEquals("sasl", id.getScheme());
        assertEquals("storm", id.getId());
    }

    /** Test parseZkId method with invalid id. Expected = IllegalArgumentException */
    @Test
    public void parseZkIdInvalidIdShouldThrowIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> Utils.parseZkId("storm", "conf"));
    }

    /** Test getSuperUserAcl method with valid config. Expected = ACL with all permissions */
    @Test
    public void getSuperUserAclValidConfShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:storm");
        ACL acl = Utils.getSuperUserAcl(conf);
        assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        assertEquals("sasl", acl.getId().getScheme());
    }

    /** Test getSuperUserAcl method with missing config. Expected = IllegalArgumentException */
    @Test
    public void getSuperUserAclMissingConfShouldThrowIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> Utils.getSuperUserAcl(new HashMap<String, Object>()));
    }

    /** Test getWorkerACL method with no topology authentication. Expected = null */
    @Test
    public void getWorkerAclNoAuthenticationShouldReturnNull() {
        assertNull(Utils.getWorkerACL(new HashMap<String, Object>()));
    }

    /** Test isZkAuthenticationConfiguredTopology method with scheme. Expected = true */
    @Test
    public void isZkAuthenticationConfiguredTopologyWithSchemeShouldReturnTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "sasl");
        assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
    }

    /** Test handleUncaughtException method with allowed exception. Expected = completes */
    @Test
    public void handleUncaughtExceptionAllowedExceptionShouldPass() {
        Set<Class<?>> allowed = new HashSet<>();
        allowed.add(IllegalArgumentException.class);
        Utils.handleUncaughtException(new IllegalArgumentException("allowed"), allowed, false);
    }

    /** Test handleUncaughtException method with disallowed exception. Expected = Error */
    @Test
    public void handleUncaughtExceptionDisallowedExceptionShouldThrowError() {
        assertThrows(Error.class, () -> Utils.handleUncaughtException(new IllegalArgumentException("bad"), Collections.<Class<?>>emptySet(), false));
    }

    /** Test handleUncaughtException overload with InterruptedException. Expected = completes because it is allowed */
    // @Test
    public void handleUncaughtExceptionOverloadInterruptedShouldPass() {
        Utils.handleUncaughtException(new InterruptedException("allowed"));
    }

    /** Test handleWorkerUncaughtException method with Hadoop UGI NPE. Expected = completes */
    @Test
    public void handleWorkerUncaughtExceptionAllowedWorkerNpeShouldPass() {
        NullPointerException npe = new NullPointerException("ugi");
        npe.setStackTrace(new StackTraceElement[] { new StackTraceElement("org.apache.hadoop.security.UserGroupInformation$1", "run", "UserGroupInformation.java", 1) });
        Utils.handleWorkerUncaughtException(npe);
    }

    /** Test sleepNoSimulation method with zero millis. Expected = completes */
    @Test
    public void sleepNoSimulationZeroShouldPass() {
        Utils.sleepNoSimulation(0L);
    }

    /** Test sleep method with zero millis. Expected = completes */
    @Test
    public void sleepZeroShouldPass() {
        Utils.sleep(0L);
    }

    /** Test makeUptimeComputer method. Expected = non-null uptime computer */
    @Test
    public void makeUptimeComputerShouldReturnComputer() {
        assertNotNull(Utils.makeUptimeComputer());
    }

    /** Test reverseMap(Map) method with duplicate values. Expected = grouped keys */
    @Test
    public void reverseMapValidMapShouldPass() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 2);
        Map<Integer, List<String>> reversed = Utils.reverseMap(map);
        assertEquals(2, reversed.get(1).size());
        assertEquals(Collections.singletonList("c"), reversed.get(2));
    }

    /** Test reverseMap(Map) method with null. Expected = empty map */
    @Test
    public void reverseMapNullMapShouldReturnEmptyMap() {
        assertTrue(Utils.reverseMap((Map<String, Integer>) null).isEmpty());
    }

    /** Test reverseMap(List) method with assoc-list input. Expected = grouped keys */
    @Test
    public void reverseMapListSeqValidShouldPass() {
        List<List<Object>> listSeq = new ArrayList<>();
        listSeq.add(Arrays.asList("a", 1));
        listSeq.add(Arrays.asList("b", 1));
        Map<Object, List<Object>> reversed = Utils.reverseMap(listSeq);
        assertEquals(Arrays.asList("a", "b"), reversed.get(1));
    }

    /** Test isOnWindows method. Expected = returns boolean without exception */
    @Test
    public void isOnWindowsShouldReturnBoolean() {
        boolean value = Utils.isOnWindows();
        assertEquals(value, Utils.isOnWindows());
    }

    /** Test checkFileExists method with existing file. Expected = true */
    @Test
    public void checkFileExistsExistingFileShouldPass() throws Exception {
        File file = temporaryFolder.newFile("existing.txt");
        assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
    }

    /** Test checkFileExists method with missing file. Expected = false */
    @Test
    public void checkFileExistsMissingFileShouldReturnFalse() {
        assertFalse(Utils.checkFileExists(new File(temporaryFolder.getRoot(), "missing.txt").getAbsolutePath()));
    }

    /** Test forceDelete method with existing file. Expected = deletes file */
    @Test
    public void forceDeleteExistingPathShouldPass() throws Exception {
        Utils.setInstance(oldInstance);
        File file = temporaryFolder.newFile("delete-me.txt");
        Utils.forceDelete(file.getAbsolutePath());
        assertFalse(file.exists());
        oldInstance = Utils.setInstance(new TestableUtils("test-host.local"));
    }

    /** Test serialize/deserialize methods with thrift object. Expected = original object restored */
    @Test
    public void serializeDeserializeValidObjectShouldPass() {
        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(7);
        byte[] serialized = Utils.serialize(expected);
        AccessControl actual = Utils.deserialize(serialized, AccessControl.class);
        assertEquals(expected, actual);
    }

    /** Test serializeToString/deserializeFromString methods with thrift object. Expected = original object restored */
    @Test
    public void serializeToStringDeserializeFromStringValidObjectShouldPass() {
        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(3);
        String serialized = Utils.serializeToString(expected);
        assertArrayEquals(Base64.getDecoder().decode(serialized), Utils.serialize(expected));
        assertEquals(expected, Utils.deserializeFromString(serialized, AccessControl.class));
    }

    /** Test deserializeFromString method with null string. Expected = NullPointerException */
    @Test
    public void deserializeFromStringNullStrShouldThrowNullPointerException() {
        assertThrows(NullPointerException.class, () -> Utils.deserializeFromString(null, AccessControl.class));
    }

    /** Test toByteArray method with buffer whose position is advanced. Expected = only remaining bytes */
    @Test
    public void toByteArrayRemainingBufferShouldPass() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        buffer.get();
        assertArrayEquals(new byte[] {2, 3, 4}, Utils.toByteArray(buffer));
    }

    /** Test mkSuicideFn method. Expected = returns a runnable without executing it */
    @Test
    public void mkSuicideFnShouldReturnRunnable() {
        assertNotNull(Utils.mkSuicideFn());
    }

    /** Test readAndLogStream method with valid stream. Expected = completes */
    @Test
    public void readAndLogStreamValidInputShouldPass() {
        Utils.readAndLogStream("prefix", new ByteArrayInputStream("one\ntwo".getBytes(StandardCharsets.UTF_8)));
    }

    /** Test getComponentCommon method with missing component. Expected = IllegalArgumentException */
    @Test
    public void getComponentCommonMissingComponentShouldThrowIllegalArgumentException() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        assertThrows(IllegalArgumentException.class, () -> Utils.getComponentCommon(topology, "missing"));
    }

    /** Test tuple method with mixed values. Expected = list preserving order */
    @Test
    public void tupleMixedValuesShouldPass() {
        assertEquals(Arrays.asList("a", 1, null), Utils.tuple("a", 1, null));
    }

    /** Test getRepeat method with duplicate values. Expected = repeated values only */
    @Test
    public void getRepeatDuplicateValuesShouldPass() {
        assertEquals(Arrays.asList("a", "b"), Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b")));
    }

    /** Test getGlobalStreamId method with null stream. Expected = default stream id */
    @Test
    public void getGlobalStreamIdNullStreamShouldUseDefault() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", null);
        assertEquals("component", id.get_componentId());
        assertEquals(Utils.DEFAULT_STREAM_ID, id.get_streamId());
    }

    /** Test getGlobalStreamId method with explicit stream. Expected = explicit stream id */
    @Test
    public void getGlobalStreamIdExplicitStreamShouldPass() {
        assertEquals("stream", Utils.getGlobalStreamId("component", "stream").get_streamId());
    }

//    /** Test getSetComponentObject method with java object. Expected = contained object */
//    @Test
//    public void getSetComponentObjectJavaObjectShouldPass() {
//        ComponentObject obj = ComponentObject.java_object("value");
//        assertEquals("value", Utils.getSetComponentObject(obj));
//    }

    /** Test toPositive method with negative, zero and positive values. Expected = non-negative deterministic value */
    @Test
    public void toPositiveBoundaryValuesShouldPass() {
        assertEquals(0, Utils.toPositive(0));
        assertEquals(1, Utils.toPositive(1));
        assertTrue(Utils.toPositive(-1) >= 0);
    }

    /** Test processPid method. Expected = numeric pid string */
    @Test
    public void processPidShouldReturnNumericString() {
        assertTrue(Utils.processPid().matches("[0-9]+"));
    }

    /** Test fromCompressedJsonConf/toCompressedJsonConf methods with valid map. Expected = round trip map */
    // @Test
    public void compressedJsonConfRoundTripShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "topology");
        map.put("workers", 2);
        Map<String, Object> actual = Utils.fromCompressedJsonConf(Utils.toCompressedJsonConf(map));
        assertEquals("topology", actual.get("name"));
        assertEquals(2L, actual.get("workers"));
    }

    /** Test fromCompressedJsonConf method with invalid bytes. Expected = RuntimeException */
    @Test
    public void fromCompressedJsonConfInvalidBytesShouldThrowRuntimeException() {
        assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(new byte[] {1, 2, 3}));
    }

    /** Test redactValue method with key present. Expected = new map with redacted value and original unchanged */
    @Test
    public void redactValueExistingKeyShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("secret", "abcd");
        Map<String, Object> redacted = Utils.redactValue(map, "secret");
        assertEquals("####", redacted.get("secret"));
        assertEquals("abcd", map.get("secret"));
        assertNotSame(map, redacted);
    }

    /** Test redactValue method with missing key. Expected = same map instance */
    @Test
    public void redactValueMissingKeyShouldReturnSameMap() {
        Map<String, Object> map = new HashMap<>();
        assertSame(map, Utils.redactValue(map, "missing"));
    }

    /** Test createDefaultUncaughtExceptionHandler method. Expected = non-null handler */
    @Test
    public void createDefaultUncaughtExceptionHandlerShouldReturnHandler() {
        assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
    }

    /** Test createWorkerUncaughtExceptionHandler method. Expected = non-null handler */
    @Test
    public void createWorkerUncaughtExceptionHandlerShouldReturnHandler() {
        assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
    }

    /** Test setupDefaultUncaughtExceptionHandler method. Expected = sets default handler */
    @Test
    public void setupDefaultUncaughtExceptionHandlerShouldSetHandler() {
        Utils.setupDefaultUncaughtExceptionHandler();
        assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    /** Test setupWorkerUncaughtExceptionHandler method. Expected = sets default handler */
    @Test
    public void setupWorkerUncaughtExceptionHandlerShouldSetHandler() {
        Utils.setupWorkerUncaughtExceptionHandler();
        assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    /** Test parseJvmHeapMemByChildOpts method with megabytes. Expected = parsed MB value */
    @Test
    public void parseJvmHeapMemByChildOptsMegabytesShouldPass() {
        assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 64.0));
    }

    /** Test parseJvmHeapMemByChildOpts method with null options. Expected = default value */
    @Test
    public void parseJvmHeapMemByChildOptsNullOptionsShouldReturnDefault() {
        assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(null, 64.0));
    }

    /** Test getClientBlobStore method with invalid configuration. Expected = RuntimeException or NullPointerException */
    @Test
    public void getClientBlobStoreInvalidConfShouldThrowException() {
        assertThrows(RuntimeException.class, () -> Utils.getClientBlobStore(new HashMap<String, Object>()));
    }

    /** Test isValidConf method with stable json values. Expected = true */
    @Test
    public void isValidConfValidJsonSerializableMapShouldReturnTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("key", "value");
        conf.put("int", 1);
        assertTrue(Utils.isValidConf(conf));
    }

    /** Test getTopologyId method with client returning summary. Expected = topology id */
    @Test
    public void getTopologyIdExistingTopologyShouldPass() throws Exception {
        Nimbus.Iface client = (Nimbus.Iface) java.lang.reflect.Proxy.newProxyInstance(
            Nimbus.Iface.class.getClassLoader(),
            new Class<?>[] { Nimbus.Iface.class },
            (proxy, method, args) -> {
                if (method.getName().equals("getTopologySummaryByName")) {
                    TopologySummary summary = new TopologySummary();
                    summary.set_id("tid");
                    return summary;
                }
                return null;
            });
        assertEquals("tid", Utils.getTopologyId("topo", client));
    }

    /** Test validateTopologyBlobStoreMap method with no blobstore map. Expected = completes */
    @Test
    public void validateTopologyBlobStoreMapEmptyMapShouldPass() throws Exception {
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), (org.apache.storm.blobstore.BlobStore) null);
    }

    /** Test threadDump method. Expected = contains current thread or dump text */
    @Test
    public void threadDumpShouldReturnText() {
        assertTrue(Utils.threadDump().length() > 0);
    }

    /** Test checkDirExists method with directory. Expected = true */
    @Test
    public void checkDirExistsExistingDirectoryShouldPass() {
        assertTrue(Utils.checkDirExists(temporaryFolder.getRoot().getAbsolutePath()));
    }

    /** Test checkDirExists method with file. Expected = false */
    @Test
    public void checkDirExistsFileShouldReturnFalse() throws Exception {
        assertFalse(Utils.checkDirExists(temporaryFolder.newFile("file.txt").getAbsolutePath()));
    }

    /** Test getConfiguredClass method with absent key. Expected = null */
    @Test
    public void getConfiguredClassAbsentKeyShouldReturnNull() {
        assertNull(Utils.getConfiguredClass(new HashMap<String, Object>(), "class.key"));
    }

    /** Test getConfiguredClass method with valid class name. Expected = instance */
    @Test
    public void getConfiguredClassValidClassShouldReturnInstance() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("class.key", "java.util.ArrayList");
        assertTrue(Utils.getConfiguredClass(conf, "class.key") instanceof ArrayList);
    }

    /** Test isZkAuthenticationConfiguredStormServer method with JAAS property. Expected = true */
    @Test
    public void isZkAuthenticationConfiguredStormServerWithPropertyShouldReturnTrue() {
        System.setProperty("java.security.auth.login.config", "jaas.conf");
        assertTrue(Utils.isZkAuthenticationConfiguredStormServer(new HashMap<String, Object>()));
    }

    /** Test isZkAuthenticationConfiguredStormServer method with no configuration. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredStormServerNoConfigurationShouldReturnFalse() {
        System.clearProperty("java.security.auth.login.config");
        assertFalse(Utils.isZkAuthenticationConfiguredStormServer(new HashMap<String, Object>()));
    }

    /** Test nullToZero method with null and valid value. Expected = zero for null */
    @Test
    public void nullToZeroBoundaryValuesShouldPass() {
        assertEquals(0.0, Utils.nullToZero(null), 0.0);
        assertEquals(2.5, Utils.nullToZero(2.5), 0.0);
    }

    /** Test OR method with null first value. Expected = second value */
    @Test
    public void orNullFirstShouldReturnSecond() {
        assertEquals("fallback", Utils.OR(null, "fallback"));
    }

    /** Test OR method with non-null first value. Expected = first value */
    @Test
    public void orNonNullFirstShouldReturnFirst() {
        assertEquals("first", Utils.OR("first", "fallback"));
    }

    /** Test integerDivided method with divisible values. Expected = base chunk only */
    @Test
    public void integerDividedDivisibleValuesShouldPass() {
        TreeMap<Integer, Integer> expected = new TreeMap<>();
        expected.put(2, 3);
        assertEquals(expected, Utils.integerDivided(6, 3));
    }

    /** Test integerDivided method with remainder. Expected = base and incremented chunk sizes */
    @Test
    public void integerDividedWithRemainderShouldPass() {
        TreeMap<Integer, Integer> expected = new TreeMap<>();
        expected.put(2, 2);
        expected.put(3, 1);
        assertEquals(expected, Utils.integerDivided(7, 3));
    }

    /** Test integerDivided method with zero pieces. Expected = ArithmeticException */
    @Test
    public void integerDividedZeroPiecesShouldThrowArithmeticException() {
        assertThrows(ArithmeticException.class, () -> Utils.integerDivided(1, 0));
    }

    /** Test partitionFixed method with valid collection. Expected = bounded chunks */
    @Test
    public void partitionFixedValidCollectionShouldPass() {
        List<List<Integer>> chunks = Utils.partitionFixed(3, Arrays.asList(1, 2, 3, 4, 5));
        assertEquals(3, chunks.size());
        assertEquals(Arrays.asList(1, 2), chunks.get(0));
    }

    /** Test partitionFixed method with zero max chunks. Expected = empty list */
    @Test
    public void partitionFixedZeroChunksShouldReturnEmptyList() {
        assertTrue(Utils.partitionFixed(0, Arrays.asList(1, 2)).isEmpty());
    }

    /** Test readYamlFile method with valid yaml. Expected = parsed map */
    @Test
    public void readYamlFileValidFileShouldPass() throws Exception {
        File yaml = temporaryFolder.newFile("conf.yaml");
        try (FileWriter writer = new FileWriter(yaml)) {
            writer.write("a: 1\n");
        }
        Map parsed = (Map) Utils.readYamlFile(yaml.getAbsolutePath());
        assertEquals(1, parsed.get("a"));
    }

    /** Test readYamlFile method with missing file. Expected = null */
    @Test
    public void readYamlFileMissingFileShouldReturnNull() {
        assertNull(Utils.readYamlFile(new File(temporaryFolder.getRoot(), "missing.yaml").getAbsolutePath()));
    }

    /** Test getAvailablePort preferred overload with zero. Expected = positive available port */
    @Test
    public void getAvailablePortZeroPreferredShouldPass() {
        assertTrue(Utils.getAvailablePort(0) > 0);
    }

    /** Test getAvailablePort no-arg overload. Expected = positive available port */
    @Test
    public void getAvailablePortNoArgShouldPass() {
        assertTrue(Utils.getAvailablePort() > 0);
    }

    /** Test findOne(Collection) method with matching predicate. Expected = first matching value */
    @Test
    public void findOneCollectionMatchingValueShouldPass() {
        String found = Utils.findOne((IPredicate<String>) value -> value.startsWith("b"), Arrays.asList("a", "bb", "bc"));
        assertEquals("bb", found);
    }

    /** Test findOne(Collection) method with null collection. Expected = null */
    @Test
    public void findOneCollectionNullShouldReturnNull() {
        assertNull(Utils.findOne((IPredicate<String>) value -> true, (Collection<String>) null));
    }

    /** Test findOne(Map) method with null map. Expected = null */
    @Test
    public void findOneMapNullShouldReturnNull() {
        assertNull(Utils.findOne((IPredicate<Object>) value -> true, (Map<String, Object>) null));
    }

    /** Test parseJson method with valid JSON. Expected = parsed map */
    // @Test
    public void parseJsonValidJsonShouldPass() {
        Map<String, Object> parsed = Utils.parseJson("{\"a\":1}");
        assertEquals(1L, parsed.get("a"));
    }

    /** Test parseJson method with null JSON. Expected = empty map */
    @Test
    public void parseJsonNullJsonShouldReturnEmptyMap() {
        assertTrue(Utils.parseJson(null).isEmpty());
    }

    /** Test parseJson method with invalid JSON. Expected = RuntimeException */
    @Test
    public void parseJsonInvalidJsonShouldThrowRuntimeException() {
        assertThrows(RuntimeException.class, () -> Utils.parseJson("{invalid"));
    }

    /** Test memoizedLocalHostname method with mocked Utils instance. Expected = hostname */
    @Test
    public void memoizedLocalHostnameMockedInstanceShouldPass() throws Exception {
        assertNotNull(Utils.memoizedLocalHostname());
    }

    /** Test addVersions method with empty topology. Expected = same topology returned and JDK version set */
    @Test
    public void addVersionsEmptyTopologyShouldPass() {
        StormTopology topology = new StormTopology();
        assertSame(topology, Utils.addVersions(topology));
        assertEquals(System.getProperty("java.version"), topology.get_jdk_version());
    }

    /** Test getConfiguredClasspathVersions method with empty config. Expected = contains current version */
    @Test
    public void getConfiguredClasspathVersionsEmptyConfShouldPass() {
        NavigableMap<SimpleVersion, List<String>> map = Utils.getConfiguredClasspathVersions(new HashMap<String, Object>(), Arrays.asList("cp"));
        assertTrue(map.containsValue(Arrays.asList("cp")));
    }

    /** Test getAlternativeVersionsMap method with empty config. Expected = empty map */
    @Test
    public void getAlternativeVersionsMapEmptyConfShouldReturnEmptyMap() {
        assertTrue(Utils.getAlternativeVersionsMap(new HashMap<String, Object>()).isEmpty());
    }

    /** Test getConfiguredWorkerMainVersions method with empty config. Expected = non-empty map with default worker */
    @Test
    public void getConfiguredWorkerMainVersionsEmptyConfShouldPass() {
        assertTrue(Utils.getConfiguredWorkerMainVersions(new HashMap<String, Object>()).containsValue("org.apache.storm.daemon.worker.Worker"));
    }

    /** Test getConfiguredWorkerLogWriterVersions method with empty config. Expected = non-empty map with default log writer */
    @Test
    public void getConfiguredWorkerLogWriterVersionsEmptyConfShouldPass() {
        assertTrue(Utils.getConfiguredWorkerLogWriterVersions(new HashMap<String, Object>()).containsValue("org.apache.storm.LogWriter"));
    }

    /** Test getCompatibleVersion method with compatible ceiling version. Expected = value from map */
    // @Test
    public void getCompatibleVersionCompatibleVersionShouldPass() {
        NavigableMap<SimpleVersion, String> versions = new TreeMap<>();
        versions.put(new SimpleVersion("1.0.0"), "one");
        versions.put(new SimpleVersion("1.1.0"), "one-one");
        assertEquals("one-one", Utils.getCompatibleVersion(versions, new SimpleVersion("1.0.5"), "test", "default"));
    }

    /** Test getCompatibleVersion method with incompatible version. Expected = default value */
    @Test
    public void getCompatibleVersionIncompatibleVersionShouldReturnDefault() {
        NavigableMap<SimpleVersion, String> versions = new TreeMap<>();
        versions.put(new SimpleVersion("2.0.0"), "two");
        assertEquals("default", Utils.getCompatibleVersion(versions, new SimpleVersion("1.0.0"), "test", "default"));
    }

    /** Test getConfigFromClasspath method with null classpath. Expected = original conf */
    @Test
    public void getConfigFromClasspathNullClasspathShouldReturnConf() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        assertSame(conf, Utils.getConfigFromClasspath(null, conf));
    }

    /** Test isLocalhostAddress method with localhost address. Expected = true */
    @Test
    public void isLocalhostAddressLocalhostShouldReturnTrue() {
        assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
    }

    /** Test isLocalhostAddress method with remote address. Expected = false */
    @Test
    public void isLocalhostAddressRemoteShouldReturnFalse() {
        assertFalse(Utils.isLocalhostAddress("8.8.8.8"));
    }

    /** Test merge method with two maps. Expected = second map overrides first */
    @Test
    public void mergeTwoMapsShouldPass() {
        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        first.put("b", 1);
        Map<String, Integer> other = new HashMap<>();
        other.put("b", 2);
        Map<String, Integer> merged = Utils.merge(first, other);
        assertEquals(Integer.valueOf(1), merged.get("a"));
        assertEquals(Integer.valueOf(2), merged.get("b"));
    }

    /** Test merge method with null second map. Expected = copy of first map */
    @Test
    public void mergeNullOtherShouldReturnCopyOfFirst() {
        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        Map<String, Integer> merged = Utils.merge(first, null);
        assertEquals(first, merged);
        assertNotSame(first, merged);
    }

    /** Test convertToArray method with valid source map and start index equal to max key. Expected = list with size 1 */
    @Test
    public void convertToArrayValidSrcMapEqualStartShouldPass() {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        assertEquals(Arrays.asList(3), Utils.convertToArray(map, 3));
    }

    /** Test convertToArray method with missing key after start. Expected = null placeholder */
    @Test
    public void convertToArrayMissingIntermediateKeyShouldPass() {
        Map<Integer, String> map = new HashMap<>();
        map.put(2, "b");
        map.put(4, "d");
        assertEquals(Arrays.asList("b", null, "d"), Utils.convertToArray(map, 2));
    }

    /** Test makeUptimeComputerImpl method. Expected = non-null uptime computer */
    @Test
    public void makeUptimeComputerImplShouldReturnComputer() {
        assertNotNull(new Utils().makeUptimeComputerImpl());
    }

    /** Test isValidKey method with valid key. Expected = true */
    @Test
    public void isValidKeyValidKeyShouldReturnTrue() {
        assertTrue(Utils.isValidKey("valid-key_1"));
    }

    /** Test isValidKey method with null, empty, dot and dotdot. Expected = false */
    @Test
    public void isValidKeyInvalidKeysShouldReturnFalse() {
        assertFalse(Utils.isValidKey(null));
        assertFalse(Utils.isValidKey(""));
        assertFalse(Utils.isValidKey("."));
        assertFalse(Utils.isValidKey(".."));
    }

    /** Test validateTopologyName method with valid name. Expected = completes */
    @Test
    public void validateTopologyNameValidNameShouldPass() {
        Utils.validateTopologyName("topology-1_ok");
    }

    /** Test validateTopologyName method with null name. Expected = IllegalArgumentException */
    @Test
    public void validateTopologyNameNullNameShouldThrowIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> Utils.validateTopologyName(null));
    }

    /** Test findComponentCycles method with topology with no spouts and no bolts. Expected = empty cycles */
    @Test
    public void findComponentCyclesEmptyTopologyShouldReturnEmptyList() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        assertTrue(Utils.findComponentCycles(topology, "topo").isEmpty());
    }

    /** Test validateCycleFree method with empty topology. Expected = completes */
    @Test
    public void validateCycleFreeEmptyTopologyShouldPass() throws InvalidTopologyException {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        Utils.validateCycleFree(topology, "topo");
    }

    // ### Test END ###
}
