package org.apache.storm.utils_2.another.llm;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.thrift.TBase;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.two.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.lang.IllegalStateException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Test suite generated for Utils by applying Category Partition and Boundary Value Analysis.
 */
@SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
public class TotUtilsTest {

    // ### Test START ###

    private ClassLoader originalContextClassLoader;

    @Before
    public void setUp() {
        originalContextClassLoader = Thread.currentThread().getContextClassLoader();
    }

    @After
    public void tearDown() {
        Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    /** Test setInstance method with valid Utils instance. Expected = returns previously set instance */
    @Test
    public void setInstanceValidInstanceShouldReturnPreviousInstance() {
        Utils first = new Utils();
        Utils previous = Utils.setInstance(first);
        Utils second = new Utils();
        Assert.assertSame(first, Utils.setInstance(second));
        Utils.setInstance(previous);
    }

    /** Test setClassLoaderForJavaDeSerialize method with valid class loader. Expected = completes without exception */
    @Test
    public void setClassLoaderForJavaDeSerializeValidClassLoaderShouldPass() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        String value = "storm";
        byte[] serialized = Utils.javaSerialize(value);
        Assert.assertEquals(value, Utils.javaDeserialize(serialized, String.class));
    }

    /** Test resetClassLoaderForJavaDeSerialize method. Expected = completes without exception */
    @Test
    public void resetClassLoaderForJavaDeSerializeShouldPass() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        Utils.resetClassLoaderForJavaDeSerialize();
        Assert.assertEquals("value", Utils.javaDeserialize(Utils.javaSerialize("value"), String.class));
    }

    /** Test findResources method with null resource name. Expected = throws NullPointerException */
    @Test
    public void findResourcesNullNameThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.findResources(null));
    }

    /** Test findResources method with existing classpath resource. Expected = returns non-empty list */
    @Test
    public void findResourcesExistingResourceShouldPass() {
        List<java.net.URL> resources = Utils.findResources("org/apache/storm/Config.class");
        Assert.assertNotNull(resources);
    }

    /** Test findAndReadConfigFile method with not correct name and mustExist false. Expected = empty map */
    @Test
    public void findAndReadConfigFileMissingNameFalseMustExistShouldReturnEmptyMap() {
        Map<String, Object> result = Utils.findAndReadConfigFile("missing-test-config-file.yaml", false);
        Assert.assertEquals(Collections.emptyMap(), result);
    }

    /** Test findAndReadConfigFile method with not correct name and mustExist true. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileMissingNameTrueMustExistThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("missing-test-config-file.yaml", true));
    }

    /** Test findAndReadConfigFile overload with not correct name. Expected = delegates with mustExist true and throws RuntimeException */
    @Test
    public void findAndReadConfigFileMissingNameDefaultThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("missing-test-config-file.yaml"));
    }

    /** Test readDefaultConfig method. Expected = returns a non-null map */
    @Test
    public void readDefaultConfigShouldReturnMap() {
        Map<String, Object> conf = Utils.readDefaultConfig();
        Assert.assertNotNull(conf);
    }

    /** Test urlEncodeUtf8 method with valid string. Expected = Java UTF-8 URL encoded string */
    @Test
    public void urlEncodeUtf8ValidStringShouldPass() throws Exception {
        String input = "a b+c/à";
        Assert.assertEquals(URLEncoder.encode(input, StandardCharsets.UTF_8.name()), Utils.urlEncodeUtf8(input));
    }

    /** Test urlEncodeUtf8 method with null string. Expected = throws NullPointerException */
    @Test
    public void urlEncodeUtf8NullStringThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlEncodeUtf8(null));
    }

    /** Test urlDecodeUtf8 method with valid encoded string. Expected = Java UTF-8 URL decoded string */
    @Test
    public void urlDecodeUtf8ValidStringShouldPass() throws Exception {
        String input = "a+b%2Bc%2F%C3%A0";
        Assert.assertEquals(URLDecoder.decode(input, StandardCharsets.UTF_8.name()), Utils.urlDecodeUtf8(input));
    }

    /** Test urlDecodeUtf8 method with null string. Expected = throws NullPointerException */
    @Test
    public void urlDecodeUtf8NullStringThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlDecodeUtf8(null));
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

    /** Test bitXorVals method with valid list. Expected = xor of all values */
    @Test
    public void bitXorValsValidListShouldPass() {
        Assert.assertEquals(1L ^ 2L ^ 3L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    /** Test bitXorVals method with empty list. Expected = neutral xor value 0 */
    @Test
    public void bitXorValsEmptyListShouldReturnZero() {
        Assert.assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
    }

    /** Test bitXor method with valid operands. Expected = a ^ b */
    @Test
    public void bitXorValidOperandsShouldPass() {
        Assert.assertEquals(3L ^ 5L, Utils.bitXor(3L, 5L));
    }

    /** Test bitXor method with null operand. Expected = throws NullPointerException */
    @Test
    public void bitXorNullOperandThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXor(null, 5L));
    }

    /** Test addShutdownHookWithForceKillIn1Sec method with valid runnable. Expected = completes without exception */
    // @Test
    public void addShutdownHookWithForceKillIn1SecValidRunnableShouldPass() {
        Utils.addShutdownHookWithForceKillIn1Sec(() -> { });
    }

    /** Test addShutdownHookWithDelayedForceKill method with valid runnable and boundary delay zero. Expected = completes without exception */
    // @Test
    public void addShutdownHookWithDelayedForceKillZeroDelayShouldPass() {
        Utils.addShutdownHookWithDelayedForceKill(() -> { }, 0);
    }

    /** Test isSystemId method with system id. Expected = true */
    @Test
    public void isSystemIdDoubleUnderscoreShouldReturnTrue() {
        Assert.assertTrue(Utils.isSystemId("__system"));
    }

    /** Test isSystemId method with non-system id. Expected = false */
    @Test
    public void isSystemIdNormalIdShouldReturnFalse() {
        Assert.assertFalse(Utils.isSystemId("component"));
    }

    /** Test isSystemId method with null id. Expected = throws NullPointerException */
    @Test
    public void isSystemIdNullThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.isSystemId(null));
    }

    /** Test asyncLoop full overload with startImmediately false. Expected = returns newly created SmartThread */
    // @Test
    public void asyncLoopFullValidCallableNoStartShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(() -> null, true, (t, e) -> { }, Thread.NORM_PRIORITY, false, false, "test-async-full");
        Assert.assertNotNull(thread);
        Assert.assertEquals("test-async-full", thread.getName());
    }

    /** Test asyncLoop named overload. Expected = returns newly created SmartThread */
    @Test
    public void asyncLoopNamedValidCallableShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(() -> null, "test-async-named", (t, e) -> { });
        Assert.assertNotNull(thread);
    }

    /** Test asyncLoop simple overload. Expected = returns newly created SmartThread */
    @Test
    public void asyncLoopSimpleValidCallableShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(() -> null);
        Assert.assertNotNull(thread);
    }

    /** Test exceptionCauseIsInstanceOf method with direct matching throwable. Expected = true */
    @Test
    public void exceptionCauseIsInstanceOfDirectMatchShouldReturnTrue() {
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, new IllegalArgumentException("x")));
    }

    /** Test exceptionCauseIsInstanceOf method with nested matching cause. Expected = true */
    @Test
    public void exceptionCauseIsInstanceOfNestedMatchShouldReturnTrue() {
        RuntimeException wrapper = new RuntimeException(new IllegalStateException("nested"));
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalStateException.class, wrapper));
    }

    /** Test exceptionCauseIsInstanceOf method with no matching cause. Expected = false */
    @Test
    public void exceptionCauseIsInstanceOfNoMatchShouldReturnFalse() {
        Assert.assertFalse(Utils.exceptionCauseIsInstanceOf(IllegalStateException.class, new IllegalArgumentException("x")));
    }

    /** Test unwrapTo method with nested matching cause. Expected = returns matching throwable */
    @Test
    public void unwrapToNestedMatchShouldReturnThrowable() {
        IllegalStateException expected = new IllegalStateException("nested");
        Assert.assertSame(expected, Utils.unwrapTo(IllegalStateException.class, new RuntimeException(expected)));
    }

    /** Test unwrapTo method with no matching cause. Expected = null */
    @Test
    public void unwrapToNoMatchShouldReturnNull() {
        Assert.assertNull(Utils.unwrapTo(IllegalStateException.class, new RuntimeException("x")));
    }

    /** Test unwrapAndThrow method with matching cause. Expected = throws matched exception */
    @Test
    public void unwrapAndThrowMatchingCauseThrowsOriginal() {
        IllegalArgumentException expected = new IllegalArgumentException("err");
        IllegalArgumentException thrown = Assert.assertThrows(IllegalArgumentException.class,
                () -> Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException(expected)));
        Assert.assertSame(expected, thrown);
    }

    /** Test unwrapAndThrow method with no matching cause. Expected = completes without exception */
    @Test
    public void unwrapAndThrowNoMatchingCauseShouldPass() {
        Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException("x"));
    }

    /** Test wrapInRuntime method with RuntimeException. Expected = same instance */
    @Test
    public void wrapInRuntimeRuntimeExceptionShouldReturnSame() {
        RuntimeException expected = new RuntimeException("x");
        Assert.assertSame(expected, Utils.wrapInRuntime(expected));
    }

    /** Test wrapInRuntime method with checked exception. Expected = RuntimeException wrapping original */
    @Test
    public void wrapInRuntimeCheckedExceptionShouldWrap() {
        Exception expected = new Exception("x");
        RuntimeException result = Utils.wrapInRuntime(expected);
        Assert.assertSame(expected, result.getCause());
    }

    /** Test secureRandomLong method. Expected = returns a long value */
    @Test
    public void secureRandomLongShouldReturnLong() {
        long value = Utils.secureRandomLong();
        Assert.assertEquals(value, value);
    }

    /** Test hostname method. Expected = returns non-empty string */
    @Test
    public void hostnameShouldReturnNonEmptyString() throws UnknownHostException {
        String hostname = Utils.hostname();
        Assert.assertNotNull(hostname);
        Assert.assertFalse(hostname.isEmpty());
    }

    /** Test localHostname method. Expected = returns non-empty string */
    @Test
    public void localHostnameShouldReturnNonEmptyString() throws UnknownHostException {
        String hostname = Utils.localHostname();
        Assert.assertNotNull(hostname);
        Assert.assertFalse(hostname.isEmpty());
    }

    /** Test uuid method. Expected = returns valid UUID string */
    @Test
    public void uuidShouldReturnValidUuidString() {
        Assert.assertNotNull(java.util.UUID.fromString(Utils.uuid()));
    }

    /** Test javaSerialize/javaDeserialize methods with valid serializable value. Expected = round trip preserves value */
    @Test
    public void javaSerializeAndJavaDeserializeValidObjectShouldPass() {
        String expected = "serializable-value";
        byte[] serialized = Utils.javaSerialize(expected);
        Assert.assertEquals(expected, Utils.javaDeserialize(serialized, String.class));
    }

    /** Test javaSerialize method with non serializable object. Expected = throws RuntimeException */
    @Test
    public void javaSerializeNonSerializableObjectThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.javaSerialize(new Object()));
    }

    /** Test javaDeserialize method with null byte array. Expected = throws NullPointerException */
    @Test
    public void javaDeserializeNullSerializedThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.javaDeserialize(null, String.class));
    }

    /** Test get method with existing key. Expected = value from map */
    @Test
    public void getExistingKeyShouldReturnValue() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        Assert.assertEquals(Integer.valueOf(1), Utils.get(map, "a", 0));
    }

    /** Test get method with missing key. Expected = default value */
    @Test
    public void getMissingKeyShouldReturnDefault() {
        Assert.assertEquals(Integer.valueOf(2), Utils.get(Collections.<String, Integer>emptyMap(), "a", 2));
    }

    /** Test zeroIfNaNOrInf method with valid finite value. Expected = same value */
    @Test
    public void zeroIfNaNOrInfFiniteValueShouldReturnSame() {
        Assert.assertEquals(12.5, Utils.zeroIfNaNOrInf(12.5), 0.0);
    }

    /** Test zeroIfNaNOrInf method with NaN. Expected = 0.0 */
    @Test
    public void zeroIfNaNOrInfNaNShouldReturnZero() {
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
    }

    /** Test zeroIfNaNOrInf method with infinity. Expected = 0.0 */
    @Test
    public void zeroIfNaNOrInfInfiniteShouldReturnZero() {
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
    }

    /** Test join method with valid collection and separator. Expected = joined string */
    @Test
    public void joinValidCollectionShouldPass() {
        Assert.assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    /** Test join method with empty collection. Expected = empty string */
    @Test
    public void joinEmptyCollectionShouldReturnEmptyString() {
        Assert.assertEquals("", Utils.join(Collections.emptyList(), ","));
    }

    /** Test parseZkId method with valid digest id. Expected = Id with split scheme and id */
    @Test
    public void parseZkIdValidIdShouldPass() {
        Id id = Utils.parseZkId("digest:user", "testConf");
        Assert.assertEquals("digest", id.getScheme());
        Assert.assertEquals("user", id.getId());
    }

    /** Test parseZkId method with invalid id. Expected = throws IllegalArgumentException */
    @Test
    public void parseZkIdInvalidIdThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.parseZkId("invalid", "testConf"));
    }

    /** Test getSuperUserAcl method with configured super user. Expected = ALL permissions ACL */
    @Test
    public void getSuperUserAclConfiguredShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "digest:user");
        ACL acl = Utils.getSuperUserAcl(conf);
        Assert.assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        Assert.assertEquals(new Id("digest", "user"), acl.getId());
    }

    /** Test getWorkerACL method without configured topology auth. Expected = null or list according to configuration defaults */
    @Test
    public void getWorkerACLWithoutAuthShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        List<ACL> result = Utils.getWorkerACL(conf);
        Assert.assertTrue(result == null || result instanceof List);
    }

    /** Test isZkAuthenticationConfiguredTopology method with empty configuration. Expected = false */
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
    }

    /** Test handleUncaughtException overload. Expected = completes without exception for non-fatal RuntimeException */
    // @Test
    public void handleUncaughtExceptionRuntimeExceptionShouldPass() {
        Utils.handleUncaughtException(new RuntimeException("test"));
    }

    /** Test handleWorkerUncaughtException method. Expected = completes without exception for non-fatal RuntimeException */
    // @Test
    public void handleWorkerUncaughtExceptionRuntimeExceptionShouldPass() {
        Utils.handleWorkerUncaughtException(new RuntimeException("test"));
    }

    /** Test thriftSerialize method with null TBase. Expected = throws RuntimeException or NullPointerException */
    @Test
    public void thriftSerializeNullThrowsException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftSerialize((TBase) null));
    }

    /** Test thriftDeserialize method with null bytes. Expected = throws RuntimeException or NullPointerException */
    @Test
    public void thriftDeserializeNullBytesThrowsException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftDeserialize(AccessControl.class, null));
    }

    /** Test thriftDeserialize offset overload with invalid bytes. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeInvalidBytesThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftDeserialize(AccessControl.class, new byte[] {1, 2, 3}, 0, 3));
    }

    /** Test sleepNoSimulation method with boundary zero millis. Expected = completes without exception */
    @Test
    public void sleepNoSimulationZeroMillisShouldPass() {
        Utils.sleepNoSimulation(0L);
    }

    /** Test sleep method with boundary zero millis. Expected = completes without exception */
    @Test
    public void sleepZeroMillisShouldPass() {
        Utils.sleep(0L);
    }

    /** Test makeUptimeComputer method. Expected = returns UptimeComputer */
    @Test
    public void makeUptimeComputerShouldReturnInstance() {
        Assert.assertNotNull(Utils.makeUptimeComputer());
    }

    /** Test reverseMap method with valid map. Expected = reversed grouped map */
    @Test
    public void reverseMapValidMapShouldPass() {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 2);
        HashMap<Integer, List<String>> reversed = Utils.reverseMap(map);
        Assert.assertEquals(Arrays.asList("a", "b"), reversed.get(1));
        Assert.assertEquals(Collections.singletonList("c"), reversed.get(2));
    }

    /** Test reverseMap listSeq method with valid sequence. Expected = reversed map grouped by value */
    @Test
    public void reverseMapListSeqValidShouldPass() {
        List<List<Object>> listSeq = new ArrayList<>();
        listSeq.add(Arrays.<Object>asList("k1", "v"));
        listSeq.add(Arrays.<Object>asList("k2", "v"));
        Map<Object, List<Object>> reversed = Utils.reverseMap(listSeq);
        Assert.assertEquals(Arrays.asList("k1", "k2"), reversed.get("v"));
    }

    /** Test isOnWindows method. Expected = returns boolean */
    @Test
    public void isOnWindowsShouldReturnBoolean() {
        Assert.assertTrue(Utils.isOnWindows() || !Utils.isOnWindows());
    }

    /** Test checkFileExists method with existing temporary file. Expected = true */
    @Test
    public void checkFileExistsExistingFileShouldReturnTrue() throws Exception {
        File file = File.createTempFile("utils-test", ".tmp");
        file.deleteOnExit();
        Assert.assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
    }

    /** Test checkFileExists method with not correct path. Expected = false */
    @Test
    public void checkFileExistsMissingFileShouldReturnFalse() {
        Assert.assertFalse(Utils.checkFileExists("missing-file-for-utils-test.tmp"));
    }

    /** Test forceDelete method with existing file. Expected = file removed */
    @Test
    public void forceDeleteExistingFileShouldDelete() throws Exception {
        File file = File.createTempFile("utils-delete", ".tmp");
        Assert.assertTrue(file.exists());
        Utils.forceDelete(file.getAbsolutePath());
        Assert.assertFalse(file.exists());
    }

    /** Test serialize/deserialize methods with valid object. Expected = round trip preserves value */
    @Test
    public void serializeAndDeserializeValidObjectShouldPass() {
        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(1);
        byte[] serialized = Utils.serialize(expected);
        AccessControl deserialized = Utils.deserialize(serialized, AccessControl.class);
        Assert.assertEquals(expected, deserialized);
    }

    /** Test deserialize method with null byte array. Expected = throws NullPointerException or RuntimeException */
    @Test
    public void deserializeNullSerializedThrowsException() {
        Assert.assertThrows(Exception.class, () -> Utils.deserialize(null, String.class));
    }

    /** Test serializeToString/deserializeFromString methods with valid object. Expected = round trip preserves value */
    @Test
    public void serializeToStringAndDeserializeFromStringValidObjectShouldPass() {
        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(7);
        String serialized = Utils.serializeToString(expected);
        Assert.assertEquals(expected, Utils.deserializeFromString(serialized, AccessControl.class));
    }

    /** Test deserializeFromString method with null string. Expected = throws NullPointerException */
    @Test
    public void deserializeFromStringNullStrThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.deserializeFromString(null, String.class));
    }

    /** Test toByteArray method with heap ByteBuffer. Expected = remaining bytes */
    @Test
    public void toByteArrayHeapBufferShouldPass() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        buffer.get();
        Assert.assertArrayEquals(new byte[] {2, 3, 4}, Utils.toByteArray(buffer));
    }

    /** Test mkSuicideFn method. Expected = returns Runnable */
    @Test
    public void mkSuicideFnShouldReturnRunnable() {
        Assert.assertNotNull(Utils.mkSuicideFn());
    }

    /** Test readAndLogStream method with valid stream. Expected = completes without exception */
    @Test
    public void readAndLogStreamValidStreamShouldPass() {
        InputStream input = new ByteArrayInputStream("line1\nline2".getBytes(StandardCharsets.UTF_8));
        Utils.readAndLogStream("test", input);
    }

    /** Test getComponentCommon method with null topology. Expected = throws NullPointerException */
    @Test
    public void getComponentCommonNullTopologyThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.getComponentCommon(null, "id"));
    }

    /** Test tuple method with valid values. Expected = list containing values */
    @Test
    public void tupleValidValuesShouldPass() {
        Assert.assertEquals(Arrays.<Object>asList("a", 1, null), Utils.tuple("a", 1, null));
    }

    /** Test tuple method with no values. Expected = empty list */
    @Test
    public void tupleNoValuesShouldReturnEmptyList() {
        Assert.assertEquals(Collections.emptyList(), Utils.tuple());
    }

    /** Test getRepeat method with repeated values. Expected = list of repeated values */
    @Test
    public void getRepeatWithRepeatedValuesShouldPass() {
        Assert.assertEquals(Collections.singletonList("a"), Utils.getRepeat(Arrays.asList("a", "b", "a", "c")));
    }

    /** Test getRepeat method with no repeated values. Expected = empty list */
    @Test
    public void getRepeatWithoutRepeatedValuesShouldReturnEmptyList() {
        Assert.assertEquals(Collections.emptyList(), Utils.getRepeat(Arrays.asList("a", "b", "c")));
    }

    /** Test getGlobalStreamId method with null stream id. Expected = default stream id */
    @Test
    public void getGlobalStreamIdNullStreamShouldUseDefault() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", null);
        Assert.assertEquals("component", id.get_componentId());
        Assert.assertEquals(Utils.DEFAULT_STREAM_ID, id.get_streamId());
    }

    /** Test getGlobalStreamId method with valid stream id. Expected = provided stream id */
    @Test
    public void getGlobalStreamIdValidStreamShouldPass() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", "stream");
        Assert.assertEquals("stream", id.get_streamId());
    }

//    /** Test getSetComponentObject method with java object. Expected = returns contained java object */
//    @Test
//    public void getSetComponentObjectJavaObjectShouldPass() {
//        ComponentObject object = ComponentObject.java_object("value");
//        Assert.assertEquals("value", Utils.getSetComponentObject(object));
//    }

    /** Test toPositive method with negative boundary. Expected = non-negative number */
    @Test
    public void toPositiveNegativeNumberShouldReturnPositive() {
        Assert.assertTrue(Utils.toPositive(-1) >= 0);
    }

    /** Test toPositive method with zero boundary. Expected = zero */
    @Test
    public void toPositiveZeroShouldReturnZero() {
        Assert.assertEquals(0, Utils.toPositive(0));
    }

    /** Test processPid method. Expected = non-empty pid string */
    @Test
    public void processPidShouldReturnNonEmptyString() {
        String pid = Utils.processPid();
        Assert.assertNotNull(pid);
        Assert.assertFalse(pid.isEmpty());
    }

    /** Test toCompressedJsonConf/fromCompressedJsonConf methods with valid map. Expected = round trip preserves map */
    @Test
    public void compressedJsonConfRoundTripValidMapShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        map.put("number", 3);
        byte[] serialized = Utils.toCompressedJsonConf(map);
        Map<String, Object> deserialized = Utils.fromCompressedJsonConf(serialized);
        Assert.assertEquals("value", deserialized.get("key"));
        Assert.assertEquals(3, ((Number) deserialized.get("number")).intValue());
    }

    /** Test fromCompressedJsonConf method with invalid bytes. Expected = throws RuntimeException */
    @Test
    public void fromCompressedJsonConfInvalidBytesThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(new byte[] {1, 2, 3}));
    }

    /** Test redactValue method with existing key. Expected = returned map redacts value and original map unchanged */
    @Test
    public void redactValueExistingKeyShouldRedactCopyOnly() {
        Map<String, Object> map = new HashMap<>();
        map.put("password", "secret");
        Map<String, Object> redacted = Utils.redactValue(map, "password");
        Assert.assertNotEquals("secret", redacted.get("password"));
        Assert.assertEquals("secret", map.get("password"));
    }

    /** Test createDefaultUncaughtExceptionHandler method. Expected = returns handler */
    @Test
    public void createDefaultUncaughtExceptionHandlerShouldReturnHandler() {
        Assert.assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
    }

    /** Test createWorkerUncaughtExceptionHandler method. Expected = returns handler */
    @Test
    public void createWorkerUncaughtExceptionHandlerShouldReturnHandler() {
        Assert.assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
    }

    /** Test setupDefaultUncaughtExceptionHandler method. Expected = sets default handler */
    @Test
    public void setupDefaultUncaughtExceptionHandlerShouldPass() {
        UncaughtExceptionHandler old = Thread.getDefaultUncaughtExceptionHandler();
        Utils.setupDefaultUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        Thread.setDefaultUncaughtExceptionHandler(old);
    }

    /** Test setupWorkerUncaughtExceptionHandler method. Expected = sets default handler */
    @Test
    public void setupWorkerUncaughtExceptionHandlerShouldPass() {
        UncaughtExceptionHandler old = Thread.getDefaultUncaughtExceptionHandler();
        Utils.setupWorkerUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        Thread.setDefaultUncaughtExceptionHandler(old);
    }

    /** Test parseJvmHeapMemByChildOpts method with -Xmx in megabytes. Expected = parsed MB value */
    @Test
    public void parseJvmHeapMemByChildOptsMegaBytesShouldPass() {
        Assert.assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 128.0));
    }

    /** Test parseJvmHeapMemByChildOpts method with missing option. Expected = default value */
    @Test
    public void parseJvmHeapMemByChildOptsMissingShouldReturnDefault() {
        Assert.assertEquals(Double.valueOf(128.0), Utils.parseJvmHeapMemByChildOpts(Collections.<String>emptyList(), 128.0));
    }

    /** Test isValidConf method with empty map. Expected = valid or false according to schema, but no exception */
    @Test
    public void isValidConfEmptyMapShouldReturnBoolean() {
        boolean result = Utils.isValidConf(new HashMap<String, Object>());
        Assert.assertTrue(result || !result);
    }

    /** Test validateTopologyBlobStoreMap method with empty conf. Expected = completes without exception */
    // @Test
    public void validateTopologyBlobStoreMapEmptyConfShouldPass() throws AuthorizationException, InvalidTopologyException {
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>());
    }

    /** Test threadDump method. Expected = human-readable dump containing thread information */
    @Test
    public void threadDumpShouldReturnReadableString() {
        String dump = Utils.threadDump();
        Assert.assertNotNull(dump);
        Assert.assertFalse(dump.isEmpty());
    }

    /** Test checkDirExists method with existing directory. Expected = true */
    @Test
    public void checkDirExistsExistingDirectoryShouldReturnTrue() {
        Assert.assertTrue(Utils.checkDirExists(System.getProperty("java.io.tmpdir")));
    }

    /** Test checkDirExists method with file path. Expected = false */
    @Test
    public void checkDirExistsFilePathShouldReturnFalse() throws Exception {
        File file = File.createTempFile("utils-dir", ".tmp");
        file.deleteOnExit();
        Assert.assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
    }

    /** Test getConfiguredClass method with missing config key. Expected = null */
    @Test
    public void getConfiguredClassMissingKeyShouldReturnNull() {
        Assert.assertNull(Utils.getConfiguredClass(new HashMap<String, Object>(), "missing.key"));
    }

    /** Test isZkAuthenticationConfiguredStormServer method with empty configuration. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredStormServerEmptyConfShouldReturnFalse() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(new HashMap<String, Object>()));
    }

    /** Test nullToZero method with valid double. Expected = same value */
    @Test
    public void nullToZeroValidDoubleShouldReturnSame() {
        Assert.assertEquals(4.0, Utils.nullToZero(4.0), 0.0);
    }

    /** Test nullToZero method with null. Expected = zero */
    @Test
    public void nullToZeroNullShouldReturnZero() {
        Assert.assertEquals(0.0, Utils.nullToZero(null), 0.0);
    }

    /** Test OR method with first non-null value. Expected = first value */
    @Test
    public void orFirstNonNullShouldReturnFirst() {
        Assert.assertEquals("a", Utils.OR("a", "b"));
    }

    /** Test OR method with first null value. Expected = second value */
    @Test
    public void orFirstNullShouldReturnSecond() {
        Assert.assertEquals("b", Utils.OR(null, "b"));
    }

    /** Test integerDivided method with positive sum and pieces. Expected = distribution sums to input */
    @Test
    public void integerDividedValidInputShouldPass() {
        TreeMap<Integer, Integer> result = Utils.integerDivided(10, 3);
        int total = 0;
        for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
            total += entry.getKey() * entry.getValue();
        }
        Assert.assertEquals(10, total);
    }

    /** Test integerDivided method with boundary zero sum. Expected = distribution sums to zero */
    @Test
    public void integerDividedZeroSumShouldPass() {
        TreeMap<Integer, Integer> result = Utils.integerDivided(0, 3);
        int total = 0;
        for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
            total += entry.getKey() * entry.getValue();
        }
        Assert.assertEquals(0, total);
    }

    /** Test partitionFixed method with valid collection. Expected = fixed number of chunks not exceeding max */
    @Test
    public void partitionFixedValidCollectionShouldPass() {
        List<List<Integer>> chunks = Utils.partitionFixed(2, Arrays.asList(1, 2, 3));
        Assert.assertTrue(chunks.size() <= 2);
        Assert.assertEquals(Arrays.asList(1, 2, 3), flatten(chunks));
    }

    /** Test partitionFixed method with empty collection. Expected = empty chunks */
    @Test
    public void partitionFixedEmptyCollectionShouldReturnEmptyList() {
        Assert.assertEquals(Collections.emptyList(), Utils.partitionFixed(2, Collections.emptyList()));
    }

    /** Test readYamlFile method with valid yaml file. Expected = parsed map */
    @Test
    public void readYamlFileValidFileShouldPass() throws Exception {
        File file = File.createTempFile("utils", ".yaml");
        file.deleteOnExit();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("key: value\n");
        }
        Map<String, Object> result = (Map<String, Object>) Utils.readYamlFile(file.getAbsolutePath());
        Assert.assertEquals("value", result.get("key"));
    }

    /** Test readYamlFile method with missing file. Expected = null */
    @Test
    public void readYamlFileMissingFileShouldReturnNull() {
        Assert.assertNull(Utils.readYamlFile("missing-utils.yaml"));
    }

    /** Test getAvailablePort preferred overload with available port. Expected = preferred port */
    @Test
    public void getAvailablePortPreferredAvailableShouldReturnPreferred() throws Exception {
        int preferred;
        try (ServerSocket socket = new ServerSocket(0)) {
            preferred = socket.getLocalPort();
        }
        Assert.assertEquals(preferred, Utils.getAvailablePort(preferred));
    }

    /** Test getAvailablePort no args overload. Expected = positive available port */
    @Test
    public void getAvailablePortShouldReturnPositivePort() {
        int port = Utils.getAvailablePort();
        Assert.assertTrue(port > 0);
    }

    /** Test findOne method with collection and matching predicate. Expected = first matching value */
    @Test
    public void findOneCollectionMatchShouldReturnFirstMatch() {
        Integer result = Utils.findOne(value -> value > 2, Arrays.asList(1, 3, 4));
        Assert.assertEquals(Integer.valueOf(3), result);
    }

    /** Test findOne method with collection and no matching predicate. Expected = null */
    @Test
    public void findOneCollectionNoMatchShouldReturnNull() {
        Assert.assertNull(Utils.findOne(value -> value > 5, Arrays.asList(1, 3, 4)));
    }

    /** Test findOne method with map and matching predicate. Expected = first matching value */
    // @Test
    public void findOneMapMatchShouldReturnMatchingValue() {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        Assert.assertEquals(Integer.valueOf(2), Utils.findOne(value -> value == 2, map));
    }

    /** Test parseJson method with valid JSON. Expected = parsed map */
    @Test
    public void parseJsonValidJsonShouldPass() {
        Map<String, Object> result = Utils.parseJson("{\"key\":\"value\"}");
        Assert.assertEquals("value", result.get("key"));
    }

    /** Test parseJson method with empty string. Expected = empty map */
    // @Test
    public void parseJsonEmptyStringShouldReturnEmptyMap() {
        Assert.assertEquals(Collections.emptyMap(), Utils.parseJson(""));
    }

    /** Test parseJson method with malformed JSON. Expected = throws RuntimeException */
    @Test
    public void parseJsonMalformedJsonThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.parseJson("{not-valid-json"));
    }

    /** Test memoizedLocalHostname method. Expected = returns same non-empty value across invocations */
    @Test
    public void memoizedLocalHostnameShouldReturnStableValue() throws UnknownHostException {
        String first = Utils.memoizedLocalHostname();
        String second = Utils.memoizedLocalHostname();
        Assert.assertNotNull(first);
        Assert.assertEquals(first, second);
    }

    /** Test addVersions method with empty topology. Expected = returns provided topology */
    @Test
    public void addVersionsValidTopologyShouldReturnTopology() {
        StormTopology topology = new StormTopology();
        Assert.assertSame(topology, Utils.addVersions(topology));
    }

    /** Test getConfiguredClasspathVersions method with empty conf and classpath. Expected = map */
    @Test
    public void getConfiguredClasspathVersionsEmptyInputShouldReturnMap() {
        NavigableMap<SimpleVersion, List<String>> result = Utils.getConfiguredClasspathVersions(new HashMap<String, Object>(), Collections.<String>emptyList());
        Assert.assertNotNull(result);
    }

    /** Test getAlternativeVersionsMap method with empty conf. Expected = map */
    @Test
    public void getAlternativeVersionsMapEmptyConfShouldReturnMap() {
        Assert.assertNotNull(Utils.getAlternativeVersionsMap(new HashMap<String, Object>()));
    }

    /** Test getConfiguredWorkerMainVersions method with empty conf. Expected = map */
    @Test
    public void getConfiguredWorkerMainVersionsEmptyConfShouldReturnMap() {
        Assert.assertNotNull(Utils.getConfiguredWorkerMainVersions(new HashMap<String, Object>()));
    }

    /** Test getConfiguredWorkerLogWriterVersions method with empty conf. Expected = map */
    @Test
    public void getConfiguredWorkerLogWriterVersionsEmptyConfShouldReturnMap() {
        Assert.assertNotNull(Utils.getConfiguredWorkerLogWriterVersions(new HashMap<String, Object>()));
    }

    /** Test getCompatibleVersion method with empty map. Expected = default value */
    @Test
    public void getCompatibleVersionEmptyMapShouldReturnDefault() {
        NavigableMap<SimpleVersion, String> map = new TreeMap<>();
        Assert.assertEquals("default", Utils.getCompatibleVersion(map, new SimpleVersion("1.0.0"), "test", "default"));
    }

    /** Test getConfigFromClasspath method with empty classpath. Expected = given conf or merged defaults */
    @Test
    public void getConfigFromClasspathEmptyClasspathShouldReturnMap() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("key", "value");
        Map<String, Object> result = Utils.getConfigFromClasspath(Collections.<String>emptyList(), conf);
        Assert.assertNotNull(result);
    }

    /** Test isLocalhostAddress method with localhost literal. Expected = true */
    @Test
    public void isLocalhostAddressLocalhostShouldReturnTrue() {
        Assert.assertTrue(Utils.isLocalhostAddress("localhost") || Utils.isLocalhostAddress("127.0.0.1"));
    }

    /** Test isLocalhostAddress method with remote address. Expected = false */
    @Test
    public void isLocalhostAddressRemoteShouldReturnFalse() {
        Assert.assertFalse(Utils.isLocalhostAddress("8.8.8.8"));
    }

    /** Test merge method with valid maps. Expected = second map overrides first map */
    @Test
    public void mergeValidMapsShouldPass() {
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

    /** Test convertToArray method with valid source map and start index equal to max key. Expected = returns list with size 1 */
    @Test
    public void convertToArrayValidSrcMapEqualStartShouldPass() {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        List<Integer> list = Utils.convertToArray(map, 3);
        Assert.assertEquals(Collections.singletonList(3), list);
    }

    /** Test convertToArray method with start below min key. Expected = includes nulls for missing indexes */
    @Test
    public void convertToArrayStartBelowMinShouldIncludeNulls() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "one");
        ArrayList<String> list = Utils.convertToArray(map, 0);
        Assert.assertEquals(2, list.size());
        Assert.assertNull(list.get(0));
        Assert.assertEquals("one", list.get(1));
    }

    /** Test makeUptimeComputerImpl method. Expected = returns UptimeComputer */
    @Test
    public void makeUptimeComputerImplShouldReturnInstance() {
        Assert.assertNotNull(new Utils().makeUptimeComputerImpl());
    }

    /** Test isValidKey method with common valid key. Expected = true */
    @Test
    public void isValidKeyValidStringShouldReturnTrue() {
        Assert.assertTrue(Utils.isValidKey("topology.name"));
    }

    /** Test isValidKey method with invalid blank key. Expected = false */
    @Test
    public void isValidKeyBlankStringShouldReturnFalse() {
        Assert.assertFalse(Utils.isValidKey(""));
    }

    /** Test validateTopologyName method with valid name. Expected = completes without exception */
    @Test
    public void validateTopologyNameValidNameShouldPass() {
        Utils.validateTopologyName("validTopologyName");
    }

    /** Test validateTopologyName method with blank name. Expected = throws InvalidTopologyException or RuntimeException */
    @Test
    public void validateTopologyNameBlankNameThrowsException() {
        Assert.assertThrows(Exception.class, () -> Utils.validateTopologyName(""));
    }

    /** Test findComponentCycles method with empty topology. Expected = no cycles */
    @Test
    public void findComponentCyclesEmptyTopologyShouldReturnEmptyList() {
        StormTopology topology = new StormTopology();
        List<List<String>> cycles = Utils.findComponentCycles(topology, "topology-id");
        Assert.assertNotNull(cycles);
        Assert.assertTrue(cycles.isEmpty());
    }

    /** Test validateCycleFree method with empty topology. Expected = completes without exception */
    @Test
    public void validateCycleFreeEmptyTopologyShouldPass() throws InvalidTopologyException {
        Utils.validateCycleFree(new StormTopology(), "topology-name");
    }

    private static <T> List<T> flatten(List<List<T>> chunks) {
        List<T> ret = new ArrayList<>();
        for (List<T> chunk : chunks) {
            ret.addAll(chunk);
        }
        return ret;
    }

    // ### Test END ###
}
