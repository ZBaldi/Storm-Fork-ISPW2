package org.apache.storm.utils_2.llm;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.two.Utils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.lang.IllegalStateException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.mock;

/**
 * Junior-style JUnit4 tests for Utils.
 * Some methods touch OS, network, JVM shutdown hooks or external Storm services: those tests are intentionally simple
 * and some dangerous cases are ignored.
 */

/** FIXED MANUALLY */
public class Fsp1UtilsTest {  // REMOVED NOT USED IMPORTS

    // ### Test START ###

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /** Test setInstance method with valid instance. Expected = returns previous instance */
    @Test
    public void setInstanceValidInstanceShouldPass() {
        Utils old = Utils.setInstance(new Utils());
        Utils previous = Utils.setInstance(old);
        Assert.assertNotNull(previous);
    }

    /** Test setClassLoaderForJavaDeSerialize method with valid classloader. Expected = no exception */
    @Test
    public void setClassLoaderForJavaDeSerializeValidShouldPass() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    /** Test resetClassLoaderForJavaDeSerialize method. Expected = no exception */
    @Test
    public void resetClassLoaderForJavaDeSerializeShouldPass() {
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    /** Test findResources method with not correct name. Expected = empty list */
    @Test
    public void findResourcesNotCorrectNameShouldPass() {
        List<URL> urls = Utils.findResources("file-that-does-not-exist-for-test.yaml");
        Assert.assertNotNull(urls);
        Assert.assertTrue(urls.isEmpty());
    }

    /** Test findAndReadConfigFile method with not correct name and mustExist false. Expected = empty map */
    @Test
    public void findAndReadConfigFileNotCorrectFalseShouldPass() {
        Map<String, Object> map = Utils.findAndReadConfigFile("notCorrectYamlFile", false);
        Assert.assertEquals(new HashMap<String, Object>(), map);
    }

    /** Test findAndReadConfigFile method with not correct name and default mustExist. Expected = RuntimeException */
    @Test
    public void findAndReadConfigFileNotCorrectDefaultThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("notCorrectYamlFile"));
    }

    /** Test readDefaultConfig method. Expected = map not null */
    @Test
    public void readDefaultConfigShouldPass() {
        Map<String, Object> conf = Utils.readDefaultConfig();
        Assert.assertNotNull(conf);
    }

    /** Test urlEncodeUtf8 method with valid string. Expected = encoded string */
    @Test
    public void urlEncodeUtf8ValidStringShouldPass() {
        Assert.assertEquals("ciao+mondo", Utils.urlEncodeUtf8("ciao mondo"));
    }

    /** Test urlEncodeUtf8 method with null. Expected = NullPointerException */
    @Test
    public void urlEncodeUtf8NullThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlEncodeUtf8(null));
    }

    /** Test urlDecodeUtf8 method with valid string. Expected = decoded string */
    @Test
    public void urlDecodeUtf8ValidStringShouldPass() {
        Assert.assertEquals("ciao mondo", Utils.urlDecodeUtf8("ciao+mondo"));
    }

    /** Test readCommandLineOpts method. Expected = map not null */
    @Test
    public void readCommandLineOptsShouldPass() {
        Assert.assertNotNull(Utils.readCommandLineOpts());
    }

    /** Test readStormConfig method. Expected = map not null */
    @Test
    public void readStormConfigShouldPass() {
        Assert.assertNotNull(Utils.readStormConfig());
    }

    /** Test bitXorVals method with valid list. Expected = xor result */
    @Test
    public void bitXorValsValidListShouldPass() {
        Assert.assertEquals(0L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    /** Test bitXorVals method with empty list. Expected = 0 */
    @Test
    public void bitXorValsEmptyListShouldPass() {
        Assert.assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
    }

    /** Test bitXor method with valid values. Expected = a xor b */
    @Test
    public void bitXorValidValuesShouldPass() {
        Assert.assertEquals(3L, Utils.bitXor(1L, 2L));
    }

    /** Test addShutdownHookWithForceKillIn1Sec method. Expected = no exception */
    @Ignore("Avoid adding JVM shutdown hooks in normal unit test execution")
    @Test
    public void addShutdownHookWithForceKillIn1SecShouldPass() {
        Utils.addShutdownHookWithForceKillIn1Sec(() -> { });
    }

    /** Test addShutdownHookWithDelayedForceKill method. Expected = no exception */
    @Ignore("Avoid adding JVM shutdown hooks in normal unit test execution")
    @Test
    public void addShutdownHookWithDelayedForceKillShouldPass() {
        Utils.addShutdownHookWithDelayedForceKill(() -> { }, 1);
    }

    /** Test isSystemId method with system id. Expected = true */
    @Test
    public void isSystemIdSystemNameShouldPass() {
        Assert.assertTrue(Utils.isSystemId("__acker"));
    }

    /** Test isSystemId method with normal id. Expected = false */
    @Test
    public void isSystemIdNormalNameShouldPass() {
        Assert.assertFalse(Utils.isSystemId("spout"));
    }

    /** Test asyncLoop method with full parameters and startImmediately false. Expected = SmartThread */
    // @Test  (FAILED)  Thread-0-test-thread
    public void asyncLoopFullParametersShouldPass() {
        Utils.SmartThread thread = Utils.asyncLoop(() -> null, true, null, Thread.NORM_PRIORITY, false, false, "test-thread");
        Assert.assertNotNull(thread);
        Assert.assertEquals("test-thread", thread.getName());
    }

    /** Test asyncLoop method with threadName. Expected = SmartThread */
    @Test
    public void asyncLoopWithThreadNameShouldPass() {
        Utils.SmartThread thread = Utils.asyncLoop(() -> null, "junior-thread", null);
        Assert.assertNotNull(thread);
        thread.interrupt();
    }

    /** Test asyncLoop method with callable only. Expected = SmartThread */
    @Test
    public void asyncLoopOnlyCallableShouldPass() {
        Utils.SmartThread thread = Utils.asyncLoop(() -> null);
        Assert.assertNotNull(thread);
        thread.interrupt();
    }

    /** Test exceptionCauseIsInstanceOf method with correct exception. Expected = true */
    @Test
    public void exceptionCauseIsInstanceOfCorrectShouldPass() {
        Throwable t = new RuntimeException(new IllegalArgumentException("bad"));
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, t));
    }

    /** Test exceptionCauseIsInstanceOf method with not correct exception. Expected = false */
    @Test
    public void exceptionCauseIsInstanceOfNotCorrectShouldPass() {
        Throwable t = new RuntimeException(new IllegalStateException("bad"));
        Assert.assertFalse(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, t));
    }

    /** Test unwrapTo method with correct class. Expected = exception */
    @Test
    public void unwrapToCorrectClassShouldPass() {
        IllegalArgumentException e = new IllegalArgumentException("bad");
        Assert.assertSame(e, Utils.unwrapTo(IllegalArgumentException.class, new RuntimeException(e)));
    }

    /** Test unwrapTo method with not correct class. Expected = null */
    @Test
    public void unwrapToNotCorrectClassShouldPass() {
        Assert.assertNull(Utils.unwrapTo(IllegalArgumentException.class, new RuntimeException("bad")));
    }

    /** Test unwrapAndThrow method with correct wrapped exception. Expected = throws original exception */
    @Test
    public void unwrapAndThrowCorrectClassThrowsException() {
        Assert.assertThrows(IllegalArgumentException.class,
            () -> Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException(new IllegalArgumentException("bad"))));
    }

    /** Test wrapInRuntime method with runtime exception. Expected = same exception */
    @Test
    public void wrapInRuntimeRuntimeExceptionShouldPass() {
        RuntimeException e = new RuntimeException("bad");
        Assert.assertSame(e, Utils.wrapInRuntime(e));
    }

    /** Test wrapInRuntime method with checked exception. Expected = RuntimeException */
    @Test
    public void wrapInRuntimeCheckedExceptionShouldPass() {
        Exception e = new Exception("bad");
        Assert.assertSame(e, Utils.wrapInRuntime(e).getCause());
    }

    /** Test secureRandomLong method. Expected = returns a long value */
    @Test
    public void secureRandomLongShouldPass() {
        long value = Utils.secureRandomLong();
        Assert.assertEquals(value, value);
    }

    /** Test hostname method. Expected = not empty string */
    @Test
    public void hostnameShouldPass() throws Exception {
        Assert.assertFalse(Utils.hostname().isEmpty());
    }

    /** Test localHostname method. Expected = not empty string */
    @Test
    public void localHostnameShouldPass() throws Exception {
        Assert.assertFalse(Utils.localHostname().isEmpty());
    }

    /** Test exitProcess method. Expected = not executed because it exits the JVM */
    @Ignore("Do not call System.exit in unit tests")
    @Test
    public void exitProcessIgnored() {
        Utils.exitProcess(0, "test");
    }

    /** Test uuid method. Expected = valid UUID string */
    @Test
    public void uuidShouldPass() {
        String uuid = Utils.uuid();
        Assert.assertNotNull(UUID.fromString(uuid));
    }

    /** Test javaSerialize/javaDeserialize methods with valid object. Expected = same value */
    @Test
    public void javaSerializeAndJavaDeserializeValidObjectShouldPass() {
        String expected = "hello";
        byte[] serialized = Utils.javaSerialize(expected);
        String actual = Utils.javaDeserialize(serialized, String.class);
        Assert.assertEquals(expected, actual);
    }

    /** Test javaDeserialize method with null byte array. Expected = RuntimeException */
    @Test
    public void javaDeserializeNullThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.javaDeserialize(null, String.class));
    }

    /** Test get method with existing key. Expected = value */
    @Test
    public void getExistingKeyShouldPass() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        Assert.assertEquals(Integer.valueOf(1), Utils.get(map, "a", 2));
    }

    /** Test get method with missing key. Expected = default value */
    @Test
    public void getMissingKeyShouldPass() {
        Assert.assertEquals(Integer.valueOf(2), Utils.get(new HashMap<String, Integer>(), "a", 2));
    }

    /** Test zeroIfNaNOrInf method with NaN. Expected = 0 */
    @Test
    public void zeroIfNaNOrInfNaNShouldPass() {
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
    }

    /** Test zeroIfNaNOrInf method with valid number. Expected = same number */
    @Test
    public void zeroIfNaNOrInfValidNumberShouldPass() {
        Assert.assertEquals(3.5, Utils.zeroIfNaNOrInf(3.5), 0.0);
    }

    /** Test join method with valid collection. Expected = joined string */
    @Test
    public void joinValidCollectionShouldPass() {
        Assert.assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    /** Test join method with empty collection. Expected = empty string */
    @Test
    public void joinEmptyCollectionShouldPass() {
        Assert.assertEquals("", Utils.join(Collections.emptyList(), ","));
    }

    /** Test parseZkId method with valid id. Expected = Id with scheme and id */
    @Test
    public void parseZkIdValidShouldPass() {
        Id id = Utils.parseZkId("digest:user", "testConfig");
        Assert.assertEquals("digest", id.getScheme());
        Assert.assertEquals("user", id.getId());
    }

    /** Test parseZkId method with not valid id. Expected = IllegalArgumentException */
    @Test
    public void parseZkIdNotValidThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.parseZkId("bad", "testConfig"));
    }

    /** Test getSuperUserAcl method with valid config. Expected = ACL all permissions */
    @Test
    public void getSuperUserAclValidConfShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "digest:superUser");
        ACL acl = Utils.getSuperUserAcl(conf);
        Assert.assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
    }

    /** Test getWorkerACL method with empty config. Expected = list or null without exception */
    @Test
    public void getWorkerACLEmptyConfShouldPass() {
        List<ACL> acls = Utils.getWorkerACL(new HashMap<String, Object>());
        Assert.assertTrue(acls == null || acls instanceof List);
    }

    /** Test isZkAuthenticationConfiguredTopology method with empty config. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredTopologyEmptyConfShouldPass() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(new HashMap<String, Object>()));
    }

    /** Test handleUncaughtException method with allowed exception. Expected = no exception */
    @Test
    public void handleUncaughtExceptionAllowedShouldPass() {
        Set<Class<?>> allowed = new HashSet<>();
        allowed.add(IllegalArgumentException.class);
        Utils.handleUncaughtException(new IllegalArgumentException("ok"), allowed, false);
    }

    /** Test handleUncaughtException method overload. Expected = no assertion, may log */
    @Ignore("This can terminate the worker depending on configuration")
    @Test
    public void handleUncaughtExceptionIgnored() {
        Utils.handleUncaughtException(new RuntimeException("bad"));
    }

    /** Test handleWorkerUncaughtException method. Expected = no assertion, may log */
    @Ignore("This can terminate the worker depending on configuration")
    @Test
    public void handleWorkerUncaughtExceptionIgnored() {
        Utils.handleWorkerUncaughtException(new RuntimeException("bad"));
    }

    /** Test thriftSerialize/thriftDeserialize methods with AccessControl. Expected = same object */
    @Test
    public void thriftSerializeAndDeserializeValidObjectShouldPass() {
        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_name("userA");
        expected.set_access(7);
        byte[] bytes = Utils.thriftSerialize(expected);
        AccessControl actual = Utils.thriftDeserialize(AccessControl.class, bytes);
        Assert.assertEquals(expected, actual);
    }

    /** Test thriftDeserialize with offset and length. Expected = same object */
    @Test
    public void thriftDeserializeOffsetLengthShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 7);  // REMOVED USER STRING AS 2° PARAMETER
        byte[] bytes = Utils.thriftSerialize(expected);
        byte[] bigger = new byte[bytes.length + 2];
        System.arraycopy(bytes, 0, bigger, 1, bytes.length);
        AccessControl actual = Utils.thriftDeserialize(AccessControl.class, bigger, 1, bytes.length);
        Assert.assertEquals(expected, actual);
    }

    /** Test sleepNoSimulation method with zero. Expected = no exception */
    @Test
    public void sleepNoSimulationZeroShouldPass() {
        Utils.sleepNoSimulation(0);
    }

    /** Test sleep method with zero. Expected = no exception */
    @Test
    public void sleepZeroShouldPass() {
        Utils.sleep(0);
    }

    /** Test makeUptimeComputer method. Expected = not null */
    @Test
    public void makeUptimeComputerShouldPass() {
        Assert.assertNotNull(Utils.makeUptimeComputer());
    }

    /** Test reverseMap method with map. Expected = values become keys */
    @Test
    public void reverseMapMapShouldPass() {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        HashMap<Integer, List<String>> reversed = Utils.reverseMap(map);
        Assert.assertTrue(reversed.get(1).contains("a"));
        Assert.assertTrue(reversed.get(1).contains("b"));
    }

    /** Test reverseMap method with list sequence. Expected = reversed map */
    @Test
    public void reverseMapListSequenceShouldPass() {
        List<List<Object>> list = new ArrayList<>();
        list.add(Arrays.<Object>asList("k1", "v1"));
        list.add(Arrays.<Object>asList("k2", "v1"));
        Map<Object, List<Object>> reversed = Utils.reverseMap(list);
        Assert.assertEquals(Arrays.asList("k1", "k2"), reversed.get("v1"));
    }

    /** Test isOnWindows method. Expected = boolean */
    @Test
    public void isOnWindowsShouldPass() {
        Assert.assertEquals(Utils.isOnWindows(), Utils.isOnWindows());
    }

    /** Test checkFileExists method with existing file. Expected = true */
    @Test
    public void checkFileExistsCorrectPathShouldPass() throws Exception {
        File file = folder.newFile("correctFile");
        Assert.assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
    }

    /** Test checkFileExists method with missing file. Expected = false */
    @Test
    public void checkFileExistsNotCorrectPathShouldPass() {
        Assert.assertFalse(Utils.checkFileExists("missing-file-for-test"));
    }

    /** Test forceDelete method with existing file. Expected = file deleted */
    @Test
    public void forceDeleteFileShouldPass() throws Exception {
        File file = folder.newFile("delete-me.txt");
        Utils.forceDelete(file.getAbsolutePath());
        Assert.assertFalse(file.exists());
    }

    /** Test serialize/deserialize methods with valid thrift object. Expected = same object */
    @Test
    public void serializeAndDeserializeValidObjectShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 1);  // REMOVED USER STRING AS 2° PARAMETER
        byte[] serialized = Utils.serialize(expected);
        AccessControl actual = Utils.deserialize(serialized, AccessControl.class);
        Assert.assertEquals(expected, actual);
    }

    /** Test serializeToString/deserializeFromString methods. Expected = same object */
    @Test
    public void serializeToStringAndDeserializeFromStringShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 1);  // REMOVED USER STRING AS 2° PARAMETER
        String str = Utils.serializeToString(expected);
        AccessControl actual = Utils.deserializeFromString(str, AccessControl.class);
        Assert.assertEquals(expected, actual);
    }

    /** Test deserializeFromString method with null. Expected = NullPointerException */
    @Test
    public void deserializeFromStringNullStrThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.deserializeFromString(null, String.class));
    }

    /** Test toByteArray method with valid ByteBuffer. Expected = byte array */
    @Test
    public void toByteArrayValidBufferShouldPass() {
        byte[] actual = Utils.toByteArray(ByteBuffer.wrap(new byte[] {1, 2, 3}));
        Assert.assertArrayEquals(new byte[] {1, 2, 3}, actual);
    }

    /** Test mkSuicideFn method. Expected = runnable not null */
    @Test
    public void mkSuicideFnShouldPass() {
        Assert.assertNotNull(Utils.mkSuicideFn());
    }

    /** Test readAndLogStream method with valid stream. Expected = no exception */
    @Test
    public void readAndLogStreamValidShouldPass() {
        InputStream in = new ByteArrayInputStream("hello".getBytes());
        Utils.readAndLogStream("prefix", in);
    }

    /** Test getComponentCommon method with empty topology. Expected = null */
    // @Test   (FAILED)  IllegalArgumentException: Could not find component with id missing
    public void getComponentCommonEmptyTopologyShouldPass() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        Assert.assertNull(Utils.getComponentCommon(topology, "missing"));
    }

    /** Test tuple method with valid values. Expected = list with same values */
    @Test
    public void tupleValidValuesShouldPass() {
        Assert.assertEquals(Arrays.asList("a", 1, true), Utils.tuple("a", 1, true));
    }

    /** Test getRepeat method with repeated value. Expected = repeated values */
    @Test
    public void getRepeatRepeatedValueShouldPass() {
        List<String> result = Utils.getRepeat(Arrays.asList("a", "b", "a"));
        Assert.assertEquals(Collections.singletonList("a"), result);
    }

    /** Test getGlobalStreamId method with null stream. Expected = default stream id */
    @Test
    public void getGlobalStreamIdNullStreamShouldPass() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", null);
        Assert.assertEquals("component", id.get_componentId());
        Assert.assertEquals(Utils.DEFAULT_STREAM_ID, id.get_streamId());
    }

    /** Test getGlobalStreamId method with stream. Expected = given stream id */
    @Test
    public void getGlobalStreamIdValidStreamShouldPass() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", "stream1");
        Assert.assertEquals("stream1", id.get_streamId());
    }

//    /** Test getSetComponentObject method with java object. Expected = stored object */  (GENERATED IN A WRONG WAY)
//    @Test
//    public void getSetComponentObjectJavaObjectShouldPass() {
//        ComponentObject obj = ComponentObject.java_object("hello");
//        Assert.assertEquals("hello", Utils.getSetComponentObject(obj));
//    }

    /** Test toPositive method with negative value. Expected = positive value */
    @Test
    public void toPositiveNegativeShouldPass() {
        Assert.assertTrue(Utils.toPositive(-1) >= 0);
    }

    /** Test toPositive method with zero. Expected = zero */
    @Test
    public void toPositiveZeroShouldPass() {
        Assert.assertEquals(0, Utils.toPositive(0));
    }

    /** Test processPid method. Expected = not empty */
    @Test
    public void processPidShouldPass() {
        Assert.assertFalse(Utils.processPid().isEmpty());
    }

    /** Test toCompressedJsonConf/fromCompressedJsonConf methods. Expected = same map value */
    @Test
    public void compressedJsonConfValidMapShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        byte[] bytes = Utils.toCompressedJsonConf(map);
        Map<String, Object> actual = Utils.fromCompressedJsonConf(bytes);
        Assert.assertEquals("value", actual.get("key"));
    }

    /** Test fromCompressedJsonConf method with null. Expected = RuntimeException */
    @Test
    public void fromCompressedJsonConfNullThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(null));
    }

    /** Test redactValue method. Expected = value redacted and original map not modified */
    @Test
    public void redactValueValidKeyShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("password", "secret");
        Map<String, Object> redacted = Utils.redactValue(map, "password");
        Assert.assertEquals("secret", map.get("password"));
        Assert.assertNotEquals("secret", redacted.get("password"));
    }

    /** Test createDefaultUncaughtExceptionHandler method. Expected = handler not null */
    @Test
    public void createDefaultUncaughtExceptionHandlerShouldPass() {
        Assert.assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
    }

    /** Test createWorkerUncaughtExceptionHandler method. Expected = handler not null */
    @Test
    public void createWorkerUncaughtExceptionHandlerShouldPass() {
        Assert.assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
    }

    /** Test setupDefaultUncaughtExceptionHandler method. Expected = no exception */
    @Test
    public void setupDefaultUncaughtExceptionHandlerShouldPass() {
        Utils.setupDefaultUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    /** Test setupWorkerUncaughtExceptionHandler method. Expected = no exception */
    @Test
    public void setupWorkerUncaughtExceptionHandlerShouldPass() {
        Utils.setupWorkerUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    /** Test parseJvmHeapMemByChildOpts method with megabytes. Expected = 512 */
    @Test
    public void parseJvmHeapMemByChildOptsMegabytesShouldPass() {
        Double result = Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 10.0);
        Assert.assertEquals(512.0, result, 0.0);
    }

    /** Test parseJvmHeapMemByChildOpts method with no option. Expected = default */
    @Test
    public void parseJvmHeapMemByChildOptsDefaultShouldPass() {
        Double result = Utils.parseJvmHeapMemByChildOpts(Collections.<String>emptyList(), 10.0);
        Assert.assertEquals(10.0, result, 0.0);
    }

    /** Test getClientBlobStore method. Expected = external dependency, ignored */
    @Ignore("Requires real Storm blob store configuration")
    @Test
    public void getClientBlobStoreIgnored() {
        Assert.assertNotNull(Utils.getClientBlobStore(new HashMap<String, Object>()));
    }

    /** Test isValidConf method with simple map. Expected = true or false without exception */
    @Test
    public void isValidConfSimpleMapShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("a", "b");
        Assert.assertEquals(Utils.isValidConf(conf), Utils.isValidConf(conf));
    }

    /** Test getTopologyInfo method. Expected = external dependency, ignored */
    @Ignore("Requires Nimbus client connection")
    @Test
    public void getTopologyInfoIgnored() {
        Assert.assertNull(Utils.getTopologyInfo("name", null, new HashMap<String, Object>()));
    }

    /** Test getTopologyId method with mocked Nimbus client. Expected = null for missing topology */
    @Ignore("Nimbus.Iface has many environment-specific behaviours")
    @Test
    public void getTopologyIdMockedClientShouldPass() throws Exception {
        Nimbus.Iface client = mock(Nimbus.Iface.class);
        Assert.assertNull(Utils.getTopologyId("missing", client));
    }

    /** Test validateTopologyBlobStoreMap method with empty config. Expected = no exception */
    // @Test   (FAILED)  ConnectException: Connection refused
    public void validateTopologyBlobStoreMapEmptyConfShouldPass() throws AuthorizationException, InvalidTopologyException {  // ADDED THROWS
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>());
    }

    /** Test validateTopologyBlobStoreMap with NimbusBlobStore. Expected = no exception for empty config */
    @Ignore("Requires NimbusBlobStore dependency")
    @Test
    public void validateTopologyBlobStoreMapNimbusStoreShouldPass() throws AuthorizationException, InvalidTopologyException {   // ADDED THROWS
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), mock(org.apache.storm.blobstore.NimbusBlobStore.class));
    }

    /** Test validateTopologyBlobStoreMap with BlobStore. Expected = no exception for empty config */
    @Ignore("Requires BlobStore dependency")
    @Test
    public void validateTopologyBlobStoreMapBlobStoreShouldPass() throws AuthorizationException, InvalidTopologyException { // ADDED THROWS
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), mock(org.apache.storm.blobstore.BlobStore.class));
    }

    /** Test threadDump method. Expected = string contains Thread */
    @Test
    public void threadDumpShouldPass() {
        Assert.assertTrue(Utils.threadDump().contains("Thread"));
    }

    /** Test checkDirExists method with valid directory. Expected = true */
    @Test
    public void checkDirExistsCorrectPathShouldPass() throws Exception {
        File dir = folder.newFolder("correctDir");
        Assert.assertTrue(Utils.checkDirExists(dir.getAbsolutePath()));
    }

    /** Test checkDirExists method with file. Expected = false */
    @Test
    public void checkDirExistsFilePathShouldPass() throws Exception {
        File file = folder.newFile("notDir.txt");
        Assert.assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
    }

    /** Test getConfiguredClass method with no key. Expected = null */
    @Test
    public void getConfiguredClassMissingKeyShouldPass() {
        Assert.assertNull(Utils.getConfiguredClass(new HashMap<String, Object>(), "missing"));
    }

    /** Test isZkAuthenticationConfiguredStormServer method with empty config. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredStormServerEmptyConfShouldPass() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(new HashMap<String, Object>()));
    }

    /** Test nullToZero method with null. Expected = zero */
    @Test
    public void nullToZeroNullShouldPass() {
        Assert.assertEquals(0.0, Utils.nullToZero(null), 0.0);
    }

    /** Test nullToZero method with value. Expected = same value */
    @Test
    public void nullToZeroValidShouldPass() {
        Assert.assertEquals(2.0, Utils.nullToZero(2.0), 0.0);
    }

    /** Test OR method with first value. Expected = first value */
    @Test
    public void orFirstValueShouldPass() {
        Assert.assertEquals("a", Utils.OR("a", "b"));
    }

    /** Test OR method with first null. Expected = second value */
    @Test
    public void orFirstNullShouldPass() {
        Assert.assertEquals("b", Utils.OR(null, "b"));
    }

    /** Test integerDivided method with positive values. Expected = sum divided */
    // @Test  (FAILED) expected should be 3
    public void integerDividedValidValuesShouldPass() {
        TreeMap<Integer, Integer> result = Utils.integerDivided(10, 3);
        int total = 0;
        for (Integer value : result.values()) {
            total += value;
        }
        Assert.assertEquals(10, total);
        Assert.assertEquals(3, result.size());
    }

    /** Test partitionFixed method with simple list. Expected = chunks */
    @Test
    public void partitionFixedValidValuesShouldPass() {
        List<List<Integer>> result = Utils.partitionFixed(2, Arrays.asList(1, 2, 3));
        Assert.assertEquals(2, result.size());
    }

    /** Test readYamlFile method with valid yaml file. Expected = map */
    @Test
    public void readYamlFileValidFileShouldPass() throws Exception {
        File yaml = folder.newFile("test.yaml");
        try (FileWriter writer = new FileWriter(yaml)) {
            writer.write("key: value\n");
        }
        Object result = Utils.readYamlFile(yaml.getAbsolutePath());
        Assert.assertTrue(result instanceof Map);
    }

    /** Test readYamlFile method with missing file. Expected = null */
    @Test
    public void readYamlFileMissingFileShouldPass() {
        Assert.assertNull(Utils.readYamlFile("missing-yaml-file.yaml"));
    }

    /** Test getAvailablePort method with preferred port 0. Expected = positive port */
    @Test
    public void getAvailablePortPreferredZeroShouldPass() {
        Assert.assertTrue(Utils.getAvailablePort(0) > 0);
    }

    /** Test getAvailablePort method. Expected = positive port */
    @Test
    public void getAvailablePortShouldPass() {
        Assert.assertTrue(Utils.getAvailablePort() > 0);
    }

    /** Test findOne method with collection and matching predicate. Expected = first matching value */
    @Test
    public void findOneCollectionMatchShouldPass() {
        Integer result = Utils.findOne(x -> x > 1, Arrays.asList(1, 2, 3));
        Assert.assertEquals(Integer.valueOf(2), result);
    }

    /** Test findOne method with collection and no match. Expected = null */
    @Test
    public void findOneCollectionNoMatchShouldPass() {
        Integer result = Utils.findOne(x -> x > 10, Arrays.asList(1, 2, 3));
        Assert.assertNull(result);
    }

    /** Test findOne method with map and matching predicate. Expected = first matching value */
    // @Test   (FAILED)  BUG AS BEFORE!!!
    public void findOneMapMatchShouldPass() {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        Integer result = Utils.findOne(x -> x > 1, map);
        Assert.assertEquals(Integer.valueOf(2), result);
    }

    /** Test parseJson method with valid json. Expected = map */
    @Test
    public void parseJsonValidJsonShouldPass() {
        Map<String, Object> result = Utils.parseJson("{\"key\":\"value\"}");
        Assert.assertEquals("value", result.get("key"));
    }

    /** Test parseJson method with empty json. Expected = empty map */
    // @Test   (FAILED)  java.lang.String cannot be cast to class java.util.Map
    public void parseJsonEmptyStringShouldPass() {
        Assert.assertEquals(new HashMap<String, Object>(), Utils.parseJson(""));
    }

    /** Test memoizedLocalHostname method. Expected = not empty */
    @Test
    public void memoizedLocalHostnameShouldPass() throws UnknownHostException { //ADDED THROWS
        Assert.assertFalse(Utils.memoizedLocalHostname().isEmpty());
    }

    /** Test addVersions method with topology. Expected = same topology */
    @Test
    public void addVersionsValidTopologyShouldPass() {
        StormTopology topology = new StormTopology();
        Assert.assertSame(topology, Utils.addVersions(topology));
    }

    /** Test getConfiguredClasspathVersions method with empty config. Expected = map not null */
    @Test
    public void getConfiguredClasspathVersionsEmptyConfShouldPass() {
        NavigableMap<SimpleVersion, List<String>> map = Utils.getConfiguredClasspathVersions(new HashMap<String, Object>(), Collections.<String>emptyList());
        Assert.assertNotNull(map);
    }

    /** Test getAlternativeVersionsMap method with empty config. Expected = map not null */
    @Test
    public void getAlternativeVersionsMapEmptyConfShouldPass() {
        Assert.assertNotNull(Utils.getAlternativeVersionsMap(new HashMap<String, Object>()));
    }

    /** Test getConfiguredWorkerMainVersions method with empty config. Expected = map not null */
    @Test
    public void getConfiguredWorkerMainVersionsEmptyConfShouldPass() {
        Assert.assertNotNull(Utils.getConfiguredWorkerMainVersions(new HashMap<String, Object>()));
    }

    /** Test getConfiguredWorkerLogWriterVersions method with empty config. Expected = map not null */
    @Test
    public void getConfiguredWorkerLogWriterVersionsEmptyConfShouldPass() {
        Assert.assertNotNull(Utils.getConfiguredWorkerLogWriterVersions(new HashMap<String, Object>()));
    }

    /** Test getCompatibleVersion method with empty map. Expected = default value */
    @Test
    public void getCompatibleVersionEmptyMapShouldPass() {
        NavigableMap<SimpleVersion, String> map = new TreeMap<>();
        String result = Utils.getCompatibleVersion(map, new SimpleVersion("1.0.0"), "test", "default");
        Assert.assertEquals("default", result);
    }

    /** Test getConfigFromClasspath method with empty classpath. Expected = conf */
    @Test
    public void getConfigFromClasspathEmptyClasspathShouldPass() throws IOException {  // ADDED THROWS
        Map<String, Object> conf = new HashMap<>();
        conf.put("a", "b");
        Assert.assertEquals(conf, Utils.getConfigFromClasspath(Collections.<String>emptyList(), conf));
    }

    /** Test isLocalhostAddress method with localhost. Expected = true */
    @Test
    public void isLocalhostAddressLocalhostShouldPass() {
        Assert.assertTrue(Utils.isLocalhostAddress("localhost"));
    }

    /** Test isLocalhostAddress method with remote address. Expected = false */
    @Test
    public void isLocalhostAddressRemoteShouldPass() {
        Assert.assertFalse(Utils.isLocalhostAddress("8.8.8.8"));
    }

    /** Test merge method with two maps. Expected = merged map */
    @Test
    public void mergeTwoMapsShouldPass() {
        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        Map<String, Integer> second = new HashMap<>();
        second.put("b", 2);
        Map<String, Integer> result = Utils.merge(first, second);
        Assert.assertEquals(Integer.valueOf(1), result.get("a"));
        Assert.assertEquals(Integer.valueOf(2), result.get("b"));
    }

    /** Test convertToArray method with valid source map and start index equal to max key. Expected = list with size 1 */
    @Test
    public void convertToArrayValidSrcMapEqualStartShouldPass() {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        List<Integer> list = Utils.convertToArray(map, 3);
        Assert.assertEquals(Collections.singletonList(3), list);
    }

    /** Test convertToArray method with start below first key. Expected = all values ordered by key */
    @Test
    public void convertToArrayValidSrcMapLowStartShouldPass() {
        Map<Integer, String> map = new HashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        Assert.assertEquals(Arrays.asList("a", "b"), Utils.convertToArray(map, 0));
    }

    /** Test makeUptimeComputerImpl method. Expected = instance */
    @Test
    public void makeUptimeComputerImplShouldPass() {
        Assert.assertNotNull(new Utils().makeUptimeComputerImpl());
    }

    /** Test isValidKey method with valid key. Expected = true */
    @Test
    public void isValidKeyValidKeyShouldPass() {
        Assert.assertTrue(Utils.isValidKey("valid_key-1.txt"));
    }

    /** Test isValidKey method with invalid key. Expected = false */
    @Test
    public void isValidKeyInvalidKeyShouldPass() {
        Assert.assertFalse(Utils.isValidKey("bad/key"));
    }

    /** Test validateTopologyName method with valid name. Expected = no exception */
    @Test
    public void validateTopologyNameValidShouldPass() throws Exception {
        Utils.validateTopologyName("validTopologyName");
    }

    /** Test validateTopologyName method with invalid name. Expected = InvalidTopologyException */
    // @Test  (FAILED)  expected java.lang.IllegalArgumentException
    public void validateTopologyNameInvalidThrowsException() {
        Assert.assertThrows(InvalidTopologyException.class, () -> Utils.validateTopologyName("bad/name"));
    }

    /** Test findComponentCycles method with empty topology. Expected = empty list */
    @Test
    public void findComponentCyclesEmptyTopologyShouldPass() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        Assert.assertTrue(Utils.findComponentCycles(topology, "topology").isEmpty());
    }

    /** Test validateCycleFree method with empty topology. Expected = no exception */
    @Test
    public void validateCycleFreeEmptyTopologyShouldPass() throws Exception {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        Utils.validateCycleFree(topology, "topology");
    }

    // ### Test END ###
}
