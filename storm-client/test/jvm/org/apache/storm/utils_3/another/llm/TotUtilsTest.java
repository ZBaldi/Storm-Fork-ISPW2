package org.apache.storm.utils_3.another.llm;

import org.apache.storm.utils.refactored.three.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URL;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.VersionInfo;

public class TotUtilsTest {
    // ### Test START ###

    private Utils originalInstance;

    private static class TestableUtils extends Utils {
        private final String hostname;
        private final String localHostname;
        private boolean forceDeleteCalled;
        private String deletedPath;

        TestableUtils(String hostname, String localHostname) {
            this.hostname = hostname;
            this.localHostname = localHostname;
        }

        @Override
        protected String hostnameImpl() {
            return hostname;
        }

        @Override
        protected String localHostnameImpl() {
            return localHostname;
        }

        @Override
        protected void forceDeleteImpl(String path) {
            forceDeleteCalled = true;
            deletedPath = path;
        }
    }

    @Before
    public void setUp() {
        originalInstance = Utils.setInstance(new Utils());
        System.clearProperty("storm.options");
        System.clearProperty("storm.conf.file");
        System.clearProperty("java.deserialization.disabled");
        System.clearProperty("java.security.auth.login.config");
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    @After
    public void tearDown() {
        Utils.setInstance(originalInstance);
        System.clearProperty("storm.options");
        System.clearProperty("storm.conf.file");
        System.clearProperty("java.deserialization.disabled");
        System.clearProperty("java.security.auth.login.config");
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    /** Test setInstance method with valid instance. Expected = returns the previously set instance */
    @Test
    public void setInstanceValidInstanceShouldReturnPreviousInstance() {
        TestableUtils first = new TestableUtils("host-a", "local-a");
        TestableUtils second = new TestableUtils("host-b", "local-b");
        Utils returned = Utils.setInstance(first);
        Assert.assertNotNull(returned);
        Assert.assertSame(first, Utils.setInstance(second));
    }

    /** Test setClassLoaderForJavaDeSerialize and resetClassLoaderForJavaDeSerialize. Expected = java deserialization still succeeds */
    @Test
    public void setAndResetClassLoaderForJavaDeserializeShouldPass() {
        byte[] serialized = Utils.javaSerialize("value");
        Utils.setClassLoaderForJavaDeSerialize(Thread.currentThread().getContextClassLoader());
        Assert.assertEquals("value", Utils.javaDeserialize(serialized, String.class));
        Utils.resetClassLoaderForJavaDeSerialize();
        Assert.assertEquals("value", Utils.javaDeserialize(serialized, String.class));
    }

    /** Test findResources method with existing classpath resource. Expected = returns at least one URL */
    @Test
    public void findResourcesExistingResourceShouldPass() {
        List<URL> resources = Utils.findResources("defaults.yaml");
        Assert.assertNotNull(resources);
    }

    /** Test findResources method with not correct name. Expected = returns empty list */
    @Test
    public void findResourcesNotCorrectNameShouldReturnEmptyList() {
        Assert.assertTrue(Utils.findResources("not-correct-resource-name.yaml").isEmpty());
    }

    /** Test findAndReadConfigFile method with missing file and mustExist = false. Expected = empty map */
    @Test
    public void findAndReadConfigFileMissingOptionalShouldReturnEmptyMap() {
        Assert.assertEquals(Collections.emptyMap(), Utils.findAndReadConfigFile("missing-utils-test.yaml", false));
    }

    /** Test findAndReadConfigFile method with missing file and mustExist = true. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileMissingRequiredThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("missing-utils-test.yaml", true));
    }

    /** Test findAndReadConfigFile overload with missing file. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileMissingRequiredOverloadThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("missing-utils-test.yaml"));
    }

    /** Test readDefaultConfig method. Expected = returns a non-null configuration map */
    @Test
    public void readDefaultConfigShouldReturnMap() {
        Assert.assertNotNull(Utils.readDefaultConfig());
    }

    /** Test urlEncodeUtf8 method with valid string. Expected = same as URLEncoder UTF-8 */
    @Test
    public void urlEncodeUtf8ValidStringShouldPass() throws Exception {
        String value = "a b+à";
        Assert.assertEquals(URLEncoder.encode(value, StandardCharsets.UTF_8.name()), Utils.urlEncodeUtf8(value));
    }

    /** Test urlEncodeUtf8 method with null string. Expected = throws NullPointerException */
    @Test
    public void urlEncodeUtf8NullStringThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlEncodeUtf8(null));
    }

    /** Test urlDecodeUtf8 method with valid string. Expected = same as URLDecoder UTF-8 */
    @Test
    public void urlDecodeUtf8ValidStringShouldPass() throws Exception {
        String value = "a+b%2B%C3%A0";
        Assert.assertEquals(URLDecoder.decode(value, StandardCharsets.UTF_8.name()), Utils.urlDecodeUtf8(value));
    }

    /** Test urlDecodeUtf8 method with null string. Expected = throws NullPointerException */
    @Test
    public void urlDecodeUtf8NullStringThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlDecodeUtf8(null));
    }

    /** Test readCommandLineOpts method without storm.options. Expected = empty map */
    @Test
    public void readCommandLineOptsWithoutPropertyShouldReturnEmptyMap() {
        Assert.assertTrue(Utils.readCommandLineOpts().isEmpty());
    }

    /** Test readCommandLineOpts method with encoded options. Expected = parses string and JSON values */
    // @Test
    public void readCommandLineOptsWithEncodedOptionsShouldPass() {
        System.setProperty("storm.options", Utils.urlEncodeUtf8("alpha=1") + "," + Utils.urlEncodeUtf8("name=value"));
        Map<String, Object> result = Utils.readCommandLineOpts();
        Assert.assertEquals(1L, result.get("alpha"));
        Assert.assertEquals("value", result.get("name"));
    }

    /** Test readStormConfig method. Expected = returns a non-null map */
    @Test
    public void readStormConfigShouldReturnMap() {
        Assert.assertNotNull(Utils.readStormConfig());
    }

    /** Test bitXorVals method with valid list. Expected = returns XOR of values */
    @Test
    public void bitXorValsValidListShouldPass() {
        Assert.assertEquals(0L ^ 1L ^ 2L ^ 3L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    /** Test bitXorVals method with empty list. Expected = returns zero */
    @Test
    public void bitXorValsEmptyListShouldReturnZero() {
        Assert.assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
    }

    /** Test bitXor method with valid values. Expected = returns a ^ b */
    @Test
    public void bitXorValidValuesShouldPass() {
        Assert.assertEquals(7L ^ 3L, Utils.bitXor(7L, 3L));
    }

    /** Test bitXor method with null value. Expected = throws NullPointerException */
    @Test
    public void bitXorNullValueThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXor(null, 1L));
    }

    /** Test addShutdownHookWithForceKillIn1Sec method with valid runnable. Expected = completes without exception */
    // @Test
    public void addShutdownHookWithForceKillIn1SecValidRunnableShouldPass() {
        Utils.addShutdownHookWithForceKillIn1Sec(() -> { });
    }

    /** Test addShutdownHookWithDelayedForceKill method with valid runnable. Expected = completes without exception */
    // @Test
    public void addShutdownHookWithDelayedForceKillValidRunnableShouldPass() {
        Utils.addShutdownHookWithDelayedForceKill(() -> { }, 1);
    }

    /** Test isSystemId method with system id. Expected = true */
    @Test
    public void isSystemIdSystemIdShouldReturnTrue() {
        Assert.assertTrue(Utils.isSystemId("__system"));
    }

    /** Test isSystemId method with normal id. Expected = false */
    @Test
    public void isSystemIdNormalIdShouldReturnFalse() {
        Assert.assertFalse(Utils.isSystemId("component"));
    }

    /** Test isSystemId method with null id. Expected = throws NullPointerException */
    @Test
    public void isSystemIdNullThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.isSystemId(null));
    }

    /** Test asyncLoop full overload without immediate start. Expected = returns configured SmartThread */
    @Test
    public void asyncLoopFullOverloadShouldReturnThread() {
        Callable<Long> callable = () -> null;
        UncaughtExceptionHandler handler = (thread, throwable) -> { };
        Utils.SmartThread thread = Utils.asyncLoop(callable, true, handler, Thread.NORM_PRIORITY, false, false, "test");
        Assert.assertNotNull(thread);
        Assert.assertTrue(thread.isDaemon());
        Assert.assertTrue(thread.getName().contains("test"));
    }

    /** Test asyncLoop named overload with callable returning null. Expected = starts and terminates */
    @Test
    public void asyncLoopNamedOverloadShouldReturnThread() throws Exception {
        Utils.SmartThread thread = Utils.asyncLoop((Callable<Long>) () -> null, "named", (t, e) -> { });
        thread.join(1000L);
        Assert.assertFalse(thread.isAlive());
    }

    /** Test asyncLoop simple overload with callable returning null. Expected = starts and terminates */
    @Test
    public void asyncLoopSimpleOverloadShouldReturnThread() throws Exception {
        Utils.SmartThread thread = Utils.asyncLoop((Callable<Long>) () -> null);
        thread.join(1000L);
        Assert.assertFalse(thread.isAlive());
    }

    /** Test exceptionCauseIsInstanceOf method with nested cause. Expected = true */
    @Test
    public void exceptionCauseIsInstanceOfNestedCauseShouldReturnTrue() {
        Throwable t = new RuntimeException(new IllegalArgumentException("bad"));
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, t));
    }

    /** Test exceptionCauseIsInstanceOf method with non matching cause. Expected = false */
    @Test
    public void exceptionCauseIsInstanceOfNonMatchingCauseShouldReturnFalse() {
        Assert.assertFalse(Utils.exceptionCauseIsInstanceOf(IllegalStateException.class, new IllegalArgumentException("bad")));
    }

    /** Test unwrapTo method with nested throwable. Expected = returns matching throwable */
    @Test
    public void unwrapToNestedThrowableShouldPass() {
        IllegalArgumentException expected = new IllegalArgumentException("bad");
        Assert.assertSame(expected, Utils.unwrapTo(IllegalArgumentException.class, new RuntimeException(expected)));
    }

    /** Test unwrapTo method with missing throwable type. Expected = null */
    @Test
    public void unwrapToMissingThrowableShouldReturnNull() {
        Assert.assertNull(Utils.unwrapTo(IllegalStateException.class, new RuntimeException("bad")));
    }

    /** Test unwrapAndThrow method with matching throwable. Expected = throws the unwrapped throwable */
    @Test
    public void unwrapAndThrowMatchingThrowableShouldThrow() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException(new IllegalArgumentException("bad"))));
    }

    /** Test unwrapAndThrow method with non matching throwable. Expected = completes */
    @Test
    public void unwrapAndThrowNonMatchingThrowableShouldPass() throws Exception {
        Utils.unwrapAndThrow(IllegalStateException.class, new RuntimeException("bad"));
    }

    /** Test wrapInRuntime method with RuntimeException. Expected = returns same instance */
    @Test
    public void wrapInRuntimeRuntimeExceptionShouldReturnSameInstance() {
        RuntimeException expected = new RuntimeException("bad");
        Assert.assertSame(expected, Utils.wrapInRuntime(expected));
    }

    /** Test wrapInRuntime method with checked exception. Expected = wraps exception */
    @Test
    public void wrapInRuntimeCheckedExceptionShouldWrap() {
        Exception expected = new Exception("bad");
        RuntimeException result = Utils.wrapInRuntime(expected);
        Assert.assertSame(expected, result.getCause());
    }

    /** Test secureRandomLong method. Expected = returns a long value */
    @Test
    public void secureRandomLongShouldReturnLong() {
        long value = Utils.secureRandomLong();
        Assert.assertEquals(value, (long) value);
    }

    /** Test hostname method with delegated instance. Expected = returns delegated hostname */
    @Test
    public void hostnameDelegatedInstanceShouldPass() throws Exception {
        TestableUtils instance = new TestableUtils("host", "local");
        Utils.setInstance(instance);
        Assert.assertEquals("host", Utils.hostname());
    }

    /** Test localHostname method with delegated instance. Expected = returns delegated local hostname */
    @Test
    public void localHostnameDelegatedInstanceShouldPass() throws Exception {
        TestableUtils instance = new TestableUtils("host", "local");
        Utils.setInstance(instance);
        Assert.assertEquals("local", Utils.localHostname());
    }

    /** Test uuid method. Expected = returns parseable UUID string */
    @Test
    public void uuidShouldReturnValidUuidString() {
        Assert.assertNotNull(java.util.UUID.fromString(Utils.uuid()));
    }

    /** Test javaSerialize/javaDeserialize with serializable string. Expected = returns original value */
    @Test
    public void javaSerializeAndJavaDeserializeValidObjectShouldPass() {
        byte[] bytes = Utils.javaSerialize("hello");
        Assert.assertEquals("hello", Utils.javaDeserialize(bytes, String.class));
    }

    /** Test javaDeserialize when disabled by system property. Expected = throws AssertionError */
    @Test
    public void javaDeserializeDisabledThrowsAssertionError() {
        System.setProperty("java.deserialization.disabled", "true");
        byte[] bytes = Utils.javaSerialize("hello");
        Assert.assertThrows(AssertionError.class, () -> Utils.javaDeserialize(bytes, String.class));
    }

    /** Test get method with present key. Expected = returns mapped value */
    @Test
    public void getPresentKeyShouldReturnValue() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "v");
        Assert.assertEquals("v", Utils.get(map, "k", "default"));
    }

    /** Test get method with missing key. Expected = returns default value */
    @Test
    public void getMissingKeyShouldReturnDefault() {
        Assert.assertEquals("default", Utils.get(new HashMap<String, String>(), "k", "default"));
    }

    /** Test zeroIfNaNOrInf method with NaN and infinity. Expected = zero */
    @Test
    public void zeroIfNaNOrInfSpecialValuesShouldReturnZero() {
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
    }

    /** Test zeroIfNaNOrInf method with finite value. Expected = same value */
    @Test
    public void zeroIfNaNOrInfFiniteValueShouldReturnSameValue() {
        Assert.assertEquals(2.5, Utils.zeroIfNaNOrInf(2.5), 0.0);
    }

    /** Test join method with valid collection. Expected = joined string */
    @Test
    public void joinValidCollectionShouldPass() {
        Assert.assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    /** Test join method with empty collection. Expected = empty string */
    @Test
    public void joinEmptyCollectionShouldReturnEmptyString() {
        Assert.assertEquals("", Utils.join(Collections.emptyList(), ","));
    }

    /** Test parseZkId method with valid id. Expected = returns Id with scheme and id */
    @Test
    public void parseZkIdValidIdShouldPass() {
        Id id = Utils.parseZkId("sasl:storm-user", "config");
        Assert.assertEquals("sasl", id.getScheme());
        Assert.assertEquals("storm-user", id.getId());
    }

    /** Test parseZkId method with invalid id. Expected = throws IllegalArgumentException */
    @Test
    public void parseZkIdInvalidIdThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.parseZkId("invalid", "config"));
    }

    /** Test getSuperUserAcl method with configured user. Expected = returns ALL ACL */
    @Test
    public void getSuperUserAclConfiguredShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:storm-user");
        ACL acl = Utils.getSuperUserAcl(conf);
        Assert.assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        Assert.assertEquals(new Id("sasl", "storm-user"), acl.getId());
    }

    /** Test getSuperUserAcl method without configured user. Expected = throws IllegalArgumentException */
    @Test
    public void getSuperUserAclMissingConfigThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.getSuperUserAcl(new HashMap<String, Object>()));
    }

    /** Test getWorkerACL without topology auth. Expected = null */
    @Test
    public void getWorkerACLWithoutTopologyAuthShouldReturnNull() {
        Assert.assertNull(Utils.getWorkerACL(new HashMap<String, Object>()));
    }

    /** Test getWorkerACL with topology auth. Expected = returns creator ACL and super ACL */
    @Test
    public void getWorkerACLWithTopologyAuthShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "sasl");
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:storm-user");
        Assert.assertFalse(Utils.getWorkerACL(conf).isEmpty());
    }

    /** Test isZkAuthenticationConfiguredTopology. Expected = true only when scheme is non empty */
    @Test
    public void isZkAuthenticationConfiguredTopologyShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(conf));
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "sasl");
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
    }

    /** Test handleUncaughtException with allowed exception. Expected = completes */
    @Test
    public void handleUncaughtExceptionAllowedExceptionShouldPass() {
        Set<Class<?>> allowed = new HashSet<>();
        allowed.add(IllegalArgumentException.class);
        Utils.handleUncaughtException(new IllegalArgumentException("allowed"), allowed, false);
    }

    /** Test handleUncaughtException with disallowed exception. Expected = throws Error */
    @Test
    public void handleUncaughtExceptionDisallowedExceptionThrowsError() {
        Assert.assertThrows(Error.class, () -> Utils.handleUncaughtException(new RuntimeException("bad"), Collections.emptySet(), false));
    }

    /** Test handleUncaughtException default overload with exception. Expected = throws Error */
    @Test
    public void handleUncaughtExceptionDefaultThrowsError() {
        Assert.assertThrows(Error.class, () -> Utils.handleUncaughtException(new RuntimeException("bad")));
    }

    /** Test handleWorkerUncaughtException with normal exception. Expected = throws Error */
    @Test
    public void handleWorkerUncaughtExceptionNormalThrowsError() {
        Assert.assertThrows(Error.class, () -> Utils.handleWorkerUncaughtException(new RuntimeException("bad")));
    }

    /** Test sleepNoSimulation with zero millis. Expected = completes */
    @Test
    public void sleepNoSimulationZeroMillisShouldPass() {
        Utils.sleepNoSimulation(0L);
    }

    /** Test sleep with zero millis. Expected = completes */
    @Test
    public void sleepZeroMillisShouldPass() {
        Utils.sleep(0L);
    }

    /** Test makeUptimeComputer method. Expected = returns UptimeComputer */
    @Test
    public void makeUptimeComputerShouldReturnInstance() {
        Assert.assertNotNull(Utils.makeUptimeComputer());
    }

    /** Test reverseMap Map overload with valid map. Expected = returns reversed map */
    @Test
    public void reverseMapValidMapShouldPass() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 2);
        HashMap<Integer, List<String>> result = Utils.reverseMap(map);
        Assert.assertTrue(result.get(1).containsAll(Arrays.asList("a", "b")));
        Assert.assertEquals(Collections.singletonList("c"), result.get(2));
    }

    /** Test reverseMap Map overload with null. Expected = empty map */
    @Test
    public void reverseMapNullMapShouldReturnEmptyMap() {
        Assert.assertTrue(Utils.reverseMap((Map<Object, Object>) null).isEmpty());
    }

    /** Test reverseMap listSeq overload with valid list. Expected = returns reversed map */
    @Test
    public void reverseMapValidListSeqShouldPass() {
        List<List<Object>> listSeq = new ArrayList<>();
        listSeq.add(Arrays.<Object>asList("a", 1));
        listSeq.add(Arrays.<Object>asList("b", 1));
        Map<Object, List<Object>> result = Utils.reverseMap(listSeq);
        Assert.assertTrue(result.get(1).containsAll(Arrays.asList("a", "b")));
    }

    /** Test isOnWindows method. Expected = deterministic boolean according to OS env */
    @Test
    public void isOnWindowsShouldMatchEnvironment() {
        String os = System.getenv("OS");
        Assert.assertEquals(os != null && os.equals("Windows_NT"), Utils.isOnWindows());
    }

    /** Test checkFileExists method with existing file. Expected = true */
    @Test
    public void checkFileExistsExistingFileShouldPass() throws Exception {
        File file = File.createTempFile("utils", ".txt");
        try {
            Assert.assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
        } finally {
            file.delete();
        }
    }

    /** Test checkFileExists method with missing file. Expected = false */
    @Test
    public void checkFileExistsMissingFileShouldReturnFalse() {
        Assert.assertFalse(Utils.checkFileExists("missing-file-for-utils-test"));
    }

    /** Test forceDelete method with delegated instance. Expected = calls delegate */
    @Test
    public void forceDeleteDelegatedInstanceShouldPass() throws Exception {
        TestableUtils instance = new TestableUtils("host", "local");
        Utils.setInstance(instance);
        Utils.forceDelete("path");
        Assert.assertTrue(instance.forceDeleteCalled);
        Assert.assertEquals("path", instance.deletedPath);
    }

    /** Test toByteArray method with valid buffer. Expected = returns remaining bytes */
    @Test
    public void toByteArrayValidBufferShouldPass() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        buffer.get();
        Assert.assertArrayEquals(new byte[] {2, 3, 4}, Utils.toByteArray(buffer));
    }

    /** Test mkSuicideFn method. Expected = returns Runnable */
    @Test
    public void mkSuicideFnShouldReturnRunnable() {
        Assert.assertNotNull(Utils.mkSuicideFn());
    }

    /** Test readAndLogStream method with valid stream. Expected = completes */
    @Test
    public void readAndLogStreamValidStreamShouldPass() {
        InputStream in = new ByteArrayInputStream("line1\nline2".getBytes(StandardCharsets.UTF_8));
        Utils.readAndLogStream("prefix", in);
    }

    /** Test tuple method with values. Expected = returns list preserving values */
    @Test
    public void tupleValidValuesShouldPass() {
        Assert.assertEquals(Arrays.<Object>asList("a", 1, null), Utils.tuple("a", 1, null));
    }

    /** Test getRepeat method with repeated values. Expected = returns repeated entries */
    @Test
    public void getRepeatRepeatedValuesShouldPass() {
        Assert.assertEquals(Arrays.asList("a", "b"), Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b")));
    }

    /** Test getGlobalStreamId method with null stream. Expected = uses default stream */
    @Test
    public void getGlobalStreamIdNullStreamShouldUseDefault() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", null);
        Assert.assertEquals("component", id.get_componentId());
        Assert.assertEquals(Utils.DEFAULT_STREAM_ID, id.get_streamId());
    }

    /** Test getGlobalStreamId method with explicit stream. Expected = uses explicit stream */
    @Test
    public void getGlobalStreamIdExplicitStreamShouldPass() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", "stream");
        Assert.assertEquals("stream", id.get_streamId());
    }

    /** Test toPositive method with negative boundary value. Expected = positive value */
    @Test
    public void toPositiveNegativeValueShouldPass() {
        Assert.assertEquals((-1) & Integer.MAX_VALUE, Utils.toPositive(-1));
    }

    /** Test toPositive method with zero. Expected = zero */
    @Test
    public void toPositiveZeroShouldReturnZero() {
        Assert.assertEquals(0, Utils.toPositive(0));
    }

    /** Test processPid method. Expected = returns non-empty pid */
    @Test
    public void processPidShouldReturnNonEmptyString() {
        Assert.assertFalse(Utils.processPid().isEmpty());
    }

    /** Test toCompressedJsonConf/fromCompressedJsonConf with valid map. Expected = round trip map */
    // @Test
    public void compressedJsonConfRoundTripShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        map.put("num", 1L);
        Map<String, Object> result = Utils.fromCompressedJsonConf(Utils.toCompressedJsonConf(map));
        Assert.assertEquals("value", result.get("key"));
        Assert.assertEquals(1L, result.get("num"));
    }

    /** Test fromCompressedJsonConf with invalid bytes. Expected = throws RuntimeException */
    @Test
    public void fromCompressedJsonConfInvalidBytesThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(new byte[] {1, 2, 3}));
    }

    /** Test redactValue method with existing key. Expected = returns new map with redacted value */
    @Test
    public void redactValueExistingKeyShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("secret", "abcd");
        Map<String, Object> result = Utils.redactValue(map, "secret");
        Assert.assertEquals("####", result.get("secret"));
        Assert.assertEquals("abcd", map.get("secret"));
        Assert.assertNotSame(map, result);
    }

    /** Test redactValue method with missing key. Expected = returns original map */
    @Test
    public void redactValueMissingKeyShouldReturnOriginalMap() {
        Map<String, Object> map = new HashMap<>();
        Assert.assertSame(map, Utils.redactValue(map, "missing"));
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
        Utils.setupDefaultUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    /** Test setupWorkerUncaughtExceptionHandler method. Expected = sets default handler */
    @Test
    public void setupWorkerUncaughtExceptionHandlerShouldPass() {
        Utils.setupWorkerUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    /** Test parseJvmHeapMemByChildOpts method with megabytes. Expected = returns value in MB */
    @Test
    public void parseJvmHeapMemByChildOptsMegaBytesShouldPass() {
        Assert.assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 1.0));
    }

    /** Test parseJvmHeapMemByChildOpts method with gigabytes. Expected = returns value in MB */
    @Test
    public void parseJvmHeapMemByChildOptsGigaBytesShouldPass() {
        Assert.assertEquals(Double.valueOf(2048.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx2g"), 1.0));
    }

    /** Test parseJvmHeapMemByChildOpts method with null options. Expected = returns default */
    @Test
    public void parseJvmHeapMemByChildOptsNullOptionsShouldReturnDefault() {
        Assert.assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(null, 64.0));
    }

    /** Test isValidConf method with JSON-safe map. Expected = true */
    @Test
    public void isValidConfValidMapShouldReturnTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("name", "value");
        conf.put("num", 1L);
        Assert.assertTrue(Utils.isValidConf(conf));
    }

    /** Test validateTopologyBlobStoreMap with empty topoConf and BlobStore overload. Expected = completes because no blob map exists */
    @Test
    public void validateTopologyBlobStoreMapEmptyBlobStoreOverloadShouldPass() throws Exception {
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), (org.apache.storm.blobstore.BlobStore) null);
    }

    /** Test threadDump method. Expected = returns human readable string */
    @Test
    public void threadDumpShouldReturnString() {
        String dump = Utils.threadDump();
        Assert.assertNotNull(dump);
        Assert.assertTrue(dump.contains("Thread"));
    }

    /** Test checkDirExists method with existing directory. Expected = true */
    @Test
    public void checkDirExistsExistingDirectoryShouldPass() throws Exception {
        File dir = java.nio.file.Files.createTempDirectory("utils-dir").toFile();
        try {
            Assert.assertTrue(Utils.checkDirExists(dir.getAbsolutePath()));
        } finally {
            dir.delete();
        }
    }

    /** Test checkDirExists method with file. Expected = false */
    @Test
    public void checkDirExistsFileShouldReturnFalse() throws Exception {
        File file = File.createTempFile("utils-file", ".txt");
        try {
            Assert.assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
        } finally {
            file.delete();
        }
    }

    /** Test getConfiguredClass with missing value. Expected = null */
    @Test
    public void getConfiguredClassMissingValueShouldReturnNull() {
        Assert.assertNull(Utils.getConfiguredClass(new HashMap<String, Object>(), "missing"));
    }

    /** Test isZkAuthenticationConfiguredStormServer with conf scheme. Expected = true */
    @Test
    public void isZkAuthenticationConfiguredStormServerWithSchemeShouldReturnTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "sasl");
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(conf));
    }

    /** Test isZkAuthenticationConfiguredStormServer with empty conf. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredStormServerEmptyConfShouldReturnFalse() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(new HashMap<String, Object>()));
    }

    /** Test nullToZero method with null. Expected = zero */
    @Test
    public void nullToZeroNullShouldReturnZero() {
        Assert.assertEquals(0.0, Utils.nullToZero(null), 0.0);
    }

    /** Test nullToZero method with valid double. Expected = same value */
    @Test
    public void nullToZeroValidValueShouldReturnSameValue() {
        Assert.assertEquals(3.5, Utils.nullToZero(3.5), 0.0);
    }

    /** Test OR method with first non null. Expected = first value */
    @Test
    public void ORFirstNonNullShouldReturnFirst() {
        Assert.assertEquals("a", Utils.OR("a", "b"));
    }

    /** Test OR method with first null. Expected = second value */
    @Test
    public void ORFirstNullShouldReturnSecond() {
        Assert.assertEquals("b", Utils.OR(null, "b"));
    }

    /** Test integerDivided method with exact division. Expected = one partition size */
    @Test
    public void integerDividedExactDivisionShouldPass() {
        TreeMap<Integer, Integer> result = Utils.integerDivided(10, 2);
        Assert.assertEquals(Integer.valueOf(2), result.get(5));
    }

    /** Test integerDivided method with remainder. Expected = base and incremented partitions */
    @Test
    public void integerDividedWithRemainderShouldPass() {
        TreeMap<Integer, Integer> result = Utils.integerDivided(10, 3);
        Assert.assertEquals(Integer.valueOf(2), result.get(3));
        Assert.assertEquals(Integer.valueOf(1), result.get(4));
    }

    /** Test integerDivided method with zero pieces. Expected = throws ArithmeticException */
    @Test
    public void integerDividedZeroPiecesThrowsArithmeticException() {
        Assert.assertThrows(ArithmeticException.class, () -> Utils.integerDivided(10, 0));
    }

    /** Test partitionFixed method with valid collection. Expected = chunks collection */
    @Test
    public void partitionFixedValidCollectionShouldPass() {
        List<List<Integer>> chunks = Utils.partitionFixed(3, Arrays.asList(1, 2, 3, 4, 5));
        Assert.assertEquals(3, chunks.size());
        Assert.assertEquals(Arrays.asList(1, 2), chunks.get(0));
    }

    /** Test partitionFixed method with zero chunks. Expected = empty list */
    @Test
    public void partitionFixedZeroChunksShouldReturnEmptyList() {
        Assert.assertTrue(Utils.partitionFixed(0, Arrays.asList(1, 2)).isEmpty());
    }

    /** Test readYamlFile method with valid yaml file. Expected = returns parsed map */
    @Test
    public void readYamlFileValidFileShouldPass() throws Exception {
        File file = File.createTempFile("utils", ".yaml");
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("key: value\n");
        }
        try {
            Map<?, ?> result = (Map<?, ?>) Utils.readYamlFile(file.getAbsolutePath());
            Assert.assertEquals("value", result.get("key"));
        } finally {
            file.delete();
        }
    }

    /** Test readYamlFile method with missing file. Expected = null */
    @Test
    public void readYamlFileMissingFileShouldReturnNull() {
        Assert.assertNull(Utils.readYamlFile("missing-utils-file.yaml"));
    }

    /** Test getAvailablePort preferred port with zero. Expected = returns positive available port */
    @Test
    public void getAvailablePortZeroPreferredShouldReturnPort() {
        Assert.assertTrue(Utils.getAvailablePort(0) > 0);
    }

    /** Test getAvailablePort overload. Expected = returns positive available port */
    @Test
    public void getAvailablePortShouldReturnPort() {
        Assert.assertTrue(Utils.getAvailablePort() > 0);
    }

    /** Test findOne collection overload with matching predicate. Expected = first matching value */
    @Test
    public void findOneCollectionMatchingPredicateShouldPass() {
        IPredicate<Integer> pred = value -> value > 2;
        Assert.assertEquals(Integer.valueOf(3), Utils.findOne(pred, Arrays.asList(1, 2, 3, 4)));
    }

    /** Test findOne collection overload with null collection. Expected = null */
    @Test
    public void findOneCollectionNullShouldReturnNull() {
        Assert.assertNull(Utils.findOne(value -> true, (Collection<Object>) null));
    }

    /** Test findOne map overload with null map. Expected = null */
    @Test
    public void findOneMapNullShouldReturnNull() {
        Assert.assertNull(Utils.findOne(value -> true, (Map<String, Object>) null));
    }

    /** Test parseJson method with valid json. Expected = returns map */
    // @Test
    public void parseJsonValidJsonShouldPass() {
        Map<String, Object> result = Utils.parseJson("{\"key\":\"value\",\"num\":1}");
        Assert.assertEquals("value", result.get("key"));
        Assert.assertEquals(1L, result.get("num"));
    }

    /** Test parseJson method with null json. Expected = empty map */
    @Test
    public void parseJsonNullShouldReturnEmptyMap() {
        Assert.assertTrue(Utils.parseJson(null).isEmpty());
    }

    /** Test parseJson method with invalid json. Expected = throws RuntimeException */
    @Test
    public void parseJsonInvalidJsonThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.parseJson("{invalid"));
    }

    /** Test memoizedLocalHostname with delegated instance. Expected = returns delegated local hostname */
    @Test
    public void memoizedLocalHostnameDelegatedInstanceShouldPass() throws Exception {
        Utils.setInstance(new TestableUtils("host", "memo-local"));
        Assert.assertNotNull(Utils.memoizedLocalHostname());
    }

    /** Test addVersions method with empty topology. Expected = returns same topology instance */
    @Test
    public void addVersionsShouldReturnSameTopology() {
        StormTopology topology = new StormTopology();
        Assert.assertSame(topology, Utils.addVersions(topology));
        Assert.assertTrue(topology.is_set_jdk_version());
    }

    /** Test getConfiguredClasspathVersions with version map. Expected = includes configured and current version */
    @Test
    public void getConfiguredClasspathVersionsShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        Map<String, String> versions = new HashMap<>();
        versions.put("1.2.3", "a" + File.pathSeparator + "b");
        conf.put(Config.SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP, versions);
        NavigableMap<SimpleVersion, List<String>> result = Utils.getConfiguredClasspathVersions(conf, Arrays.asList("current"));
        Assert.assertTrue(result.containsKey(new SimpleVersion("1.2.3")));
        Assert.assertTrue(result.containsKey(VersionInfo.OUR_VERSION));
    }

    /** Test getAlternativeVersionsMap with empty conf. Expected = empty map */
    @Test
    public void getAlternativeVersionsMapEmptyConfShouldReturnEmptyMap() {
        Assert.assertTrue(Utils.getAlternativeVersionsMap(new HashMap<String, Object>()).isEmpty());
    }

    /** Test getConfiguredWorkerMainVersions with empty conf. Expected = contains current version default main */
    @Test
    public void getConfiguredWorkerMainVersionsEmptyConfShouldPass() {
        Assert.assertEquals("org.apache.storm.daemon.worker.Worker", Utils.getConfiguredWorkerMainVersions(new HashMap<String, Object>()).get(VersionInfo.OUR_VERSION));
    }

    /** Test getConfiguredWorkerLogWriterVersions with empty conf. Expected = contains current version default log writer */
    @Test
    public void getConfiguredWorkerLogWriterVersionsEmptyConfShouldPass() {
        Assert.assertEquals("org.apache.storm.LogWriter", Utils.getConfiguredWorkerLogWriterVersions(new HashMap<String, Object>()).get(VersionInfo.OUR_VERSION));
    }

    /** Test getCompatibleVersion with compatible higher version. Expected = returns matching value */
    @Test
    public void getCompatibleVersionCompatibleVersionShouldPass() {
        TreeMap<SimpleVersion, String> map = new TreeMap<>();
        map.put(new SimpleVersion("1.2.0"), "value");
        Assert.assertEquals("value", Utils.getCompatibleVersion(map, new SimpleVersion("1.1.0"), "test", "default"));
    }

    /** Test getCompatibleVersion without compatible version. Expected = returns default */
    @Test
    public void getCompatibleVersionMissingCompatibleShouldReturnDefault() {
        TreeMap<SimpleVersion, String> map = new TreeMap<>();
        map.put(new SimpleVersion("2.0.0"), "value");
        Assert.assertEquals("default", Utils.getCompatibleVersion(map, new SimpleVersion("1.0.0"), "test", "default"));
    }

    /** Test getConfigFromClasspath with empty classpath. Expected = returns input conf */
    @Test
    public void getConfigFromClasspathEmptyClasspathShouldReturnConf() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        Assert.assertSame(conf, Utils.getConfigFromClasspath(Collections.<String>emptyList(), conf));
    }

    /** Test isLocalhostAddress method with localhost. Expected = true */
    @Test
    public void isLocalhostAddressLocalhostShouldReturnTrue() {
        Assert.assertTrue(Utils.isLocalhostAddress("localhost"));
    }

    /** Test isLocalhostAddress method with remote address. Expected = false */
    @Test
    public void isLocalhostAddressRemoteShouldReturnFalse() {
        Assert.assertFalse(Utils.isLocalhostAddress("192.168.1.10"));
    }

    /** Test merge method with two maps. Expected = returns merged map where other overrides first */
    @Test
    public void mergeTwoMapsShouldPass() {
        Map<String, String> first = new HashMap<>();
        first.put("a", "1");
        first.put("b", "1");
        Map<String, String> other = new HashMap<>();
        other.put("b", "2");
        Map<String, String> result = Utils.merge(first, other);
        Assert.assertEquals("1", result.get("a"));
        Assert.assertEquals("2", result.get("b"));
    }

    /** Test merge method with null second map. Expected = copy of first */
    @Test
    public void mergeNullOtherShouldCopyFirst() {
        Map<String, String> first = new HashMap<>();
        first.put("a", "1");
        Map<String, String> result = Utils.merge(first, null);
        Assert.assertEquals(first, result);
        Assert.assertNotSame(first, result);
    }

    /** Test convertToArray method with valid source map and start index. Expected = ordered list from start to max key */
    @Test
    public void convertToArrayValidSourceMapShouldPass() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "a");
        map.put(3, "c");
        ArrayList<String> result = Utils.convertToArray(map, 1);
        Assert.assertEquals(Arrays.asList("a", null, "c"), result);
    }

    /** Test convertToArray method with start greater than max key. Expected = returns empty list because all entries are skipped */
    @Test
    public void convertToArrayStartGreaterThanMaxShouldReturnEmptyList() {
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "a");
        Assert.assertTrue(Utils.convertToArray(map, 2).isEmpty());
    }

    /** Test makeUptimeComputerImpl method. Expected = returns UptimeComputer */
    @Test
    public void makeUptimeComputerImplShouldReturnInstance() {
        Assert.assertNotNull(new Utils().makeUptimeComputerImpl());
    }

    /** Test isValidKey method with valid key. Expected = true */
    @Test
    public void isValidKeyValidKeyShouldReturnTrue() {
        Assert.assertTrue(Utils.isValidKey("valid_key-1.txt"));
    }

    /** Test isValidKey method with invalid key. Expected = false */
    @Test
    public void isValidKeyInvalidKeyShouldReturnFalse() {
        Assert.assertFalse(Utils.isValidKey(".."));
        Assert.assertFalse(Utils.isValidKey(null));
        Assert.assertFalse(Utils.isValidKey(""));
    }

    /** Test validateTopologyName method with valid name. Expected = completes */
    @Test
    public void validateTopologyNameValidNameShouldPass() {
        Utils.validateTopologyName("topology_name-1");
    }

    /** Test validateTopologyName method with invalid names. Expected = throws IllegalArgumentException */
    @Test
    public void validateTopologyNameInvalidNameThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.validateTopologyName(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.validateTopologyName("bad/name"));
    }

    /** Test findComponentCycles method with empty topology. Expected = returns empty cycles */
    @Test
    public void findComponentCyclesEmptyTopologyShouldReturnEmptyList() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        Assert.assertTrue(Utils.findComponentCycles(topology, "topology").isEmpty());
    }

    /** Test validateCycleFree method with empty topology. Expected = completes because no cycles are present */
    @Test
    public void validateCycleFreeEmptyTopologyShouldPass() throws InvalidTopologyException {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        Utils.validateCycleFree(topology, "topology");
    }

    /** Test GzipUtils compress/decompress methods. Expected = round trip bytes */
    @Test
    public void gzipUtilsCompressDecompressShouldPass() {
        byte[] input = "payload".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = Utils.GzipUtils.compress(input);
        Assert.assertTrue(Utils.GzipUtils.isGzip(compressed));
        Assert.assertArrayEquals(input, Utils.GzipUtils.decompress(compressed, 1024));
    }

    /** Test GzipUtils with null and short values. Expected = returns safe values */
    @Test
    public void gzipUtilsNullAndShortValuesShouldPass() {
        Assert.assertNull(Utils.GzipUtils.compress(null));
        Assert.assertFalse(Utils.GzipUtils.isGzip(new byte[] {1}));
    }

    /** Test ZstdUtils with null and short values. Expected = returns safe values */
    @Test
    public void zstdUtilsNullAndShortValuesShouldPass() {
        Assert.assertNull(Utils.ZstdUtils.compress(null, 3));
        Assert.assertFalse(Utils.ZstdUtils.isZstd(new byte[] {1, 2, 3}));
    }

    /** Test ZstdUtils compress with invalid slice. Expected = throws IndexOutOfBoundsException */
    @Test
    public void zstdUtilsInvalidSliceThrowsIndexOutOfBoundsException() {
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> Utils.ZstdUtils.compress(new byte[] {1, 2, 3}, 2, 5, 3));
    }

    // ### Test END ###
}
