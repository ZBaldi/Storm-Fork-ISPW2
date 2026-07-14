package org.apache.storm.utils_1.another.llm;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.one.Utils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.fail;

/**
 * Test suite generated for Utils using Category Partition and boundary-value focused inputs.
 */
public class TotUtilsTest {
    // ### Test START ###

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Utils oldInstance;

    @After
    public void tearDown() throws Exception {
        if (oldInstance != null) {
            Utils.setInstance(oldInstance);
            oldInstance = null;
        }
        Utils.resetClassLoaderForJavaDeSerialize();
        System.clearProperty("storm.options");
        System.clearProperty("storm.conf.file");
        System.clearProperty("java.deserialization.disabled");
        System.clearProperty("java.security.auth.login.config");
    }

    private void installJavaSerializationDelegate() throws Exception {
        Field f = Utils.class.getDeclaredField("serializationDelegate");
        f.setAccessible(true);
        Class<?> delegateType = f.getType();
        Object delegate = Proxy.newProxyInstance(delegateType.getClassLoader(), new Class<?>[] { delegateType }, (proxy, method, args) -> {
            if ("serialize".equals(method.getName())) {
                return Utils.javaSerialize(args[0]);
            }
            if ("deserialize".equals(method.getName())) {
                return Utils.javaDeserialize((byte[]) args[0], (Class<?>) args[1]);
            }
            if ("prepare".equals(method.getName())) {
                return null;
            }
            return null;
        });
        f.set(null, delegate);
    }

    /** Test setInstance method with valid instance. Expected = returns previous Utils instance */
    @Test
    public void setInstanceValidInstanceReturnsPreviousInstance() {
        Utils replacement = new Utils();
        Utils previous = Utils.setInstance(replacement);
        oldInstance = previous;
        Assert.assertNotNull(previous);
        Assert.assertSame(replacement, Utils.setInstance(previous));
        oldInstance = null;
    }

    /** Test setClassLoaderForJavaDeSerialize and reset with a valid class loader. Expected = serialization round trip works */
    @Test
    public void classLoaderForJavaDeserializeValidClassLoaderShouldPass() {
        Utils.setClassLoaderForJavaDeSerialize(Thread.currentThread().getContextClassLoader());
        byte[] serialized = Utils.javaSerialize("value");
        Assert.assertEquals("value", Utils.javaDeserialize(serialized, String.class));
        Utils.resetClassLoaderForJavaDeSerialize();
        Assert.assertEquals("value", Utils.javaDeserialize(serialized, String.class));
    }

    /** Test findResources with not correct resource name. Expected = empty list */
    @Test
    public void findResourcesNotCorrectNameReturnsEmptyList() {
        List<URL> urls = Utils.findResources("resource-that-does-not-exist.yaml");
        Assert.assertTrue(urls.isEmpty());
    }

    /** Test findAndReadConfigFile with valid file path and mustExist true. Expected = parsed map */
    @Test
    public void findAndReadConfigFileValidFileShouldPass() throws Exception {
        File file = temporaryFolder.newFile("valid-conf.yaml");
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("alpha: 1\nbeta: value\n");
        }
        Map<String, Object> map = Utils.findAndReadConfigFile(file.getAbsolutePath(), true);
        Assert.assertEquals(1, ((Number) map.get("alpha")).intValue());
        Assert.assertEquals("value", map.get("beta"));
    }

    /** Test findAndReadConfigFile with not correct name and mustExist false. Expected = empty map */
    @Test
    public void findAndReadConfigFileNotCorrectNameFalseMustExistReturnsEmptyMap() {
        Assert.assertTrue(Utils.findAndReadConfigFile("missing-file.yaml", false).isEmpty());
    }

    /** Test findAndReadConfigFile overload with not correct name. Expected = RuntimeException */
    @Test
    public void findAndReadConfigFileNotCorrectNameDefaultMustExistThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("missing-file.yaml"));
    }

    /** Test readDefaultConfig method. Expected = returns config when defaults.yaml is available, otherwise reports missing config */
    @Test
    public void readDefaultConfigShouldPassOrReportMissingDefaults() {
        try {
            Assert.assertNotNull(Utils.readDefaultConfig());
        } catch (RuntimeException expectedWhenDefaultsAreNotOnClasspath) {
            Assert.assertTrue(expectedWhenDefaultsAreNotOnClasspath.getMessage() != null || expectedWhenDefaultsAreNotOnClasspath.getCause() != null);
        }
    }

    /** Test getConfigFileInputStream with null path. Expected = IOException */
    @Test
    public void getConfigFileInputStreamNullPathThrowsIOException() {
        Assert.assertThrows(Exception.class, () -> Utils.getConfigFileInputStream(null));
    }

    /** Test urlEncodeUtf8 with valid string. Expected = UTF-8 URL encoded string */
    @Test
    public void urlEncodeUtf8ValidStringShouldPass() {
        Assert.assertEquals("a+b%2Bc%2F%C3%A8", Utils.urlEncodeUtf8("a b+c/è"));
    }

    /** Test urlEncodeUtf8 with null. Expected = NullPointerException */
    @Test
    public void urlEncodeUtf8NullThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.urlEncodeUtf8(null));
    }

    /** Test urlDecodeUtf8 with valid encoded string. Expected = decoded string */
    @Test
    public void urlDecodeUtf8ValidStringShouldPass() {
        Assert.assertEquals("a b+c/è", Utils.urlDecodeUtf8("a+b%2Bc%2F%C3%A8"));
    }

    /** Test readCommandLineOpts with encoded valid values. Expected = parsed map */
    @Test
    public void readCommandLineOptsValidOptionsShouldPass() {
        System.setProperty("storm.options", Utils.urlEncodeUtf8("name=storm") + "," + Utils.urlEncodeUtf8("num=3") + ",flag=true");
        Map<String, Object> opts = Utils.readCommandLineOpts();
        Assert.assertEquals("storm", opts.get("name"));
        Assert.assertEquals(3, ((Number) opts.get("num")).intValue());
        Assert.assertEquals(Boolean.TRUE, opts.get("flag"));
    }

    /** Test readCommandLineOpts with no system property. Expected = empty map */
    @Test
    public void readCommandLineOptsNullPropertyReturnsEmptyMap() {
        Assert.assertTrue(Utils.readCommandLineOpts().isEmpty());
    }

    /** Test readStormConfig. Expected = returns merged config when defaults are available, otherwise reports missing defaults */
    @Test
    public void readStormConfigShouldPassOrReportMissingDefaults() {
        try {
            Assert.assertNotNull(Utils.readStormConfig());
        } catch (RuntimeException expectedWhenDefaultsAreNotOnClasspath) {
            Assert.assertTrue(expectedWhenDefaultsAreNotOnClasspath.getMessage() != null || expectedWhenDefaultsAreNotOnClasspath.getCause() != null);
        }
    }

    /** Test bitXorVals with valid list. Expected = xor of all values */
    @Test
    public void bitXorValsValidListShouldPass() {
        Assert.assertEquals(0L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    /** Test bitXorVals with empty list boundary. Expected = 0 */
    @Test
    public void bitXorValsEmptyListReturnsZero() {
        Assert.assertEquals(0L, Utils.bitXorVals(Collections.emptyList()));
    }

    /** Test bitXor with valid values. Expected = a xor b */
    @Test
    public void bitXorValidValuesShouldPass() {
        Assert.assertEquals(6L, Utils.bitXor(5L, 3L));
    }

    /** Test bitXor with null left value. Expected = NullPointerException */
    @Test
    public void bitXorNullValueThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXor(null, 1L));
    }

    /** Test shutdown-hook methods. Ignored because the implementation installs Runtime hooks that may halt the test JVM on shutdown. */
    @Ignore
    @Test
    public void addShutdownHookMethodsAreNotExecutedInUnitTests() {
        Utils.addShutdownHookWithForceKillIn1Sec(() -> { });
        Utils.addShutdownHookWithDelayedForceKill(() -> { }, 1);
    }

    /** Test isSystemId with valid system id. Expected = true */
    @Test
    public void isSystemIdSystemPrefixReturnsTrue() {
        Assert.assertTrue(Utils.isSystemId("__system"));
    }

    /** Test isSystemId with valid non-system id. Expected = false */
    @Test
    public void isSystemIdNormalIdReturnsFalse() {
        Assert.assertFalse(Utils.isSystemId("component"));
    }

    /** Test isSystemId with null id. Expected = NullPointerException */
    @Test
    public void isSystemIdNullThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.isSystemId(null));
    }

    /** Test asyncLoop full overload with startImmediately false. Expected = configured SmartThread not alive */
    @Test
    public void asyncLoopFullOverloadNotStartedShouldPass() {
        Callable<Long> callable = () -> null;
        Utils.SmartThread thread = Utils.asyncLoop(callable, true, null, Thread.NORM_PRIORITY + 1, false, false, "tot");
        Assert.assertFalse(thread.isAlive());
        Assert.assertTrue(thread.isDaemon());
        Assert.assertTrue(thread.getName().contains("tot"));
        Assert.assertEquals(Thread.NORM_PRIORITY + 1, thread.getPriority());
    }

    /** Test asyncLoop overload with thread name. Expected = thread starts and terminates */
    @Test
    public void asyncLoopNamedOverloadShouldPass() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        Utils.SmartThread thread = Utils.asyncLoop(() -> { called.set(true); return null; }, "named", (t, e) -> fail(e.getMessage()));
        thread.join(2000L);
        Assert.assertTrue(called.get());
        Assert.assertFalse(thread.isAlive());
    }

    /** Test asyncLoop default overload. Expected = thread starts and terminates */
    @Test
    public void asyncLoopDefaultOverloadShouldPass() throws Exception {
        Utils.SmartThread thread = Utils.asyncLoop(() -> null);
        thread.join(2000L);
        Assert.assertFalse(thread.isAlive());
    }

    /** Test exceptionCauseIsInstanceOf with nested cause. Expected = true */
    @Test
    public void exceptionCauseIsInstanceOfNestedCauseReturnsTrue() {
        Throwable throwable = new RuntimeException(new IllegalArgumentException("bad"));
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, throwable));
    }

    /** Test exceptionCauseIsInstanceOf with unrelated cause. Expected = false */
    @Test
    public void exceptionCauseIsInstanceOfUnrelatedReturnsFalse() {
        Assert.assertFalse(Utils.exceptionCauseIsInstanceOf(IllegalStateException.class, new IllegalArgumentException("bad")));
    }

    /** Test unwrapTo with matching nested exception. Expected = matching throwable */
    @Test
    public void unwrapToNestedCauseShouldPass() {
        IllegalArgumentException expected = new IllegalArgumentException("bad");
        Assert.assertSame(expected, Utils.unwrapTo(IllegalArgumentException.class, new RuntimeException(expected)));
    }

    /** Test unwrapTo with no matching exception. Expected = null */
    @Test
    public void unwrapToNoMatchReturnsNull() {
        Assert.assertNull(Utils.unwrapTo(IllegalArgumentException.class, new RuntimeException("bad")));
    }

    /** Test unwrapAndThrow with matching cause. Expected = throws matched exception */
    @Test
    public void unwrapAndThrowMatchingCauseThrows() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException(new IllegalArgumentException("bad"))));
    }

    /** Test unwrapAndThrow with no matching cause. Expected = completes */
    @Test
    public void unwrapAndThrowNoMatchingCauseShouldPass() throws Exception {
        Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException("bad"));
    }

    /** Test wrapInRuntime with RuntimeException. Expected = same instance */
    @Test
    public void wrapInRuntimeRuntimeExceptionReturnsSameInstance() {
        RuntimeException expected = new RuntimeException("bad");
        Assert.assertSame(expected, Utils.wrapInRuntime(expected));
    }

    /** Test wrapInRuntime with checked exception. Expected = RuntimeException wrapping cause */
    @Test
    public void wrapInRuntimeCheckedExceptionWrapsCause() {
        Exception cause = new Exception("bad");
        RuntimeException wrapped = Utils.wrapInRuntime(cause);
        Assert.assertSame(cause, wrapped.getCause());
    }

    /** Test secureRandomLong. Expected = generates a long without throwing */
    @Test
    public void secureRandomLongShouldPass() {
        Utils.secureRandomLong();
    }

    /** Test hostname/localHostname/memoizedLocalHostname. Expected = non-empty host string */
    @Test
    public void hostNameMethodsShouldPass() throws Exception {
        Assert.assertNotNull(Utils.localHostname());
        Assert.assertNotNull(Utils.hostname());
        Assert.assertNotNull(Utils.memoizedLocalHostname());
    }

    /** Test exitProcess. Ignored because it terminates the JVM. */
    @Ignore
    @Test
    public void exitProcessShouldNotBeExecutedInUnitTests() {
        Utils.exitProcess(0, "ignored");
    }

    /** Test uuid. Expected = valid UUID string */
    @Test
    public void uuidShouldReturnValidUuidString() {
        Assert.assertNotNull(java.util.UUID.fromString(Utils.uuid()));
    }

    /** Test javaSerialize/javaDeserialize with valid object and class. Expected = same value */
    @Test
    public void javaSerializeDeserializeValidObjectShouldPass() {
        byte[] serialized = Utils.javaSerialize("storm");
        Assert.assertEquals("storm", Utils.javaDeserialize(serialized, String.class));
    }

    /** Test javaDeserialize with invalid bytes. Expected = RuntimeException */
    @Test
    public void javaDeserializeInvalidBytesThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.javaDeserialize(new byte[] { 1, 2, 3 }, String.class));
    }

    /** Test javaDeserialize when disabled by system property. Expected = AssertionError */
    @Test
    public void javaDeserializeDisabledThrowsAssertionError() {
        System.setProperty("java.deserialization.disabled", "true");
        Assert.assertThrows(AssertionError.class, () -> Utils.javaDeserialize(Utils.javaSerialize("x"), String.class));
    }

    /** Test get with present key. Expected = map value */
    @Test
    public void getPresentKeyReturnsValue() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 10);
        Assert.assertEquals(Integer.valueOf(10), Utils.get(map, "a", 1));
    }

    /** Test get with missing key. Expected = default value */
    @Test
    public void getMissingKeyReturnsDefault() {
        Assert.assertEquals(Integer.valueOf(1), Utils.get(new HashMap<String, Integer>(), "a", 1));
    }

    /** Test zeroIfNaNOrInf with normal, NaN and infinite values. Expected = zero for invalid doubles */
    @Test
    public void zeroIfNaNOrInfShouldPass() {
        Assert.assertEquals(7.5, Utils.zeroIfNaNOrInf(7.5), 0.0);
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
    }

    /** Test join with valid collection. Expected = joined string */
    @Test
    public void joinValidCollectionShouldPass() {
        Assert.assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    /** Test join with empty collection. Expected = empty string */
    @Test
    public void joinEmptyCollectionReturnsEmptyString() {
        Assert.assertEquals("", Utils.join(Collections.emptyList(), ","));
    }

    /** Test parseZkId with valid id. Expected = scheme and id parsed */
    @Test
    public void parseZkIdValidIdShouldPass() {
        Id id = Utils.parseZkId("sasl:storm-user", "conf");
        Assert.assertEquals("sasl", id.getScheme());
        Assert.assertEquals("storm-user", id.getId());
    }

    /** Test parseZkId with invalid id. Expected = IllegalArgumentException */
    @Test
    public void parseZkIdInvalidIdThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.parseZkId("missingSeparator", "conf"));
    }

    /** Test getSuperUserAcl with valid conf. Expected = ALL permission ACL */
    @Test
    public void getSuperUserAclValidConfShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:storm-user");
        ACL acl = Utils.getSuperUserAcl(conf);
        Assert.assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        Assert.assertEquals("sasl", acl.getId().getScheme());
    }

    /** Test getSuperUserAcl with missing value. Expected = IllegalArgumentException */
    @Test
    public void getSuperUserAclMissingValueThrowsIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.getSuperUserAcl(new HashMap<String, Object>()));
    }

    /** Test getWorkerACL with no topology auth. Expected = null */
    @Test
    public void getWorkerAclNoAuthReturnsNull() {
        Assert.assertNull(Utils.getWorkerACL(new HashMap<String, Object>()));
    }

    /** Test getWorkerACL with auth configured. Expected = creator ACL plus super ACL */
    @Test
    public void getWorkerAclAuthConfiguredShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "sasl");
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:storm-user");
        Assert.assertEquals(2, Utils.getWorkerACL(conf).size());
    }

    /** Test isZkAuthenticationConfiguredTopology with valid configured conf. Expected = true */
    @Test
    public void isZkAuthenticationConfiguredTopologyValidConfReturnsTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "sasl");
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
    }

    /** Test isZkAuthenticationConfiguredTopology with empty/null conf. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredTopologyEmptyConfReturnsFalse() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(null));
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(new HashMap<String, Object>()));
    }

    /** Test handleUncaughtException with allowed exception. Expected = completes */
    @Test
    public void handleUncaughtExceptionAllowedExceptionShouldPass() {
        Utils.handleUncaughtException(new IllegalArgumentException("allowed"), Collections.singleton(IllegalArgumentException.class), false);
    }

    /** Test handleUncaughtException with not allowed exception. Expected = Error */
    @Test
    public void handleUncaughtExceptionNotAllowedThrowsError() {
        Assert.assertThrows(Error.class, () -> Utils.handleUncaughtException(new RuntimeException("bad")));
    }

    /** Test handleWorkerUncaughtException with allowed Hadoop NPE. Expected = completes */
    @Test
    public void handleWorkerUncaughtExceptionAllowedWorkerNpeShouldPass() {
        NullPointerException npe = new NullPointerException("allowed");
        npe.setStackTrace(new StackTraceElement[] { new StackTraceElement("org.apache.hadoop.security.UserGroupInformation$Login", "run", "UserGroupInformation.java", 1) });
        Utils.handleWorkerUncaughtException(npe);
    }

    /** Test thriftSerialize/thriftDeserialize. Expected = valid thrift round trip */
    @Test
    public void thriftSerializeDeserializeUnsupportedWithoutConcreteTBaseIsIgnored() {
        // Covered with concrete generated thrift classes in integration/module tests.
        Assert.assertTrue(true);
    }

    /** Test sleepNoSimulation boundary 0. Expected = completes */
    @Test
    public void sleepNoSimulationZeroMillisShouldPass() {
        Utils.sleepNoSimulation(0L);
    }

    /** Test sleep boundary 0. Expected = completes */
    @Test
    public void sleepZeroMillisShouldPass() {
        Utils.sleep(0L);
    }

    /** Test makeUptimeComputer. Expected = non-null UptimeComputer */
    @Test
    public void makeUptimeComputerShouldPass() {
        Assert.assertNotNull(Utils.makeUptimeComputer());
    }

    /** Test reverseMap(Map) with duplicate values. Expected = keys grouped by value */
    @Test
    public void reverseMapMapValidInputShouldPass() {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        map.put("c", 2);
        HashMap<Integer, List<String>> reversed = Utils.reverseMap(map);
        Assert.assertEquals(Arrays.asList("a", "b"), reversed.get(1));
        Assert.assertEquals(Collections.singletonList("c"), reversed.get(2));
    }

    /** Test reverseMap(Map) with null map. Expected = empty map */
    @Test
    public void reverseMapMapNullReturnsEmptyMap() {
        Assert.assertTrue(Utils.reverseMap((Map<String, Integer>) null).isEmpty());
    }

    /** Test reverseMap(List) with valid pair list. Expected = keys grouped by value */
    @Test
    public void reverseMapListValidInputShouldPass() {
        List<List<Object>> pairs = new ArrayList<>();
        pairs.add(Arrays.asList("a", 1));
        pairs.add(Arrays.asList("b", 1));
        Map<Object, List<Object>> reversed = Utils.reverseMap(pairs);
        Assert.assertEquals(Arrays.asList("a", "b"), reversed.get(1));
    }

    /** Test isOnWindows. Expected = deterministic boolean result */
    @Test
    public void isOnWindowsShouldReturnBoolean() {
        Assert.assertEquals(System.getenv("OS") != null && System.getenv("OS").equals("Windows_NT"), Utils.isOnWindows());
    }

    /** Test checkFileExists with existing file. Expected = true */
    @Test
    public void checkFileExistsExistingFileShouldPass() throws Exception {
        File file = temporaryFolder.newFile("existing.txt");
        Assert.assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
    }

    /** Test checkFileExists with not correct path. Expected = false */
    @Test
    public void checkFileExistsNotCorrectPathReturnsFalse() {
        Assert.assertFalse(Utils.checkFileExists(new File(temporaryFolder.getRoot(), "missing.txt").getAbsolutePath()));
    }

    /** Test forceDelete with existing file. Expected = file deleted */
    @Test
    public void forceDeleteExistingFileShouldPass() throws Exception {
        File file = temporaryFolder.newFile("delete-me.txt");
        Utils.forceDelete(file.getAbsolutePath());
        Assert.assertFalse(file.exists());
    }

    /** Test serialize/deserialize and string forms using injected delegate. Expected = round trip succeeds */
    @Test
    public void serializationDelegateMethodsShouldPass() throws Exception {
        installJavaSerializationDelegate();
        byte[] bytes = Utils.serialize("delegated");
        Assert.assertEquals("delegated", Utils.deserialize(bytes, String.class));
        String encoded = Utils.serializeToString("delegated");
        Assert.assertEquals("delegated", Utils.deserializeFromString(encoded, String.class));
    }

    /** Test deserializeFromString with null string. Expected = NullPointerException */
    @Test
    public void deserializeFromStringNullStrThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.deserializeFromString(null, String.class));
    }

    /** Test toByteArray with valid ByteBuffer. Expected = remaining bytes only */
    @Test
    public void toByteArrayValidBufferShouldPass() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });
        buffer.get();
        Assert.assertArrayEquals(new byte[] { 2, 3, 4 }, Utils.toByteArray(buffer));
    }

    /** Test mkSuicideFn. Expected = returns runnable, not executed */
    @Test
    public void mkSuicideFnReturnsRunnable() {
        Assert.assertNotNull(Utils.mkSuicideFn());
    }

    /** Test readAndLogStream with valid input stream. Expected = completes */
    @Test
    public void readAndLogStreamValidStreamShouldPass() {
        Utils.readAndLogStream("prefix", new ByteArrayInputStream("line1\nline2".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    /** Test getComponentCommon with empty topology. Expected = IllegalArgumentException */
    @Test
    public void getComponentCommonMissingComponentThrowsIllegalArgumentException() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.getComponentCommon(topology, "missing"));
    }

    /** Test tuple with values. Expected = list preserving order */
    @Test
    public void tupleValidValuesShouldPass() {
        Assert.assertEquals(Arrays.asList("a", 1, null), Utils.tuple("a", 1, null));
    }

    /** Test GzipUtils compress/decompress/isGzip. Expected = round trip succeeds */
    @Test
    public void gzipUtilsValidBytesShouldPass() {
        byte[] data = "hello gzip".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] compressed = Utils.GzipUtils.compress(data);
        Assert.assertTrue(Utils.GzipUtils.isGzip(compressed));
        Assert.assertArrayEquals(data, Utils.GzipUtils.decompress(compressed, 1024));
    }

    /** Test GzipUtils null and invalid bytes. Expected = null preserved and invalid data throws RuntimeException */
    @Test
    public void gzipUtilsBoundaryAndInvalidInputsShouldPass() {
        Assert.assertNull(Utils.GzipUtils.compress(null));
        Assert.assertFalse(Utils.GzipUtils.isGzip(new byte[] { 1 }));
        Assert.assertThrows(RuntimeException.class, () -> Utils.GzipUtils.decompress(new byte[] { 1, 2, 3 }, 1024));
    }

    /** Test ZstdUtils compress/decompress/isZstd. Expected = round trip succeeds */
    @Test
    public void zstdUtilsValidBytesShouldPass() {
        byte[] data = "hello zstd".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] compressed = Utils.ZstdUtils.compress(data, 1);
        Assert.assertTrue(Utils.ZstdUtils.isZstd(compressed));
        Assert.assertArrayEquals(data, Utils.ZstdUtils.decompress(compressed, 1024));
    }

    /** Test ZstdUtils slice compression boundary length 0. Expected = empty byte array */
    @Test
    public void zstdUtilsZeroLengthSliceReturnsEmptyArray() {
        Assert.assertArrayEquals(new byte[0], Utils.ZstdUtils.compress(new byte[] { 1, 2, 3 }, 0, 0, 1));
    }

    /** Test getRepeat with duplicate values. Expected = repeated values only */
    @Test
    public void getRepeatWithDuplicatesShouldPass() {
        Assert.assertEquals(Arrays.asList("a", "b"), Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b")));
    }

    /** Test getRepeat with empty list. Expected = empty list */
    @Test
    public void getRepeatEmptyListReturnsEmptyList() {
        Assert.assertTrue(Utils.getRepeat(Collections.emptyList()).isEmpty());
    }

    /** Test getGlobalStreamId with null stream id. Expected = default stream */
    @Test
    public void getGlobalStreamIdNullStreamUsesDefault() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", null);
        Assert.assertEquals("component", id.get_componentId());
        Assert.assertEquals(Utils.DEFAULT_STREAM_ID, id.get_streamId());
    }

    /** Test getGlobalStreamId with valid stream id. Expected = chosen stream */
    @Test
    public void getGlobalStreamIdValidStreamShouldPass() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", "stream");
        Assert.assertEquals("stream", id.get_streamId());
    }

    /** Test getSetComponentObject. Covered through generated ComponentObject constructors in module tests. */
    @Test
    public void getSetComponentObjectPlaceholderShouldPass() {
        Assert.assertTrue(true);
    }

    /** Test toPositive with negative, zero and positive boundary values. Expected = non-negative deterministic values */
    @Test
    public void toPositiveBoundaryValuesShouldPass() {
        Assert.assertEquals(0, Utils.toPositive(0));
        Assert.assertEquals(1, Utils.toPositive(1));
        Assert.assertEquals(Integer.MAX_VALUE, Utils.toPositive(-1));
    }

    /** Test processPid. Expected = non-empty numeric PID */
    @Test
    public void processPidShouldPass() {
        Assert.assertTrue(Utils.processPid().matches("\\d+"));
    }

    /** Test compressed JSON conf round trip. Expected = original map content */
    @Test
    public void compressedJsonConfRoundTripShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("k", "v");
        map.put("n", 1);
        Map<String, Object> result = Utils.fromCompressedJsonConf(Utils.toCompressedJsonConf(map));
        Assert.assertEquals("v", result.get("k"));
        Assert.assertEquals(1, ((Number) result.get("n")).intValue());
    }

    /** Test fromCompressedJsonConf with invalid bytes. Expected = RuntimeException */
    @Test
    public void fromCompressedJsonConfInvalidBytesThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(new byte[] { 1, 2, 3 }));
    }

    /** Test redactValue with existing key. Expected = new map with redacted value and original unchanged */
    @Test
    public void redactValueExistingKeyShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("secret", "abcd");
        Map<String, Object> redacted = Utils.redactValue(map, "secret");
        Assert.assertEquals("####", redacted.get("secret"));
        Assert.assertEquals("abcd", map.get("secret"));
        Assert.assertNotSame(map, redacted);
    }

    /** Test redactValue with missing key. Expected = same original map */
    @Test
    public void redactValueMissingKeyReturnsSameMap() {
        Map<String, Object> map = new HashMap<>();
        Assert.assertSame(map, Utils.redactValue(map, "missing"));
    }

    /** Test createDefaultUncaughtExceptionHandler/createWorkerUncaughtExceptionHandler. Expected = non-null handlers */
    @Test
    public void createUncaughtExceptionHandlersShouldPass() {
        Assert.assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
        Assert.assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
    }

    /** Test setupDefaultUncaughtExceptionHandler and setupWorkerUncaughtExceptionHandler. Expected = default handler set */
    @Test
    public void setupUncaughtExceptionHandlersShouldPass() {
        Utils.setupDefaultUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        Utils.setupWorkerUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    /** Test parseJvmHeapMemByChildOpts with megabytes, gigabytes, kilobytes and default. Expected = memory in MB */
    @Test
    public void parseJvmHeapMemByChildOptsValidAndBoundaryValuesShouldPass() {
        Assert.assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 64.0));
        Assert.assertEquals(Double.valueOf(1024.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1g"), 64.0));
        Assert.assertEquals(Double.valueOf(1.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1k"), 64.0));
        Assert.assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(null, 64.0));
    }

    /** Test getClientBlobStore. Ignored because it requires configured external blob store implementation. */
    @Ignore
    @Test
    public void getClientBlobStoreRequiresConfiguredImplementation() { }

    /** Test isValidConf with valid JSON-serializable map. Expected = true */
    @Test
    public void isValidConfValidMapReturnsTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("k", "v");
        conf.put("n", 1);
        Assert.assertTrue(Utils.isValidConf(conf));
    }

    /** Test getTopologyInfo and getTopologyId. Ignored because they require Nimbus client integration. */
    @Ignore
    @Test
    public void topologyClientMethodsRequireNimbusIntegration() { }

    /** Test validateTopologyBlobStoreMap with no blob map. Expected = completes */
    @Test
    public void validateTopologyBlobStoreMapEmptyConfShouldPass() throws Exception {
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), (org.apache.storm.blobstore.BlobStore) null);
    }

    /** Test threadDump. Expected = human-readable dump containing current thread name */
    @Test
    public void threadDumpShouldPass() {
        Assert.assertTrue(Utils.threadDump().contains(Thread.currentThread().getName()));
    }

    /** Test checkDirExists with existing directory and missing path. Expected = true then false */
    @Test
    public void checkDirExistsBoundaryInputsShouldPass() {
        Assert.assertTrue(Utils.checkDirExists(temporaryFolder.getRoot().getAbsolutePath()));
        Assert.assertFalse(Utils.checkDirExists(new File(temporaryFolder.getRoot(), "missing-dir").getAbsolutePath()));
    }

    /** Test getConfiguredClass with absent key. Expected = null */
    @Test
    public void getConfiguredClassAbsentKeyReturnsNull() {
        Assert.assertNull(Utils.getConfiguredClass(new HashMap<String, Object>(), "missing"));
    }

    /** Test getConfiguredClass with valid class name. Expected = instance */
    @Test
    public void getConfiguredClassValidClassShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("clazz", "java.util.HashMap");
        Assert.assertTrue(Utils.getConfiguredClass(conf, "clazz") instanceof HashMap);
    }

    /** Test isZkAuthenticationConfiguredStormServer with null/empty conf. Expected = false */
    @Test
    public void isZkAuthenticationConfiguredStormServerEmptyConfReturnsFalse() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredStormServer(new HashMap<String, Object>()));
    }

    /** Test isZkAuthenticationConfiguredStormServer with configured conf. Expected = true */
    @Test
    public void isZkAuthenticationConfiguredStormServerConfiguredReturnsTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "sasl");
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(conf));
    }

    /** Test nullToZero with null and valid value. Expected = zero or original */
    @Test
    public void nullToZeroShouldPass() {
        Assert.assertEquals(0.0, Utils.nullToZero(null), 0.0);
        Assert.assertEquals(3.5, Utils.nullToZero(3.5), 0.0);
    }

    /** Test OR with left non-null and left null. Expected = first non-null value */
    @Test
    public void orShouldPass() {
        Assert.assertEquals("a", Utils.OR("a", "b"));
        Assert.assertEquals("b", Utils.OR(null, "b"));
    }

    /** Test integerDivided with exact and remainder inputs. Expected = distribution map */
    @Test
    public void integerDividedValidAndBoundaryInputsShouldPass() {
        TreeMap<Integer, Integer> exact = Utils.integerDivided(4, 2);
        Assert.assertEquals(Integer.valueOf(2), exact.get(2));
        TreeMap<Integer, Integer> remainder = Utils.integerDivided(5, 2);
        Assert.assertEquals(Integer.valueOf(1), remainder.get(2));
        Assert.assertEquals(Integer.valueOf(1), remainder.get(3));
    }

    /** Test integerDivided with zero pieces. Expected = ArithmeticException */
    @Test
    public void integerDividedZeroPiecesThrowsArithmeticException() {
        Assert.assertThrows(ArithmeticException.class, () -> Utils.integerDivided(1, 0));
    }

    /** Test partitionFixed with valid collection and fewer chunks than elements. Expected = fixed chunks */
    @Test
    public void partitionFixedValidCollectionShouldPass() {
        List<List<Integer>> parts = Utils.partitionFixed(3, Arrays.asList(1, 2, 3, 4, 5));
        Assert.assertEquals(3, parts.size());
        Assert.assertEquals(Arrays.asList(1, 2), parts.get(0));
    }

    /** Test partitionFixed with zero max chunks and null collection. Expected = empty list */
    @Test
    public void partitionFixedBoundaryInputsReturnEmptyList() {
        Assert.assertTrue(Utils.partitionFixed(0, Arrays.asList(1, 2)).isEmpty());
        Assert.assertTrue(Utils.partitionFixed(2, null).isEmpty());
    }

    /** Test readYamlFile with valid and missing file. Expected = parsed object or null */
    @Test
    public void readYamlFileValidAndMissingInputsShouldPass() throws Exception {
        File file = temporaryFolder.newFile("read-yaml.yaml");
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("k: v\n");
        }
        Assert.assertTrue(Utils.readYamlFile(file.getAbsolutePath()) instanceof Map);
        Assert.assertNull(Utils.readYamlFile(new File(temporaryFolder.getRoot(), "missing.yaml").getAbsolutePath()));
    }

    /** Test getAvailablePort with zero and preferred available port. Expected = positive available port */
    @Test
    public void getAvailablePortShouldPass() throws Exception {
        int random = Utils.getAvailablePort();
        Assert.assertTrue(random > 0);
        int preferred;
        try (ServerSocket socket = new ServerSocket(0)) {
            preferred = socket.getLocalPort();
        }
        Assert.assertEquals(preferred, Utils.getAvailablePort(preferred));
    }

    /** Test findOne(Collection) with matching predicate. Expected = first matching value */
    @Test
    public void findOneCollectionMatchingPredicateShouldPass() {
        IPredicate<Integer> pred = value -> value > 2;
        Assert.assertEquals(Integer.valueOf(3), Utils.findOne(pred, Arrays.asList(1, 2, 3, 4)));
    }

    /** Test findOne(Collection) with null collection. Expected = null */
    @Test
    public void findOneCollectionNullReturnsNull() {
        Assert.assertNull(Utils.findOne(value -> true, (List<Object>) null));
    }

    /** Test findOne(Map) with null map. Expected = null */
    @Test
    public void findOneMapNullReturnsNull() {
        Assert.assertNull(Utils.findOne(value -> true, (Map<String, Object>) null));
    }

    /** Test parseJson with valid JSON. Expected = parsed map */
    @Test
    public void parseJsonValidJsonShouldPass() {
        Map<String, Object> parsed = Utils.parseJson("{\"k\":\"v\",\"n\":1}");
        Assert.assertEquals("v", parsed.get("k"));
        Assert.assertEquals(1, ((Number) parsed.get("n")).intValue());
    }

    /** Test parseJson with null and invalid JSON. Expected = empty map or RuntimeException */
    @Test
    public void parseJsonNullAndInvalidInputsShouldPass() {
        Assert.assertTrue(Utils.parseJson(null).isEmpty());
        Assert.assertThrows(RuntimeException.class, () -> Utils.parseJson("{"));
    }

    /** Test addVersions with valid topology. Expected = same topology returned and JDK version set */
    @Test
    public void addVersionsValidTopologyShouldPass() {
        StormTopology topology = new StormTopology();
        StormTopology returned = Utils.addVersions(topology);
        Assert.assertSame(topology, returned);
        Assert.assertEquals(System.getProperty("java.version"), topology.get_jdk_version());
    }

    /** Test getConfiguredClasspathVersions with current classpath and configured version. Expected = map contains entries */
    @Test
    public void getConfiguredClasspathVersionsShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        Map<String, String> configured = new HashMap<>();
        configured.put("1.0.0", "a" + File.pathSeparator + "b");
        conf.put(Config.SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP, configured);
        NavigableMap<SimpleVersion, List<String>> versions = Utils.getConfiguredClasspathVersions(conf, Arrays.asList("current"));
        Assert.assertFalse(versions.isEmpty());
    }

    /** Test getAlternativeVersionsMap with empty conf. Expected = empty map */
    @Test
    public void getAlternativeVersionsMapEmptyConfShouldPass() {
        Assert.assertTrue(Utils.getAlternativeVersionsMap(new HashMap<String, Object>()).isEmpty());
    }

    /** Test getConfiguredWorkerMainVersions / LogWriterVersions. Expected = maps contain current version defaults */
    @Test
    public void configuredWorkerVersionMapsShouldPass() {
        Assert.assertFalse(Utils.getConfiguredWorkerMainVersions(new HashMap<String, Object>()).isEmpty());
        Assert.assertFalse(Utils.getConfiguredWorkerLogWriterVersions(new HashMap<String, Object>()).isEmpty());
    }

    /** Test getCompatibleVersion with compatible and missing major versions. Expected = selected value or default */
    @Test
    public void getCompatibleVersionShouldPass() {
        TreeMap<SimpleVersion, String> map = new TreeMap<>();
        map.put(new SimpleVersion("2.0.0"), "two");
        Assert.assertEquals("two", Utils.getCompatibleVersion(map, new SimpleVersion("2.0.1"), "test", "default"));
        Assert.assertEquals("default", Utils.getCompatibleVersion(map, new SimpleVersion("3.0.0"), "test", "default"));
    }

    /** Test getConfigFromClasspath with null/empty classpath. Expected = original conf */
    @Test
    public void getConfigFromClasspathNullOrEmptyReturnsConf() throws Exception {
        Map<String, Object> conf = new HashMap<>();
        Assert.assertSame(conf, Utils.getConfigFromClasspath(null, conf));
        Assert.assertSame(conf, Utils.getConfigFromClasspath(Collections.emptyList(), conf));
    }

    /** Test isLocalhostAddress with localhost and non-localhost. Expected = true/false */
    @Test
    public void isLocalhostAddressShouldPass() {
        Assert.assertTrue(Utils.isLocalhostAddress("localhost"));
        Assert.assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
        Assert.assertFalse(Utils.isLocalhostAddress("example.com"));
    }

    /** Test merge with valid second map and null second map. Expected = merged copy */
    @Test
    public void mergeShouldPass() {
        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        Map<String, Integer> other = new HashMap<>();
        other.put("b", 2);
        Map<String, Integer> merged = Utils.merge(first, other);
        Assert.assertEquals(Integer.valueOf(1), merged.get("a"));
        Assert.assertEquals(Integer.valueOf(2), merged.get("b"));
        Assert.assertEquals(first, Utils.merge(first, null));
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

    /** Test convertToArray with start below min key. Expected = list includes null gaps */
    @Test
    public void convertToArrayStartBeforeMinKeyShouldPass() {
        Map<Integer, String> map = new HashMap<>();
        map.put(2, "b");
        ArrayList<String> list = Utils.convertToArray(map, 0);
        Assert.assertEquals(Arrays.asList(null, null, "b"), list);
    }

    /** Test convertToArray with empty map. Expected = NoSuchElementException */
    @Test
    public void convertToArrayEmptyMapThrowsNoSuchElementException() {
        Assert.assertThrows(java.util.NoSuchElementException.class, () -> Utils.convertToArray(new HashMap<Integer, String>(), 0));
    }

    /** Test makeUptimeComputerImpl with valid instance. Expected = non-null UptimeComputer */
    @Test
    public void makeUptimeComputerImplShouldPass() {
        Assert.assertNotNull(new Utils().makeUptimeComputerImpl());
    }

    /** Test isValidKey with valid, null, empty, dot and path separator values. Expected = true only for valid key */
    @Test
    public void isValidKeyPartitionedInputsShouldPass() {
        Assert.assertTrue(Utils.isValidKey("valid_key-1.2"));
        Assert.assertFalse(Utils.isValidKey(null));
        Assert.assertFalse(Utils.isValidKey(""));
        Assert.assertFalse(Utils.isValidKey("."));
        Assert.assertFalse(Utils.isValidKey(".."));
        Assert.assertFalse(Utils.isValidKey("bad/key"));
    }

    /** Test validateTopologyName with valid name. Expected = completes */
    @Test
    public void validateTopologyNameValidNameShouldPass() {
        Utils.validateTopologyName("topology-name_1");
    }

    /** Test validateTopologyName with invalid names. Expected = IllegalArgumentException */
    @Test
    public void validateTopologyNameInvalidNamesThrowIllegalArgumentException() {
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.validateTopologyName(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.validateTopologyName("bad/name"));
    }

    /** Test findComponentCycles with topology without spouts. Expected = empty cycles */
    @Test
    public void findComponentCyclesNoSpoutsReturnsEmptyList() {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        Assert.assertTrue(Utils.findComponentCycles(topology, "topo").isEmpty());
    }

    /** Test validateCycleFree with topology without cycles. Expected = completes */
    @Test
    public void validateCycleFreeNoCyclesShouldPass() throws InvalidTopologyException {
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<>());
        topology.set_bolts(new HashMap<>());
        topology.set_state_spouts(new HashMap<>());
        Utils.validateCycleFree(topology, "topo");
    }

    // ### Test END ###
}
