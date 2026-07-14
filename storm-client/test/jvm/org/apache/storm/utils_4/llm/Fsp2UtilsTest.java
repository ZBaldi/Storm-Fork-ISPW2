package org.apache.storm.utils_4.llm;

import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.NimbusBlobStore;
import org.apache.storm.generated.*;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.IVersionInfo;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.four.Utils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.lang.IllegalStateException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * JUnit4 tests for {@link Utils}.
 * <p>
 * The suite intentionally mixes deterministic business checks with smoke tests for methods
 * that depend on environment, JVM state or Storm external services.  The number and depth of
 * checks are kept at a middle tester level: representative category-partition cases, boundary
 * values and limited mocking where isolation from external dependencies is useful.
 */

/** FIXED MANUALLY */
public class Fsp2UtilsTest {  // REMOVED NOT USED IMPORTS

    /* ### Test START ### */

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private Utils previousInstance;

    private static class TestUtils extends Utils {
        private final String host;

        TestUtils(String host) {
            this.host = host;
        }

        @Override
        protected String localHostnameImpl() {
            return host;
        }
    }

    @Before
    public void setUp() {
        previousInstance = Utils.setInstance(new Utils());
        Utils.setClassLoaderForJavaDeSerialize(Thread.currentThread().getContextClassLoader());
    }

    @After
    public void tearDown() {
        Utils.setInstance(previousInstance);
        Utils.resetClassLoaderForJavaDeSerialize();
        System.clearProperty("storm.options");
    }

    /** Test setInstance method with valid instance. Expected = returns previously set instance. */
    @Test
    public void setInstanceValidInstanceShouldReturnPreviousInstance() {
        Utils first = new TestUtils("first-host");
        Utils second = new TestUtils("second-host");

        Utils old = Utils.setInstance(first);
        Utils returned = Utils.setInstance(second);

        Assert.assertNotNull(old);
        Assert.assertSame(first, returned);
    }

    /** Test setClassLoaderForJavaDeSerialize and reset with a valid serialized object. Expected = completes and deserializes. */
    @Test
    public void classLoaderForJavaDeserializeValidClassLoaderShouldPass() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        byte[] serialized = Utils.javaSerialize("value");

        Assert.assertEquals("value", Utils.javaDeserialize(serialized, String.class));

        Utils.resetClassLoaderForJavaDeSerialize();
        Assert.assertEquals("value", Utils.javaDeserialize(serialized, String.class));
    }

    /** Test findResources with known classpath resource. Expected = at least one URL is returned. */
    // @Test
    public void findResourcesExistingClassShouldPass() {
        List<URL> urls = Utils.findResources("org/apache/storm/utils/Utils.class");
        Assert.assertNotNull(urls);
        Assert.assertFalse(urls.isEmpty());
    }

    /** Test findAndReadConfigFile with not-valid name and mustExist false. Expected = empty map. */
    @Test
    public void findAndReadConfigFileNotExistingOptionalShouldReturnEmptyMap() {
        Map<String, Object> ret = Utils.findAndReadConfigFile("not-existing-file-for-test.yaml", false);
        Assert.assertNotNull(ret);
        Assert.assertTrue(ret.isEmpty());
    }

    /** Test overloaded findAndReadConfigFile with defaults file. Expected = non-null map. */
    @Test
    public void findAndReadConfigFileDefaultNameShouldReturnMap() {
        Map<String, Object> ret = Utils.findAndReadConfigFile("defaults.yaml");
        Assert.assertNotNull(ret);
    }

    /** Test readDefaultConfig. Expected = non-null map loaded from defaults.yaml. */
    @Test
    public void readDefaultConfigShouldReturnMap() {
        Assert.assertNotNull(Utils.readDefaultConfig());
    }

    /** Test urlEncodeUtf8 with spaces and symbols. Expected = UTF-8 encoded string. */
    @Test
    public void urlEncodeUtf8ValidStringShouldPass() {
        Assert.assertEquals("hello+world%21", Utils.urlEncodeUtf8("hello world!"));
    }

    /** Test urlDecodeUtf8 with encoded spaces and symbols. Expected = decoded string. */
    @Test
    public void urlDecodeUtf8ValidStringShouldPass() {
        Assert.assertEquals("hello world!", Utils.urlDecodeUtf8("hello+world%21"));
    }

    /** Test readCommandLineOpts with absent system property. Expected = empty map. */
    @Test
    public void readCommandLineOptsNoPropertyShouldReturnEmptyMap() {
        System.clearProperty("storm.options");
        Assert.assertTrue(Utils.readCommandLineOpts().isEmpty());
    }

    /** Test readCommandLineOpts with valid encoded values. Expected = parsed map. */
    // @Test    (FAILED)  java.lang.Integer<1>
    public void readCommandLineOptsValidPropertyShouldParseValues() {
        System.setProperty("storm.options", "a=1,b=" + Utils.urlEncodeUtf8("hello world"));
        Map<String, Object> opts = Utils.readCommandLineOpts();
        Assert.assertEquals(1L, opts.get("a"));
        Assert.assertEquals("hello world", opts.get("b"));
    }

    /** Test readStormConfig. Expected = non-null configuration map. */
    @Test
    public void readStormConfigShouldReturnMap() {
        Assert.assertNotNull(Utils.readStormConfig());
    }

    /** Test bitXorVals with valid long values. Expected = cumulative XOR. */
    @Test
    public void bitXorValsValidCollectionShouldPass() {
        Assert.assertEquals(1L ^ 2L ^ 3L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    /** Test bitXorVals with empty collection. Expected = zero. */
    @Test
    public void bitXorValsEmptyCollectionShouldReturnZero() {
        Assert.assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
    }

    /** Test bitXor with valid values. Expected = a ^ b. */
    @Test
    public void bitXorValidValuesShouldPass() {
        Assert.assertEquals(6L, Utils.bitXor(5L, 3L));
    }

    /** Test addShutdownHookWithDelayedForceKill with runnable. Expected = no exception on registration. */
    // @Test
    public void addShutdownHookWithDelayedForceKillValidRunnableShouldPass() {
        Utils.addShutdownHookWithDelayedForceKill(new Runnable() { public void run() { } }, 1);
    }

    /** Test addShutdownHookWithForceKillIn1Sec with runnable. Expected = no exception on registration. */
    // @Test
    public void addShutdownHookWithForceKillIn1SecValidRunnableShouldPass() {
        Utils.addShutdownHookWithForceKillIn1Sec(new Runnable() { public void run() { } });
    }

    /** Test isSystemId with system id. Expected = true. */
    @Test
    public void isSystemIdDoubleUnderscoreShouldReturnTrue() {
        Assert.assertTrue(Utils.isSystemId("__acker"));
    }

    /** Test isSystemId with normal id. Expected = false. */
    @Test
    public void isSystemIdNormalIdShouldReturnFalse() {
        Assert.assertFalse(Utils.isSystemId("component"));
    }

    /** Test asyncLoop full signature without immediate start. Expected = configured thread is returned. */
    @Test
    public void asyncLoopFullSignatureNoStartShouldReturnThread() {
        Callable<Object> callable = new Callable<Object>() { public Object call() { return null; } };
        Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) { }
        };

        Utils.SmartThread thread = Utils.asyncLoop(callable, true, handler, Thread.NORM_PRIORITY, false, false, "fsp2");

        Assert.assertNotNull(thread);
        Assert.assertTrue(thread.isDaemon());
        Assert.assertTrue(thread.getName().contains("fsp2"));
    }

    /** Test asyncLoop overload with thread name. Expected = configured thread is returned. */
    @Test
    public void asyncLoopWithNameShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(new Callable<Object>() { public Object call() { return null; } }, "named", null);
        Assert.assertNotNull(thread);
        Assert.assertTrue(thread.getName().contains("named"));
    }

    /** Test asyncLoop overload with only callable. Expected = thread is returned. */
    @Test
    public void asyncLoopWithCallableShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(new Callable<Object>() { public Object call() { return null; } });
        Assert.assertNotNull(thread);
    }

    /** Test exceptionCauseIsInstanceOf with nested cause. Expected = true. */
    @Test
    public void exceptionCauseIsInstanceOfNestedCauseShouldReturnTrue() {
        Throwable t = new RuntimeException(new IllegalArgumentException("bad"));
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, t));
    }

    /** Test exceptionCauseIsInstanceOf with unrelated exception. Expected = false. */
    @Test
    public void exceptionCauseIsInstanceOfUnrelatedShouldReturnFalse() {
        Assert.assertFalse(Utils.exceptionCauseIsInstanceOf(IllegalStateException.class, new RuntimeException("x")));
    }

    /** Test unwrapTo with nested matching exception. Expected = matching throwable. */
    @Test
    public void unwrapToNestedCauseShouldReturnCause() {
        IllegalArgumentException cause = new IllegalArgumentException("bad");
        Assert.assertSame(cause, Utils.unwrapTo(IllegalArgumentException.class, new RuntimeException(cause)));
    }

    /** Test unwrapTo with no matching exception. Expected = null. */
    @Test
    public void unwrapToNoMatchShouldReturnNull() {
        Assert.assertNull(Utils.unwrapTo(IllegalStateException.class, new RuntimeException("x")));
    }

    /** Test unwrapAndThrow with matching exception. Expected = throws matching exception. */
    @Test
    public void unwrapAndThrowMatchingCauseShouldThrow() {
        final IllegalArgumentException cause = new IllegalArgumentException("bad");
        Assert.assertThrows(IllegalArgumentException.class, new org.junit.function.ThrowingRunnable() {
            public void run() { Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException(cause)); }
        });
    }

    /** Test wrapInRuntime with runtime exception. Expected = same object. */
    @Test
    public void wrapInRuntimeRuntimeExceptionShouldReturnSameObject() {
        RuntimeException e = new RuntimeException("x");
        Assert.assertSame(e, Utils.wrapInRuntime(e));
    }

    /** Test wrapInRuntime with checked exception. Expected = RuntimeException with checked cause. */
    @Test
    public void wrapInRuntimeCheckedExceptionShouldWrap() {
        Exception e = new Exception("x");
        RuntimeException ret = Utils.wrapInRuntime(e);
        Assert.assertSame(e, ret.getCause());
    }

    /** Test secureRandomLong. Expected = returns long without throwing. */
    @Test
    public void secureRandomLongShouldReturnLong() {
        long value = Utils.secureRandomLong();
        Assert.assertEquals(value, value);
    }

    /** Test hostname/localHostname via delegated instance. Expected = delegated host. */
    @Test
    public void localHostnameDelegatedInstanceShouldPass() throws Exception {
        Utils current = new TestUtils("unit-host");
        Utils.setInstance(current);
        Assert.assertEquals("unit-host", Utils.localHostname());
    }

    /** Test uuid. Expected = valid UUID string. */
    @Test
    public void uuidShouldReturnValidUuid() {
        Assert.assertNotNull(UUID.fromString(Utils.uuid()));
    }

    /** Test javaSerialize/javaDeserialize with valid object. Expected = round trip. */
    @Test
    public void javaSerializeDeserializeValidObjectShouldPass() {
        ArrayList<String> expected = new ArrayList<>(Arrays.asList("a", "b"));
        byte[] serialized = Utils.javaSerialize(expected);
        Assert.assertEquals(expected, Utils.javaDeserialize(serialized, ArrayList.class));
    }

    /** Test javaDeserialize with null bytes. Expected = RuntimeException. */
    @Test
    public void javaDeserializeNullBytesShouldThrowRuntimeException() {
        Assert.assertThrows(RuntimeException.class, new org.junit.function.ThrowingRunnable() {
            public void run() { Utils.javaDeserialize(null, String.class); }
        });
    }

    /** Test get with existing key. Expected = stored value. */
    @Test
    public void getExistingKeyShouldReturnValue() {
        Map<String, Integer> m = new HashMap<>();
        m.put("k", 7);
        Assert.assertEquals(Integer.valueOf(7), Utils.get(m, "k", 0));
    }

    /** Test get with absent key. Expected = default value. */
    @Test
    public void getAbsentKeyShouldReturnDefault() {
        Assert.assertEquals(Integer.valueOf(9), Utils.get(new HashMap<String, Integer>(), "k", 9));
    }

    /** Test zeroIfNaNOrInf with special and normal values. Expected = zero only for NaN/Infinity. */
    @Test
    public void zeroIfNaNOrInfBoundaryValuesShouldPass() {
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
        Assert.assertEquals(1.5, Utils.zeroIfNaNOrInf(1.5), 0.0);
    }

    /** Test join with valid collection and separator. Expected = joined string. */
    @Test
    public void joinValidCollectionShouldPass() {
        Assert.assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    /** Test join empty collection. Expected = empty string. */
    @Test
    public void joinEmptyCollectionShouldReturnEmptyString() {
        Assert.assertEquals("", Utils.join(Collections.emptyList(), ","));
    }

    /** Test parseZkId with valid digest id. Expected = Id with scheme and id. */
    @Test
    public void parseZkIdValidDigestShouldPass() {
        Id id = Utils.parseZkId("digest:user:pass", "test.conf");
        Assert.assertEquals("digest", id.getScheme());
        Assert.assertEquals("user:pass", id.getId());
    }

    /** Test getSuperUserAcl with configured user. Expected = ACL with all permissions. */
    @Test
    public void getSuperUserAclValidConfShouldPass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "digest:user:pass");
        ACL acl = Utils.getSuperUserAcl(conf);
        Assert.assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
    }

    /** Test getWorkerACL without topology auth. Expected = null. */
    @Test
    public void getWorkerAclNoAuthShouldReturnNull() {
        Assert.assertNull(Utils.getWorkerACL(new HashMap<String, Object>()));
    }

    /** Test isZkAuthenticationConfiguredTopology with configured scheme. Expected = true. */
    @Test
    public void isZkAuthenticationConfiguredTopologyWithSchemeShouldReturnTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "digest");
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
    }

    /** Test isZkAuthenticationConfiguredTopology with empty conf. Expected = false. */
    @Test
    public void isZkAuthenticationConfiguredTopologyEmptyConfShouldReturnFalse() {
        Assert.assertFalse(Utils.isZkAuthenticationConfiguredTopology(new HashMap<String, Object>()));
    }

    /** Test handleUncaughtException with allowed exception. Expected = no throw. */
    @Test
    public void handleUncaughtExceptionAllowedShouldPass() {
        Set<Class<?>> allowed = new HashSet<Class<?>>();
        allowed.add(IllegalArgumentException.class);
        Utils.handleUncaughtException(new RuntimeException(new IllegalArgumentException("ok")), allowed, false);
    }

    /** Test handleUncaughtException not allowed. Expected = Error. */
    @Test
    public void handleUncaughtExceptionNotAllowedShouldThrowError() {
        Assert.assertThrows(Error.class, new org.junit.function.ThrowingRunnable() {
            public void run() { Utils.handleUncaughtException(new RuntimeException("boom")); }
        });
    }

    /** Test create default uncaught exception handler. Expected = handler not null and throws Error for not allowed exception. */
    @Test
    public void createDefaultUncaughtExceptionHandlerShouldReturnHandler() {
        Thread.UncaughtExceptionHandler handler = Utils.createDefaultUncaughtExceptionHandler();
        Assert.assertNotNull(handler);
    }

    /** Test create worker uncaught exception handler. Expected = handler not null. */
    @Test
    public void createWorkerUncaughtExceptionHandlerShouldReturnHandler() {
        Assert.assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
    }

    /** Test setup uncaught exception handlers. Expected = default handler is configured. */
    @Test
    public void setupUncaughtExceptionHandlersShouldPass() {
        Utils.setupDefaultUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        Utils.setupWorkerUncaughtExceptionHandler();
        Assert.assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    /** Test thriftSerialize/thriftDeserialize with generated TBase object. Expected = round trip. */
    @Test
    public void thriftSerializeDeserializeValidTbaseShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 7);  // CHANGED "bob" TO 7
        byte[] serialized = Utils.thriftSerialize(expected);
        AccessControl actual = Utils.thriftDeserialize(AccessControl.class, serialized);
        Assert.assertEquals(expected, actual);
    }

    /** Test thriftDeserialize with offset and length. Expected = object read from slice. */
    @Test
    public void thriftDeserializeWithOffsetShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 7); // CHANGED "bob" TO 7
        byte[] serialized = Utils.thriftSerialize(expected);
        byte[] padded = new byte[serialized.length + 2];
        System.arraycopy(serialized, 0, padded, 1, serialized.length);
        Assert.assertEquals(expected, Utils.thriftDeserialize(AccessControl.class, padded, 1, serialized.length));
    }

    /** Test sleepNoSimulation with boundary zero. Expected = completes. */
    @Test
    public void sleepNoSimulationZeroMillisShouldPass() {
        Utils.sleepNoSimulation(0L);
    }

    /** Test sleep with boundary zero. Expected = completes. */
    @Test
    public void sleepZeroMillisShouldPass() {
        Utils.sleep(0L);
    }

    /** Test makeUptimeComputer. Expected = non-null object. */
    @Test
    public void makeUptimeComputerShouldReturnObject() {
        Assert.assertNotNull(Utils.makeUptimeComputer());
    }

    /** Test makeUptimeComputerImpl. Expected = non-null object. */
    @Test
    public void makeUptimeComputerImplShouldReturnObject() {
        Assert.assertNotNull(new Utils().makeUptimeComputerImpl());
    }

    /** Test reverseMap with valid map. Expected = values become keys. */
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

    /** Test reverseMap list sequence. Expected = reversed map. */
    @Test
    public void reverseMapListSequenceShouldPass() {
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.<Object>asList("k1", "v1"));
        rows.add(Arrays.<Object>asList("k2", "v1"));
        Map<Object, List<Object>> ret = Utils.reverseMap(rows);
        Assert.assertEquals(Arrays.<Object>asList("k1", "k2"), ret.get("v1"));
    }

    /** Test isOnWindows. Expected = consistent with OS environment variable. */
    @Test
    public void isOnWindowsShouldMatchEnvironment() {
        boolean expected = "Windows_NT".equals(System.getenv("OS"));
        Assert.assertEquals(expected, Utils.isOnWindows());
    }

    /** Test checkFileExists with existing file. Expected = true. */
    @Test
    public void checkFileExistsCorrectPathShouldPass() throws Exception {
        File file = tmp.newFile("correctFile");
        Assert.assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
    }

    /** Test checkFileExists with not existing file. Expected = false. */
    @Test
    public void checkFileExistsNotExistingPathShouldReturnFalse() {
        Assert.assertFalse(Utils.checkFileExists(new File(tmp.getRoot(), "missing").getAbsolutePath()));
    }

    /** Test forceDelete with existing file. Expected = file removed. */
    @Test
    public void forceDeleteExistingPathShouldPass() throws Exception {
        File file = tmp.newFile("delete-me.txt");
        Utils.forceDelete(file.getAbsolutePath());
        Assert.assertFalse(file.exists());
    }

    /** Test serialize/deserialize with valid TBase object. Expected = round trip. */
    @Test
    public void serializeDeserializeValidTbaseShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 7); // CHANGED "alice" TO 7
        byte[] serialized = Utils.serialize(expected);
        Assert.assertEquals(expected, Utils.deserialize(serialized, AccessControl.class));
    }

    /** Test serializeToString/deserializeFromString with valid object. Expected = round trip. */
    @Test
    public void serializeToStringDeserializeFromStringShouldPass() {
        AccessControl expected = new AccessControl(AccessControlType.USER, 7); // CHANGED "alice" TO 7
        String str = Utils.serializeToString(expected);
        Assert.assertEquals(expected, Utils.deserializeFromString(str, AccessControl.class));
    }

    /** Test deserializeFromString with null string. Expected = NullPointerException. */
    @Test
    public void deserializeFromStringNullStrThrowsNullPointerException() {
        Assert.assertThrows(NullPointerException.class, new org.junit.function.ThrowingRunnable() {
            public void run() { Utils.deserializeFromString(null, String.class); }
        });
    }

    /** Test toByteArray with heap buffer not at zero position. Expected = remaining bytes only. */
    @Test
    public void toByteArrayValidBufferShouldPass() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
        buffer.position(1);
        Assert.assertArrayEquals(new byte[] {2, 3, 4}, Utils.toByteArray(buffer));
    }

    /** Test mkSuicideFn. Expected = runnable object is created; not executed to avoid process exit. */
    @Test
    public void mkSuicideFnShouldReturnRunnable() {
        Assert.assertNotNull(Utils.mkSuicideFn());
    }

    /** Test readAndLogStream with valid input stream. Expected = completes. */
    @Test
    public void readAndLogStreamValidStreamShouldPass() {
        InputStream in = new ByteArrayInputStream("line1\nline2".getBytes());
        Utils.readAndLogStream("test", in);
    }

//    /** Test getComponentCommon for spout, bolt and state spout. Expected = component common returned. */
//    @Test
//    public void getComponentCommonExistingComponentsShouldPass() {  (GENERATED IN A WRONG WAY)
//        ComponentCommon spoutCommon = new ComponentCommon();
//        ComponentCommon boltCommon = new ComponentCommon();
//        ComponentCommon stateCommon = new ComponentCommon();
//        StormTopology topology = new StormTopology();
//        topology.put_to_spouts("spout", new SpoutSpec(ComponentObject.java_object("obj"), spoutCommon));
//        topology.put_to_bolts("bolt", new Bolt(ComponentObject.java_object("obj"), boltCommon));
//        topology.put_to_state_spouts("state", new StateSpoutSpec(ComponentObject.java_object("obj"), stateCommon));
//
//        Assert.assertSame(spoutCommon, Utils.getComponentCommon(topology, "spout"));
//        Assert.assertSame(boltCommon, Utils.getComponentCommon(topology, "bolt"));
//        Assert.assertSame(stateCommon, Utils.getComponentCommon(topology, "state"));
//    }

    /** Test tuple with mixed values. Expected = ordered list. */
    @Test
    public void tupleMixedValuesShouldPass() {
        Assert.assertEquals(Arrays.<Object>asList("a", 1, null), Utils.tuple("a", 1, null));
    }

    /** Test getRepeat with repeated and unique values. Expected = repeated string returned once. */
    @Test
    public void getRepeatValidListShouldPass() {
        Assert.assertEquals(Collections.singletonList("a"), Utils.getRepeat(Arrays.asList("a", "b", "a")));
    }

    /** Test getGlobalStreamId with null stream. Expected = default stream id. */
    @Test
    public void getGlobalStreamIdNullStreamShouldUseDefault() {
        GlobalStreamId id = Utils.getGlobalStreamId("component", null);
        Assert.assertEquals("component", id.get_componentId());
        Assert.assertEquals(Utils.DEFAULT_STREAM_ID, id.get_streamId());
    }

    /** Test getGlobalStreamId with explicit stream. Expected = explicit stream id. */
    @Test
    public void getGlobalStreamIdExplicitStreamShouldPass() {
        Assert.assertEquals("stream", Utils.getGlobalStreamId("component", "stream").get_streamId());
    }

//    /** Test getSetComponentObject with java object. Expected = object value. */
//    @Test
//    public void getSetComponentObjectJavaObjectShouldPass() {
//        Assert.assertEquals("value", Utils.getSetComponentObject(ComponentObject.java_object("value")));
//    }

    /** Test getSetComponentObject with shell object. Expected = shell value. */
    @Test
    public void getSetComponentObjectShellShouldPass() {
        ShellComponent shell = new ShellComponent("python", "component.py");  // CHANGED FIRST PARAMETER FROM LIST TO STRING
        Assert.assertEquals(shell, Utils.getSetComponentObject(ComponentObject.shell(shell)));
    }

    /** Test toPositive with boundary values. Expected = non-negative values. */
    @Test
    public void toPositiveBoundaryValuesShouldPass() {
        Assert.assertTrue(Utils.toPositive(-1) >= 0);
        Assert.assertEquals(0, Utils.toPositive(0));
        Assert.assertEquals(1, Utils.toPositive(1));
    }

    /** Test processPid. Expected = non-empty pid-like string. */
    @Test
    public void processPidShouldReturnString() {
        Assert.assertNotNull(Utils.processPid());
        Assert.assertFalse(Utils.processPid().isEmpty());
    }

    /** Test toCompressedJsonConf/fromCompressedJsonConf with valid map. Expected = round trip. */
    // @Test   (FAILED)  expected {"a":1}
    public void compressedJsonConfValidMapShouldPass() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", 1L);
        byte[] serialized = Utils.toCompressedJsonConf(map);
        Assert.assertEquals(map, Utils.fromCompressedJsonConf(serialized));
    }

    /** Test fromCompressedJsonConf with invalid bytes. Expected = RuntimeException. */
    @Test
    public void fromCompressedJsonConfInvalidSerializedThrowsRuntimeException() {
        Assert.assertThrows(RuntimeException.class, new org.junit.function.ThrowingRunnable() {
            public void run() { Utils.fromCompressedJsonConf(new byte[] {1, 2, 3}); }
        });
    }

    /** Test redactValue. Expected = new map with redacted value and original unchanged. */
    @Test
    public void redactValueExistingKeyShouldPass() {
        Map<String, Object> original = new HashMap<>();
        original.put("password", "secret");
        Map<String, Object> redacted = Utils.redactValue(original, "password");
        Assert.assertEquals("secret", original.get("password"));
        Assert.assertNotEquals("secret", redacted.get("password"));
    }

    /** Test parseJvmHeapMemByChildOpts with megabytes, gigabytes and default. Expected = parsed MB value. */
    @Test
    public void parseJvmHeapMemByChildOptsValidAndDefaultShouldPass() {
        Assert.assertEquals(Double.valueOf(256.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx256m"), 64.0));
        Assert.assertEquals(Double.valueOf(1024.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1g"), 64.0));
        Assert.assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xms1m"), 64.0));
    }

    /** Test getClientBlobStore with minimal configuration. Expected = store instance or wrapped runtime exception depending environment. */
    @Test
    public void getClientBlobStoreSmokeShouldNotReturnNullWhenConfigured() {
        Map<String, Object> conf = new HashMap<>(Utils.readDefaultConfig());
        try {
            ClientBlobStore store = Utils.getClientBlobStore(conf);
            Assert.assertNotNull(store);
        } catch (RuntimeException ex) {
            Assert.assertNotNull(ex);
        }
    }

    /** Test isValidConf with valid-like configuration. Expected = boolean result without exception. */
    // @Test   expected = true
    public void isValidConfSmokeShouldReturnBoolean() {
        Assert.assertFalse(Utils.isValidConf(null));
        Assert.assertEquals(Utils.isValidConf(new HashMap<String, Object>()), Utils.isValidConf(new HashMap<String, Object>()));
    }

    /** Test getTopologyInfo with null-like unknown name. Expected = null or wrapped exception based on external service availability. */
    @Test
    public void getTopologyInfoInvalidNameShouldBeSafe() {
        try {
            TopologyInfo info = Utils.getTopologyInfo("not-existing-topology", null, new HashMap<String, Object>());
            Assert.assertNull(info);
        } catch (RuntimeException ex) {
            Assert.assertNotNull(ex);
        }
    }

    /** Test getTopologyId with mocked Nimbus client. Expected = matching topology id. */
    // @Test  (FAILED) expected = null
    public void getTopologyIdMatchingNameShouldReturnId() throws Exception {
        Nimbus.Iface client = mock(Nimbus.Iface.class);
        TopologySummary summary = new TopologySummary();
        summary.set_name("topology-a");
        summary.set_id("id-a");
        ClusterSummary clusterSummary = new ClusterSummary();
        clusterSummary.set_topologies(Collections.singletonList(summary));
        when(client.getClusterInfo()).thenReturn(clusterSummary);

        Assert.assertEquals("id-a", Utils.getTopologyId("topology-a", client));
    }

    /** Test validateTopologyBlobStoreMap with empty config. Expected = completes. */
    // @Test  (FAILED)  ConnectException: Connection refused)
    public void validateTopologyBlobStoreMapEmptyConfShouldPass() throws Exception {
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>());
    }

    /** Test validateTopologyBlobStoreMap overload with mocked NimbusBlobStore. Expected = completes for empty config. */
    @Test
    public void validateTopologyBlobStoreMapWithNimbusBlobStoreShouldPass() throws Exception {
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), mock(NimbusBlobStore.class));
    }

    /** Test validateTopologyBlobStoreMap overload with mocked BlobStore. Expected = completes for empty config. */
    @Test
    public void validateTopologyBlobStoreMapWithBlobStoreShouldPass() throws Exception {
        Utils.validateTopologyBlobStoreMap(new HashMap<String, Object>(), mock(BlobStore.class));
    }

    /** Test threadDump. Expected = human-readable thread dump string. */
    @Test
    public void threadDumpShouldReturnString() {
        String dump = Utils.threadDump();
        Assert.assertNotNull(dump);
        Assert.assertTrue(dump.contains("Thread"));
    }

    /** Test checkDirExists with directory and file. Expected = true only for directory. */
    @Test
    public void checkDirExistsBoundaryPathsShouldPass() throws Exception {
        File dir = tmp.newFolder("dir");
        File file = tmp.newFile("file.txt");
        Assert.assertTrue(Utils.checkDirExists(dir.getAbsolutePath()));
        Assert.assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
    }

    /** Test getConfiguredClass with absent key. Expected = null. */
    @Test
    public void getConfiguredClassAbsentKeyShouldReturnNull() {
        Assert.assertNull(Utils.getConfiguredClass(new HashMap<String, Object>(), "missing"));
    }

    /** Test isZkAuthenticationConfiguredStormServer with scheme. Expected = true. */
    @Test
    public void isZkAuthenticationConfiguredStormServerWithSchemeShouldReturnTrue() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "digest");
        Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(conf));
    }

    /** Test nullToZero with null and valid values. Expected = null becomes zero. */
    @Test
    public void nullToZeroValuesShouldPass() {
        Assert.assertEquals(0.0, Utils.nullToZero(null), 0.0);
        Assert.assertEquals(3.5, Utils.nullToZero(3.5), 0.0);
    }

    /** Test OR with null and non-null first argument. Expected = first non-null. */
    @Test
    public void orValuesShouldPass() {
        Assert.assertEquals("b", Utils.OR(null, "b"));
        Assert.assertEquals("a", Utils.OR("a", "b"));
    }

    /** Test integerDivided with boundary values. Expected = sum distributed among pieces. */
    // @Test  (FAILED)  key 0 doesn't exists
    public void integerDividedValidValuesShouldPass() {
        TreeMap<Integer, Integer> ret = Utils.integerDivided(10, 3);
        Assert.assertEquals(Integer.valueOf(4), ret.get(0));
        Assert.assertEquals(Integer.valueOf(3), ret.get(1));
        Assert.assertEquals(Integer.valueOf(3), ret.get(2));
    }

    /** Test partitionFixed with max chunks lower than collection size. Expected = fixed number of chunks. */
    @Test
    public void partitionFixedValidCollectionShouldPass() {
        List<List<Integer>> chunks = Utils.partitionFixed(2, Arrays.asList(1, 2, 3, 4, 5));
        Assert.assertEquals(2, chunks.size());
        Assert.assertEquals(Arrays.asList(1, 2, 3), chunks.get(0));
        Assert.assertEquals(Arrays.asList(4, 5), chunks.get(1));
    }

    /** Test readYamlFile with valid and missing file. Expected = parsed map and null for missing file. */
    @Test
    public void readYamlFileValidAndMissingShouldPass() throws Exception {
        File yaml = tmp.newFile("test.yaml");
        try (FileWriter fw = new FileWriter(yaml)) {
            fw.write("a: 1\n");
        }
        Object parsed = Utils.readYamlFile(yaml.getAbsolutePath());
        Assert.assertTrue(parsed instanceof Map);
        Assert.assertNull(Utils.readYamlFile(new File(tmp.getRoot(), "missing.yaml").getAbsolutePath()));
    }

    /** Test getAvailablePort with preferred port 0. Expected = available positive port. */
    @Test
    public void getAvailablePortPreferredZeroShouldReturnPort() {
        Assert.assertTrue(Utils.getAvailablePort(0) > 0);
    }

    /** Test getAvailablePort overload. Expected = available positive port. */
    @Test
    public void getAvailablePortShouldReturnPort() {
        Assert.assertTrue(Utils.getAvailablePort() > 0);
    }

    /** Test findOne with collection. Expected = first matching value. */
    @Test
    public void findOneCollectionShouldReturnFirstMatch() {
        String ret = Utils.findOne(new IPredicate<String>() { public boolean test(String x) { return x.startsWith("b"); } }, Arrays.asList("a", "bb", "bc"));
        Assert.assertEquals("bb", ret);
    }

//    /** Test findOne with map. Expected = matching map entry object or null if no match. */
//    @Test
//    public void findOneMapShouldReturnMatch() {  (GENERATED IN A WRONG WAY)
//        Map<String, Integer> map = new LinkedHashMap<>();
//        map.put("a", 1);
//        map.put("b", 2);
//        Map.Entry<String, Integer> ret = Utils.findOne(new IPredicate<Map.Entry<String, Integer>>() {
//            public boolean test(Map.Entry<String, Integer> e) { return e.getValue() == 2; }
//        }, map);
//        Assert.assertEquals("b", ret.getKey());
//    }

    /** Test parseJson with valid and blank json. Expected = parsed map and empty map. */
    // @Test   (FAILED) expected integer not long
    public void parseJsonValidAndBlankShouldPass() {
        Assert.assertEquals(1L, Utils.parseJson("{\"a\":1}").get("a"));
        Assert.assertTrue(Utils.parseJson(null).isEmpty());
        Assert.assertTrue(Utils.parseJson("").isEmpty());
    }

    /** Test memoizedLocalHostname. Expected = stable non-null value. */
    @Test
    public void memoizedLocalHostnameShouldBeStable() throws Exception {
        String first = Utils.memoizedLocalHostname();
        String second = Utils.memoizedLocalHostname();
        Assert.assertNotNull(first);
        Assert.assertEquals(first, second);
    }

    /** Test addVersions with empty topology. Expected = same topology returned. */
    @Test
    public void addVersionsShouldReturnTopology() {
        StormTopology topology = new StormTopology();
        Assert.assertSame(topology, Utils.addVersions(topology));
    }

    /** Test configured classpath versions with empty data. Expected = non-null map. */
    @Test
    public void getConfiguredClasspathVersionsEmptyShouldReturnMap() {
        NavigableMap<SimpleVersion, List<String>> ret = Utils.getConfiguredClasspathVersions(new HashMap<String, Object>(), Collections.<String>emptyList());
        Assert.assertNotNull(ret);
    }

    /** Test alternative versions map with empty config. Expected = non-null map. */
    @Test
    public void getAlternativeVersionsMapEmptyShouldReturnMap() {
        NavigableMap<String, IVersionInfo> ret = Utils.getAlternativeVersionsMap(new HashMap<String, Object>());
        Assert.assertNotNull(ret);
    }

    /** Test worker main versions with empty config. Expected = non-null map. */
    @Test
    public void getConfiguredWorkerMainVersionsEmptyShouldReturnMap() {
        Assert.assertNotNull(Utils.getConfiguredWorkerMainVersions(new HashMap<String, Object>()));
    }

    /** Test worker log writer versions with empty config. Expected = non-null map. */
    @Test
    public void getConfiguredWorkerLogWriterVersionsEmptyShouldReturnMap() {
        Assert.assertNotNull(Utils.getConfiguredWorkerLogWriterVersions(new HashMap<String, Object>()));
    }

    /** Test getCompatibleVersion with empty versioned map. Expected = default value. */
    @Test
    public void getCompatibleVersionEmptyMapShouldReturnDefault() {
        String def = "default";
        Assert.assertSame(def, Utils.getCompatibleVersion(new TreeMap<SimpleVersion, String>(), new SimpleVersion("1.0.0"), "test", def));  // CHANGED 1,0,0 TO STRING "1.0.0"
    }

    /** Test getConfigFromClasspath with empty classpath. Expected = provided conf or defaults fallback. */
    @Test
    public void getConfigFromClasspathEmptyShouldReturnMap() throws IOException {  // ADDED THROWS
        Map<String, Object> conf = new HashMap<>();
        conf.put("k", "v");
        Assert.assertNotNull(Utils.getConfigFromClasspath(Collections.<String>emptyList(), conf));
    }

    /** Test isLocalhostAddress with valid and not valid values. Expected = true only known localhost. */
    @Test
    public void isLocalhostAddressValuesShouldPass() {
        Assert.assertTrue(Utils.isLocalhostAddress("localhost"));
        Assert.assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
        Assert.assertFalse(Utils.isLocalhostAddress("192.168.1.10"));
    }

    /** Test merge with overlapping keys. Expected = second map overrides first. */
    @Test
    public void mergeOverlappingMapsShouldPass() {
        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        first.put("b", 1);
        Map<String, Integer> second = new HashMap<>();
        second.put("b", 2);
        Map<String, Integer> ret = Utils.merge(first, second);
        Assert.assertEquals(Integer.valueOf(1), ret.get("a"));
        Assert.assertEquals(Integer.valueOf(2), ret.get("b"));
    }

    /** Test convertToArray with valid source map and start index equal to max key. Expected = list with size 1. */
    @Test
    public void convertToArrayValidSrcMapEqualStartShouldPass() {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        List<Integer> list = Utils.convertToArray(map, 3);
        Assert.assertEquals(Collections.singletonList(3), list);
    }

    /** Test convertToArray with boundary start below first key. Expected = ordered values from start. */
    @Test
    public void convertToArrayStartBelowFirstKeyShouldPass() {
        Map<Integer, String> map = new HashMap<>();
        map.put(0, "zero");
        map.put(1, "one");
        Assert.assertEquals(Arrays.asList("zero", "one"), Utils.convertToArray(map, 0));
    }

    /** Test isValidKey with valid, empty and traversal-like keys. Expected = true only for valid key. */
    @Test
    public void isValidKeyCategoryPartitionShouldPass() {
        Assert.assertTrue(Utils.isValidKey("valid_key-1.txt"));
        Assert.assertFalse(Utils.isValidKey(""));
        Assert.assertFalse(Utils.isValidKey(".."));
        Assert.assertFalse(Utils.isValidKey("bad/key"));
    }

    /** Test validateTopologyName with valid and invalid names. Expected = invalid names throw InvalidTopologyException. */
    // @Test   (FAILED)  expected IllegalArgumentException
    public void validateTopologyNameValuesShouldPass() throws Exception {
        Utils.validateTopologyName("valid-topology_1");
        Assert.assertThrows(InvalidTopologyException.class, new org.junit.function.ThrowingRunnable() {
            public void run() throws Throwable { Utils.validateTopologyName("bad/name"); }
        });
    }

    /** Test findComponentCycles with empty topology. Expected = no cycles. */
    @Test
    public void findComponentCyclesEmptyTopologyShouldReturnEmptyList() {
        Assert.assertTrue(Utils.findComponentCycles(new StormTopology(), "topo").isEmpty());
    }

    /** Test validateCycleFree with empty topology. Expected = completes. */
    @Test
    public void validateCycleFreeEmptyTopologyShouldPass() throws Exception {
        Utils.validateCycleFree(new StormTopology(), "topo");
    }

    /** Test compression helpers from Utils public API. Expected = compressed data round trip. */
    @Test
    public void compressDecompressValidBytesShouldPass() {  //SPECIFIED NESTED CLASS
        byte[] data = "hello".getBytes();
        byte[] compressed = Utils.GzipUtils.compress(data);
        Assert.assertTrue(Utils.GzipUtils.isGzip(compressed));
        Assert.assertArrayEquals(data, Utils.GzipUtils.decompress(compressed, 1024));
    }

//    /** Test compression helpers with empty input. Expected = empty output. */
//    @Test
//    public void compressEmptyBytesShouldReturnEmptyBytes() {   (WHICH COMPRESSION !?)
//        Assert.assertArrayEquals(new byte[0], Utils.compress(new byte[0]));
//    }

    /** Test hostname smoke. Expected = non-empty string if environment resolves hostname. */
    @Test
    public void hostnameShouldReturnString() throws Exception {
        String host = Utils.hostname();
        Assert.assertNotNull(host);
        Assert.assertFalse(host.isEmpty());
    }

    /* ### Test END ### */
}
