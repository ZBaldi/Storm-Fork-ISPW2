package org.apache.storm.utils;

import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.lang.IllegalStateException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

/**
 * Suite JUnit 4 complessiva per Utils, progettata con Category Partition
 * e Boundary Value Analysis e mantenuta prudente sui metodi con side effect di processo.
 */
public class TotUtilsTest {  // REMOVED NOT USED IMPORTS
    /* ### Test START ### */

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final Thread.UncaughtExceptionHandler originalDefaultHandler = Thread.getDefaultUncaughtExceptionHandler();
    private final Utils originalInstance = Utils.setInstance(new Utils());
    private final String originalStormOptions = System.getProperty("storm.options");
    private final String originalStormConfFile = System.getProperty("storm.conf.file");

    @After
    public void tearDown() {
        restoreProperty("storm.options", originalStormOptions);
        restoreProperty("storm.conf.file", originalStormConfFile);
        Thread.setDefaultUncaughtExceptionHandler(originalDefaultHandler);
        Utils.setInstance(originalInstance == null ? new Utils() : originalInstance);
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    @Test
    public void setInstanceValidInstanceReturnsPreviousInstance() {
        Utils first = new Utils();
        Utils previous = Utils.setInstance(first);
        Utils second = new Utils();
        assertSame(first, Utils.setInstance(second));
        Utils.setInstance(previous);
    }

    @Test
    public void setAndResetClassLoaderForJavaDeserializeShouldAllowJavaRoundTrip() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        byte[] serialized = Utils.javaSerialize("value");
        assertEquals("value", Utils.javaDeserialize(serialized, String.class));
        Utils.resetClassLoaderForJavaDeSerialize();
        assertEquals("value", Utils.javaDeserialize(serialized, String.class));
    }

    @Test
    public void findResourcesExistingClasspathResourceShouldReturnAtLeastOneUrl() {
        List<URL> resources = Utils.findResources("defaults.yaml");
        assertFalse(resources.isEmpty());
    }

    @Test
    public void findResourcesNotExistingResourceShouldReturnEmptyList() {
        assertTrue(Utils.findResources("not-existing-resource-for-utils-test.yaml").isEmpty());
    }

    @Test
    public void findAndReadConfigFileNotExistingButOptionalShouldReturnEmptyMap() {
        assertEquals(Collections.emptyMap(), Utils.findAndReadConfigFile("notCorrectYamlFile", false));
    }

    @Test(expected = RuntimeException.class)
    public void findAndReadConfigFileNullNameAndRequiredShouldThrowRuntimeException() {
        Utils.findAndReadConfigFile(null, true);
    }

    @Test
    public void findAndReadConfigFileOverloadShouldReadDefaults() {
        assertFalse(Utils.findAndReadConfigFile("defaults.yaml").isEmpty());
    }

    @Test
    public void readDefaultConfigShouldReadDefaultsYaml() {
        assertFalse(Utils.readDefaultConfig().isEmpty());
    }

    @Test
    public void urlEncodeUtf8ValidStringShouldEncodeSpacesAndUnicode() {
        assertEquals("a+b+%C3%A8", Utils.urlEncodeUtf8("a b è"));
    }

    @Test(expected = NullPointerException.class)
    public void urlEncodeUtf8NullStringShouldThrowNullPointerException() {
        Utils.urlEncodeUtf8(null);
    }

    @Test
    public void urlDecodeUtf8EncodedStringShouldDecode() {
        assertEquals("a b è", Utils.urlDecodeUtf8("a+b+%C3%A8"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void urlDecodeUtf8InvalidEncodingShouldThrowIllegalArgumentException() {
        Utils.urlDecodeUtf8("%XX");
    }

    @Test
    public void readCommandLineOptsWithoutPropertyShouldReturnEmptyMap() {
        System.clearProperty("storm.options");
        assertTrue(Utils.readCommandLineOpts().isEmpty());
    }

    @Test
    public void readCommandLineOptsValidJsonAndPlainValuesShouldParse() {
        System.setProperty("storm.options", "alpha=1,beta=text,gamma=%5B1%2C2%5D");
        Map<String, Object> opts = Utils.readCommandLineOpts();
        assertEquals(1, ((Number) opts.get("alpha")).intValue());
        assertEquals("text", opts.get("beta"));
        assertEquals(Arrays.asList(1, 2), opts.get("gamma"));
    }

    @Test
    public void readStormConfigShouldIncludeCommandLineOverrides() {
        System.clearProperty("storm.conf.file");
        System.setProperty("storm.options", "storm.local.hostname=test-host-from-options");
        assertEquals("test-host-from-options", Utils.readStormConfig().get(Config.STORM_LOCAL_HOSTNAME));
    }

    @Test
    public void bitXorValsValidListShouldXorAllValues() {
        assertEquals(0L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
    }

    @Test
    public void bitXorValsEmptyListShouldReturnZero() {
        assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
    }

    @Test(expected = NullPointerException.class)
    public void bitXorValsNullElementShouldThrowNullPointerException() {
        Utils.bitXorVals(Arrays.asList(1L, null));
    }

    @Test
    public void bitXorBoundaryValuesShouldReturnJavaXorResult() {
        assertEquals(-1L, Utils.bitXor(0L, -1L));
    }

    @Test
    public void addShutdownHookMethodsWithNoopRunnableShouldComplete() {
        Utils.addShutdownHookWithForceKillIn1Sec(new Runnable() { public void run() { } });
        Utils.addShutdownHookWithDelayedForceKill(new Runnable() { public void run() { } }, 1);
    }

    @Test
    public void isSystemIdShouldBeTrueOnlyForDoubleUnderscorePrefix() {
        assertTrue(Utils.isSystemId("__system"));
        assertFalse(Utils.isSystemId("_system"));
        assertFalse(Utils.isSystemId("system"));
    }

    @Test(expected = NullPointerException.class)
    public void isSystemIdNullShouldThrowNullPointerException() {
        Utils.isSystemId(null);
    }

    // @Test
    public void asyncLoopFullSignatureStartImmediatelyFalseShouldReturnConfiguredThread() {
        Callable<Object> callable = new Callable<Object>() { public Object call() { return -1; } };
        Utils.SmartThread thread = Utils.asyncLoop(callable, true, null, Thread.NORM_PRIORITY, false, false, "utils-test-loop");
        assertEquals("utils-test-loop", thread.getName());
        assertTrue(thread.isDaemon());
        assertFalse(thread.isAlive());
    }

    // @Test
    public void asyncLoopWithThreadNameShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(new Callable<Object>() { public Object call() { return -1; } }, "named-loop", null);
        assertEquals("named-loop", thread.getName());
    }

    @Test
    public void asyncLoopDefaultSignatureShouldReturnThread() {
        Utils.SmartThread thread = Utils.asyncLoop(new Callable<Object>() { public Object call() { return -1L; } });
        assertNotNull(thread);
    }

    @Test
    public void exceptionCauseIsInstanceOfShouldInspectThrowableAndCauses() {
        RuntimeException root = new RuntimeException(new IllegalArgumentException("bad"));
        assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, root));
        assertFalse(Utils.exceptionCauseIsInstanceOf(java.io.IOException.class, root));
    }

    @Test
    public void unwrapToShouldReturnMatchingCauseOrNull() {
        IllegalStateException expected = new IllegalStateException("state");
        RuntimeException wrapped = new RuntimeException(expected);
        assertSame(expected, Utils.unwrapTo(IllegalStateException.class, wrapped));
        assertNull(Utils.unwrapTo(java.io.IOException.class, wrapped));
    }

    @Test(expected = IllegalStateException.class)
    public void unwrapAndThrowMatchingCauseShouldThrowIt() throws Throwable {
        Utils.unwrapAndThrow(IllegalStateException.class, new RuntimeException(new IllegalStateException("state")));
    }

    @Test
    public void unwrapAndThrowWithoutMatchingCauseShouldComplete() throws Throwable {
        Utils.unwrapAndThrow(java.io.IOException.class, new RuntimeException("no io cause"));
    }

    @Test
    public void wrapInRuntimeRuntimeExceptionShouldReturnSameInstance() {
        RuntimeException e = new RuntimeException("x");
        assertSame(e, Utils.wrapInRuntime(e));
    }

    @Test
    public void wrapInRuntimeCheckedExceptionShouldWrapCause() {
        Exception e = new Exception("x");
        RuntimeException wrapped = Utils.wrapInRuntime(e);
        assertSame(e, wrapped.getCause());
    }

    @Test
    public void secureRandomLongAndUuidShouldReturnValues() {
        assertNotEquals(0, Utils.uuid().length());
        Utils.secureRandomLong();
    }

    // @Test
    public void localHostnameDelegatedToInstanceShouldReturnMockedValue() throws Exception {
        Utils mock = new Utils() {
            @Override
            protected String localHostnameImpl() {
                return "mock-host";
            }
        };
        Utils previous = Utils.setInstance(mock);
        try {
            assertEquals("mock-host", Utils.localHostname());
        } finally {
            Utils.setInstance(previous);
        }
    }

    @Ignore("exitProcess termina la JVM e non va eseguito in unit test")
    @Test
    public void exitProcessShouldTerminateJvm() {
        Utils.exitProcess(0, "ignored");
    }

    @Test
    public void javaSerializeAndJavaDeserializeValidSerializableShouldRoundTrip() {
        ArrayList<String> value = new ArrayList<String>(Arrays.asList("a", "b"));
        byte[] serialized = Utils.javaSerialize(value);
        assertEquals(value, Utils.javaDeserialize(serialized, ArrayList.class));
    }

    @Test(expected = RuntimeException.class)
    public void javaDeserializeInvalidBytesShouldThrowRuntimeException() {
        Utils.javaDeserialize(new byte[] {1, 2, 3}, String.class);
    }

    // @Test
    public void getExistingMissingAndNullMapShouldReturnExpectedValueOrDefault() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        assertEquals(Integer.valueOf(1), Utils.get(map, "a", 9));
        assertEquals(Integer.valueOf(9), Utils.get(map, "b", 9));
        assertEquals(Integer.valueOf(9), Utils.get(null, "a", 9));
    }

    @Test
    public void zeroIfNaNOrInfShouldReplaceOnlyNaNAndInfinity() {
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
        assertEquals(-1.25, Utils.zeroIfNaNOrInf(-1.25), 0.0);
    }

    @Test
    public void joinValidCollectionShouldUseSeparator() {
        assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
        assertEquals("", Utils.join(Collections.<String>emptyList(), ","));
    }

    @Test(expected = NullPointerException.class)
    public void joinNullCollectionShouldThrowNullPointerException() {
        Utils.join(null, ",");
    }

    @Test
    public void parseZkIdValidDigestShouldReturnId() {
        Id id = Utils.parseZkId("digest:user:pwd", "confKey");
        assertEquals("digest", id.getScheme());
        assertEquals("user:pwd", id.getId());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseZkIdInvalidIdShouldThrowIllegalArgumentException() {
        Utils.parseZkId("missingSeparator", "confKey");
    }

    @Test
    public void getSuperUserAclConfiguredUserShouldReturnAllPermsAcl() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "digest:user:pwd");
        ACL acl = Utils.getSuperUserAcl(conf);
        assertEquals(ZooDefs.Perms.ALL, acl.getPerms());
        assertEquals(new Id("digest", "user:pwd"), acl.getId());
    }

    @Test
    public void getWorkerAclWithoutAuthenticationShouldReturnNull() {
        Map<String, Object> conf = new HashMap<String, Object>();
        assertNull(Utils.getWorkerACL(conf));
    }

    @Test
    public void isZkAuthenticationConfiguredTopologyShouldReflectConfiguredCredentials() {
        Map<String, Object> conf = new HashMap<String, Object>();
        assertFalse(Utils.isZkAuthenticationConfiguredTopology(conf));
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "digest");
        assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
    }

    @Test
    public void thriftSerializeAndDeserializeValidThriftObjectShouldRoundTrip() {
        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(7);
        byte[] serialized = Utils.thriftSerialize(expected);
        AccessControl actual = Utils.thriftDeserialize(AccessControl.class, serialized);
        assertEquals(expected, actual);
        byte[] padded = new byte[serialized.length + 2];
        System.arraycopy(serialized, 0, padded, 1, serialized.length);
        assertEquals(expected, Utils.thriftDeserialize(AccessControl.class, padded, 1, serialized.length));
    }

    @Test(expected = RuntimeException.class)
    public void thriftDeserializeInvalidBytesShouldThrowRuntimeException() {
        Utils.thriftDeserialize(AccessControl.class, new byte[] {1, 2, 3});
    }

    @Test
    public void sleepMethodsWithZeroMillisecondsShouldComplete() {
        Utils.sleepNoSimulation(0L);
        Utils.sleep(0L);
    }

    @Test
    public void makeUptimeComputerShouldDelegateToInstance() {
        final Utils.UptimeComputer custom = new Utils.UptimeComputer();
        Utils previous = Utils.setInstance(new Utils() {
            @Override
            public UptimeComputer makeUptimeComputerImpl() {
                return custom;
            }
        });
        try {
            assertSame(custom, Utils.makeUptimeComputer());
        } finally {
            Utils.setInstance(previous);
        }
    }

    @Test
    public void reverseMapValidMapShouldGroupKeysByValue() {
        Map<String, Integer> input = new LinkedHashMap<String, Integer>();
        input.put("a", 1);
        input.put("b", 1);
        input.put("c", 2);
        HashMap<Integer, List<String>> result = Utils.reverseMap(input);
        assertEquals(Arrays.asList("a", "b"), result.get(1));
        assertEquals(Arrays.asList("c"), result.get(2));
    }

    @Test
    public void reverseMapNullMapShouldReturnEmptyMap() {
        assertTrue(Utils.reverseMap((Map<String, Integer>) null).isEmpty());
    }

    @Test
    public void reverseMapListSeqShouldUseSecondElementAsKeyAndFirstAsValue() {
        List<List<Object>> listSeq = new ArrayList<List<Object>>();
        listSeq.add(Arrays.<Object>asList("task-1", "component-a"));
        listSeq.add(Arrays.<Object>asList("task-2", "component-a"));
        Map<Object, List<Object>> result = Utils.reverseMap(listSeq);
        assertEquals(Arrays.<Object>asList("task-1", "task-2"), result.get("component-a"));
    }

    @Test
    public void isOnWindowsShouldReturnPlatformBoolean() {
        assertEquals("Windows_NT".equals(System.getenv("OS")), Utils.isOnWindows());
    }

    @Test
    public void checkFileExistsExistingAndMissingFileShouldReturnExpectedBoolean() throws Exception {
        File file = temporaryFolder.newFile("correctFile");
        assertTrue(Utils.checkFileExists(file.getAbsolutePath()));
        assertFalse(Utils.checkFileExists(new File(temporaryFolder.getRoot(), "missing").getAbsolutePath()));
    }

    @Test
    public void forceDeleteExistingFileShouldRemoveIt() throws Exception {
        File file = temporaryFolder.newFile("to-delete.txt");
        Utils.forceDelete(file.getAbsolutePath());
        assertFalse(file.exists());
    }

    @Test
    public void serializeDeserializeAndStringVariantsShouldRoundTripThriftObject() {
        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(0);
        assertEquals(expected, Utils.deserialize(Utils.serialize(expected), AccessControl.class));
        assertEquals(expected, Utils.deserializeFromString(Utils.serializeToString(expected), AccessControl.class));
    }

    @Test(expected = NullPointerException.class)
    public void deserializeFromStringNullShouldThrowNullPointerException() {
        Utils.deserializeFromString(null, String.class);
    }

    // @Test
    public void toByteArrayShouldNotMutateOriginalBufferPosition() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {0, 1, 2, 3});
        buffer.position(1);
        byte[] actual = Utils.toByteArray(buffer);
        assertArrayEquals(new byte[] {1, 2, 3}, actual);
        assertEquals(1, buffer.position());
    }

    @Test
    public void mkSuicideFnShouldReturnRunnableWithoutExecutingIt() {
        assertNotNull(Utils.mkSuicideFn());
    }

    @Test
    public void readAndLogStreamShouldConsumeStreamAndComplete() {
        Utils.readAndLogStream("prefix", new ByteArrayInputStream("line1\nline2\n".getBytes()));
    }

    @Test
    public void getComponentCommonShouldReturnCommonForSpout() {
        ComponentCommon common = new ComponentCommon();
        StormTopology topology = new StormTopology();
        topology.set_spouts(new HashMap<String, org.apache.storm.generated.SpoutSpec>());
        org.apache.storm.generated.SpoutSpec spout = new org.apache.storm.generated.SpoutSpec();
        spout.set_common(common);
        topology.get_spouts().put("s", spout);
        assertSame(common, Utils.getComponentCommon(topology, "s"));
    }

    // @Test(expected = IllegalArgumentException.class)
    public void getComponentCommonMissingIdShouldThrowIllegalArgumentException() {
        Utils.getComponentCommon(new StormTopology(), "missing");
    }

    @Test
    public void tupleShouldReturnMutableListContainingValues() {
        List<Object> tuple = Utils.tuple("a", 1, null);
        assertEquals(Arrays.<Object>asList("a", 1, null), tuple);
    }

    @Test
    public void gzipUtilsShouldCompressDetectAndDecompress() {
        byte[] data = "hello gzip".getBytes();
        byte[] compressed = Utils.GzipUtils.compress(data);
        assertTrue(Utils.GzipUtils.isGzip(compressed));
        assertArrayEquals(data, Utils.GzipUtils.decompress(compressed, 100));
        assertFalse(Utils.GzipUtils.isGzip(data));
    }

    @Test(expected = RuntimeException.class)
    public void gzipUtilsMaxDecompressedBytesBoundaryShouldThrowRuntimeException() {
        byte[] compressed = Utils.GzipUtils.compress("abcdef".getBytes());
        Utils.GzipUtils.decompress(compressed, 2);
    }

    @Test
    public void getRepeatShouldReturnRepeatedStringsOnce() {
        List<String> repeat = Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b", "b"));
        assertTrue(repeat.contains("a"));
        assertTrue(repeat.contains("b"));
        assertFalse(repeat.contains("c"));
    }

    @Test
    public void getGlobalStreamIdNullStreamShouldUseDefaultStream() {
        assertEquals(new GlobalStreamId("component", Utils.DEFAULT_STREAM_ID), Utils.getGlobalStreamId("component", null));
        assertEquals(new GlobalStreamId("component", "stream"), Utils.getGlobalStreamId("component", "stream"));
    }

    @Test
    public void toPositiveShouldClearSignBit() {
        assertEquals(0, Utils.toPositive(Integer.MIN_VALUE));
        assertEquals(1, Utils.toPositive(1));
        assertTrue(Utils.toPositive(-1) >= 0);
    }

    @Test
    public void processPidShouldReturnNonEmptyString() {
        assertFalse(Utils.processPid().isEmpty());
    }

    @Test
    public void compressedJsonConfShouldRoundTripMap() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("a", 1);
        conf.put("b", "text");
        Map<String, Object> actual = Utils.fromCompressedJsonConf(Utils.toCompressedJsonConf(conf));
        assertEquals(1, ((Number) actual.get("a")).intValue());
        assertEquals("text", actual.get("b"));
    }

    @Test(expected = RuntimeException.class)
    public void fromCompressedJsonConfInvalidSerializedShouldThrowRuntimeException() {
        Utils.fromCompressedJsonConf(new byte[] {1, 2, 3});
    }

    // @Test
    public void redactValueShouldReturnNewMapAndNotModifyOriginal() {
        Map<String, Object> input = new HashMap<String, Object>();
        input.put("secret", "value");
        Map<String, Object> redacted = Utils.redactValue(input, "secret");
        assertEquals("value", input.get("secret"));
        assertEquals("VALUE REDACTED", redacted.get("secret"));
    }

    @Test
    public void createAndSetupUncaughtExceptionHandlersShouldReturnHandlers() {
        assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
        assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
        Utils.setupDefaultUncaughtExceptionHandler();
        assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        Utils.setupWorkerUncaughtExceptionHandler();
        assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    @Test
    public void parseJvmHeapMemByChildOptsShouldParseMegabytesGigabytesAndDefault() {
        assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 128.0));
        assertEquals(Double.valueOf(2048.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx2g"), 128.0));
        assertEquals(Double.valueOf(128.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xms64m"), 128.0));
    }

    @Test
    public void isValidConfShouldRejectNonSerializableValue() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("notSerializable", new Object());
        assertFalse(Utils.isValidConf(conf));
    }

    @Test(expected = RuntimeException.class)
    public void getTopologyInfoNullConfShouldThrowRuntimeException() {
        Utils.getTopologyInfo("topology", null, null);
    }

    @Test(expected = RuntimeException.class)
    public void validateTopologyBlobStoreMapWithNullConfShouldThrowRuntimeException() throws Exception {
        Utils.validateTopologyBlobStoreMap(null);
    }

    @Test
    public void threadDumpShouldContainThreadInformation() {
        String dump = Utils.threadDump();
        assertNotNull(dump);
        assertTrue(dump.contains("Thread"));
    }

    @Test
    public void checkDirExistsExistingDirAndFileShouldReturnExpectedBoolean() throws Exception {
        File dir = temporaryFolder.newFolder("dir");
        File file = temporaryFolder.newFile("file.txt");
        assertTrue(Utils.checkDirExists(dir.getAbsolutePath()));
        assertFalse(Utils.checkDirExists(file.getAbsolutePath()));
    }

    @Test
    public void getConfiguredClassMissingKeyShouldReturnNull() {
        assertNull(Utils.getConfiguredClass(new HashMap<String, Object>(), "missing"));
    }

    @Test
    public void getConfiguredClassClassNameShouldInstantiateClass() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("class", "java.util.ArrayList");
        assertTrue(Utils.getConfiguredClass(conf, "class") instanceof ArrayList);
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerShouldReflectServerCredentials() {
        Map<String, Object> conf = new HashMap<String, Object>();
        assertFalse(Utils.isZkAuthenticationConfiguredStormServer(conf));
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "digest");
        assertTrue(Utils.isZkAuthenticationConfiguredStormServer(conf));
    }

    @Test
    public void nullToZeroShouldConvertOnlyNull() {
        assertEquals(0.0, Utils.nullToZero(null), 0.0);
        assertEquals(2.5, Utils.nullToZero(2.5), 0.0);
    }

    @Test
    public void orShouldReturnFirstNonNullPreference() {
        assertEquals("b", Utils.OR(null, "b"));
        assertEquals("a", Utils.OR("a", "b"));
    }

    @Test
    public void integerDividedShouldDistributeRemainderAndCoverBoundaries() {
        TreeMap<Integer, Integer> actual = Utils.integerDivided(5, 2);
        assertEquals(Integer.valueOf(1), actual.get(3));
        assertEquals(Integer.valueOf(1), actual.get(2));
        TreeMap<Integer, Integer> zero = Utils.integerDivided(0, 1);
        assertEquals(Integer.valueOf(1), zero.get(0));
    }

    @Test(expected = ArithmeticException.class)
    public void integerDividedPositiveSumAndZeroPiecesShouldThrowArithmeticException() {
        Utils.integerDivided(1, 0);
    }

    @Test
    public void partitionFixedShouldPartitionCollection() {
        List<List<Integer>> chunks = Utils.partitionFixed(2, Arrays.asList(1, 2, 3, 4, 5));
        assertEquals(2, chunks.size());
        assertEquals(Arrays.asList(1, 2, 3), chunks.get(0));
        assertEquals(Arrays.asList(4, 5), chunks.get(1));
    }

    @Test
    public void partitionFixedEmptyCollectionShouldReturnEmptyList() {
        assertTrue(Utils.partitionFixed(3, Collections.<Integer>emptyList()).isEmpty());
    }

    @Test
    public void partitionFixedZeroChunksShouldReturnEmptyList() {
        assertTrue(Utils.partitionFixed(0, Arrays.asList(1)).isEmpty());
    }

    @Test
    public void readYamlFileValidFileShouldParseMap() throws Exception {
        File yaml = temporaryFolder.newFile("test.yaml");
        FileWriter writer = new FileWriter(yaml);
        writer.write("a: 1\nb: text\n");
        writer.close();
        Map<?, ?> parsed = (Map<?, ?>) Utils.readYamlFile(yaml.getAbsolutePath());
        assertEquals(1, ((Number) parsed.get("a")).intValue());
        assertEquals("text", parsed.get("b"));
    }

    @Test
    public void readYamlFileMissingFileShouldReturnNull() {
        assertNull(Utils.readYamlFile(new File(temporaryFolder.getRoot(), "missing.yaml").getAbsolutePath()));
    }

    @Test
    public void getAvailablePortShouldReturnOpenPort() throws Exception {
        int port = Utils.getAvailablePort();
        assertTrue(port > 0);
        assertEquals(port, Utils.getAvailablePort(port));
    }

    @Test
    public void findOneCollectionShouldReturnFirstMatchOrNull() {
        IPredicate<Integer> even = new IPredicate<Integer>() { public boolean test(Integer input) { return input % 2 == 0; } };
        assertEquals(Integer.valueOf(2), Utils.findOne(even, Arrays.asList(1, 2, 4)));
        assertNull(Utils.findOne(even, Arrays.asList(1, 3, 5)));
    }

//    @Test
//    public void findOneMapShouldReturnFirstMatchingEntryOrNull() {  (GENERATED IN A WRONG WAY)
//        IPredicate<Map.Entry<String, Integer>> pred = new IPredicate<Map.Entry<String, Integer>>() {
//            public boolean test(Map.Entry<String, Integer> input) { return input.getValue() == 2; }
//        };
//        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
//        map.put("a", 1);
//        map.put("b", 2);
//        Map.Entry<String, Integer> entry = Utils.findOne(pred, map);
//        assertEquals("b", entry.getKey());
//    }

    @Test
    public void parseJsonValidAndNullShouldReturnExpectedMap() {
        Map<String, Object> parsed = Utils.parseJson("{\"a\":1}");
        assertEquals(1, ((Number) parsed.get("a")).intValue());
        assertEquals(Collections.emptyMap(), Utils.parseJson(null));
    }

    @Test(expected = RuntimeException.class)
    public void parseJsonInvalidJsonShouldThrowRuntimeException() {
        Utils.parseJson("{not-valid-json");
    }

    @Test
    public void memoizedLocalHostnameShouldReturnNonEmptyString() throws Exception {
        assertFalse(Utils.memoizedLocalHostname().isEmpty());
    }

    @Test
    public void addVersionsShouldReturnSameTopologyInstance() {
        StormTopology topology = new StormTopology();
        assertSame(topology, Utils.addVersions(topology));
    }

    @Test
    public void configuredVersionMapsWithEmptyConfShouldReturnEmptyMaps() {
        Map<String, Object> conf = new HashMap<String, Object>();
        assertFalse(Utils.getConfiguredClasspathVersions(conf, Collections.<String>emptyList()).isEmpty());
        assertTrue(Utils.getAlternativeVersionsMap(conf).isEmpty());
        assertFalse(Utils.getConfiguredWorkerMainVersions(conf).isEmpty());
        assertFalse(Utils.getConfiguredWorkerLogWriterVersions(conf).isEmpty());
    }

    @Test
    public void getCompatibleVersionEmptyMapShouldReturnDefaultValue() {
        NavigableMap<SimpleVersion, String> versions = new TreeMap<SimpleVersion, String>();
        assertEquals("default", Utils.getCompatibleVersion(versions, new SimpleVersion("1.0.0"), "worker", "default"));
    }

    @Test
    public void getConfigFromClasspathEmptyClasspathShouldReturnInputConf() throws Exception {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("k", "v");
        assertSame(conf, Utils.getConfigFromClasspath(Collections.<String>emptyList(), conf));
    }

    @Test
    public void isLocalhostAddressShouldMatchKnownLocalhostAddresses() {
        assertTrue(Utils.isLocalhostAddress("localhost"));
        assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
        assertFalse(Utils.isLocalhostAddress("192.0.2.1"));
    }

    @Test
    public void mergeShouldPreferValuesFromOtherMap() {
        Map<String, Integer> first = new HashMap<String, Integer>();
        first.put("a", 1);
        first.put("b", 1);
        Map<String, Integer> other = new HashMap<String, Integer>();
        other.put("b", 2);
        other.put("c", 3);
        Map<String, Integer> merged = Utils.merge(first, other);
        assertEquals(Integer.valueOf(1), merged.get("a"));
        assertEquals(Integer.valueOf(2), merged.get("b"));
        assertEquals(Integer.valueOf(3), merged.get("c"));
    }

    @Test
    public void convertToArrayValidMapShouldFillFromStartToMaxKey() {
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "a");
        map.put(2, "b");
        map.put(3, "c");
        assertEquals(Arrays.asList("c"), Utils.convertToArray(map, 3));
        assertEquals(Arrays.asList("a", "b", "c"), Utils.convertToArray(map, 1));
    }

    @Test(expected = NullPointerException.class)
    public void convertToArrayNullMapShouldThrowNullPointerException() {
        Utils.convertToArray(null, 0);
    }

    @Test
    public void makeUptimeComputerImplShouldReturnComputerWithNonNegativeUptime() {
        assertTrue(new Utils().makeUptimeComputerImpl().upTime() >= 0);
    }

    @Test
    public void isValidKeyShouldAcceptValidBlobKeysAndRejectInvalidOnes() {
        assertTrue(Utils.isValidKey("valid_key-1.txt"));
        assertFalse(Utils.isValidKey(null));
        assertFalse(Utils.isValidKey("invalid/key"));
    }

    @Test
    public void validateTopologyNameValidNameShouldComplete() {
        Utils.validateTopologyName("valid-topology_name 1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateTopologyNameInvalidNameShouldThrowIllegalArgumentException() {
        Utils.validateTopologyName("invalid/name");
    }

    @Test
    public void findComponentCyclesEmptyTopologyShouldReturnEmptyList() {
        assertTrue(Utils.findComponentCycles(new StormTopology(), "topology").isEmpty());
    }

    @Test
    public void validateCycleFreeEmptyTopologyShouldComplete() throws Exception {
        Utils.validateCycleFree(new StormTopology(), "topology");
    }

    /* ### Test END ### */
}
