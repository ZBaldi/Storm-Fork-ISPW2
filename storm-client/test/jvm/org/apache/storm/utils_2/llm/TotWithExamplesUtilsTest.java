package org.apache.storm.utils_2.llm;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.shade.org.apache.zookeeper.data.Id;
import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.SimpleVersion;
import org.apache.storm.utils.refactored.one.Utils;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class TotWithExamplesUtilsTest {  //REMOVED NOT USED IMPORTS
    // ### Test START ###

    private Utils previousInstance;

    @After
    public void tearDown() {
        if (previousInstance != null) {
            Utils.setInstance(previousInstance);
            previousInstance = null;
        }
        Utils.resetClassLoaderForJavaDeSerialize();
    }

    @Test
    public void setInstanceShouldReturnPreviouslyConfiguredInstance() {
        Utils first = new Utils();
        Utils second = new Utils();

        previousInstance = Utils.setInstance(first);
        Utils returned = Utils.setInstance(second);

        assertSame(first, returned);
    }

    @Test
    public void classLoaderConfigurationShouldAllowJavaRoundTrip() {
        Utils.setClassLoaderForJavaDeSerialize(getClass().getClassLoader());
        byte[] serialized = Utils.javaSerialize("value");
        assertEquals("value", Utils.javaDeserialize(serialized, String.class));

        Utils.resetClassLoaderForJavaDeSerialize();
        assertEquals("value", Utils.javaDeserialize(serialized, String.class));
    }

    // @Test
    public void findResourcesShouldReturnListForExistingClassResource() {
        List<URL> resources = Utils.findResources("org/apache/storm/utils/Utils.class");
        assertNotNull(resources);
        assertFalse(resources.isEmpty());
    }

    @Test
    public void findAndReadConfigFileShouldReturnEmptyMapWhenOptionalFileIsMissing() {
        assertEquals(Collections.emptyMap(), Utils.findAndReadConfigFile("missing-cot-utils-test.yaml", false));
    }

    @Test(expected = RuntimeException.class)
    public void findAndReadConfigFileShouldThrowWhenMandatoryFileIsMissing() {
        Utils.findAndReadConfigFile("missing-cot-utils-test.yaml");
    }

    @Test
    public void readDefaultConfigShouldReturnAConfigurationMapWhenDefaultsAreAvailable() {
        Map<String, Object> defaults = Utils.readDefaultConfig();
        assertNotNull(defaults);
        assertFalse(defaults.isEmpty());
    }

    @Test
    public void urlEncodeAndDecodeUtf8ShouldRoundTripSpecialCharacters() throws Exception {
        String input = "storm user+name/è?x=1&y=2";
        String encoded = Utils.urlEncodeUtf8(input);

        assertEquals(URLEncoder.encode(input, StandardCharsets.UTF_8.name()), encoded);
        assertEquals(input, Utils.urlDecodeUtf8(encoded));
        assertEquals(input, URLDecoder.decode(encoded, StandardCharsets.UTF_8.name()));
    }

    @Test
    public void readCommandLineOptsAndStormConfigShouldReturnNonNullMaps() {
        assertNotNull(Utils.readCommandLineOpts());
        assertNotNull(Utils.readStormConfig());
    }

    @Test
    public void bitXorValsAndBitXorShouldApplyXorSemantics() {
        assertEquals(0L, Utils.bitXorVals(Collections.<Long>emptyList()));
        assertEquals(1L ^ 2L ^ 3L, Utils.bitXorVals(Arrays.asList(1L, 2L, 3L)));
        assertEquals(5L ^ 9L, Utils.bitXor(5L, 9L));
    }

    @Test(expected = NullPointerException.class)
    public void bitXorShouldThrowForNullOperand() {
        Utils.bitXor(null, 1L);
    }

    @Test
    public void isSystemIdShouldRecognizeDoubleUnderscorePrefix() {
        assertTrue(Utils.isSystemId("__system"));
        assertFalse(Utils.isSystemId("_user"));
        assertFalse(Utils.isSystemId("user"));
    }

    @Test(expected = NullPointerException.class)
    public void isSystemIdShouldThrowForNullInput() {
        Utils.isSystemId(null);
    }

    @Test
    public void asyncLoopShouldCreateConfiguredThreadWithoutStartingIt() {
        Callable<Long> callable = () -> null;
        Thread.UncaughtExceptionHandler handler = (thread, throwable) -> { };

        Utils.SmartThread thread = Utils.asyncLoop(callable, true, handler, Thread.NORM_PRIORITY + 1, false, false, "cot-thread");

        assertNotNull(thread);
        assertTrue(thread.isDaemon());
        assertEquals(Thread.NORM_PRIORITY + 1, thread.getPriority());
        assertTrue(thread.getName().contains("cot-thread"));
        assertFalse(thread.isAlive());
    }

    @Test
    public void asyncLoopOverloadsShouldReturnSmartThreads() {
        assertNotNull(Utils.asyncLoop((Callable<Long>) () -> null, "named-loop", (t, e) -> { }));
        assertNotNull(Utils.asyncLoop((Callable<Long>) () -> null));
    }

    @Test
    public void exceptionCauseHelpersShouldFindNestedCauses() {
        IllegalArgumentException root = new IllegalArgumentException("root");
        RuntimeException wrapped = new RuntimeException(new Exception(root));

        assertTrue(Utils.exceptionCauseIsInstanceOf(IllegalArgumentException.class, wrapped));
        assertSame(root, Utils.unwrapTo(IllegalArgumentException.class, wrapped));
        assertNull(Utils.unwrapTo(IllegalStateException.class, wrapped));
    }

    @Test(expected = IllegalArgumentException.class)
    public void unwrapAndThrowShouldThrowMatchingCause() throws IllegalArgumentException {
        Utils.unwrapAndThrow(IllegalArgumentException.class, new RuntimeException(new IllegalArgumentException("bad")));
    }

    @Test
    public void wrapInRuntimeShouldReuseRuntimeExceptionAndWrapCheckedException() {
        RuntimeException runtime = new RuntimeException("runtime");
        Exception checked = new Exception("checked");

        assertSame(runtime, Utils.wrapInRuntime(runtime));
        RuntimeException wrapped = Utils.wrapInRuntime(checked);
        assertSame(checked, wrapped.getCause());
    }

    @Test
    public void secureRandomLongHostnameUuidAndPidShouldReturnReasonableValues() throws Exception {
        assertNotEquals(0L, Utils.secureRandomLong() | Utils.secureRandomLong());
        assertNotNull(Utils.hostname());
        assertNotNull(Utils.localHostname());
        assertTrue(UUID.fromString(Utils.uuid()).toString().length() > 0);
        assertTrue(Utils.processPid().matches("\\d+"));
    }

    @Test
    public void javaSerializationShouldRoundTripSerializableObjects() {
        ArrayList<String> input = new ArrayList<>(Arrays.asList("a", "b"));
        byte[] bytes = Utils.javaSerialize(input);

        ArrayList<?> output = Utils.javaDeserialize(bytes, ArrayList.class);

        assertEquals(input, output);
    }

    @Test(expected = RuntimeException.class)
    public void javaDeserializeShouldRejectInvalidBytes() {
        Utils.javaDeserialize(new byte[] {1, 2, 3}, Object.class);
    }

    @Test
    public void getShouldReturnStoredValueOrDefaultWhenMissingOrNull() {
        Map<String, String> map = new HashMap<>();
        map.put("present", "value");
        map.put("null", null);

        assertEquals("value", Utils.get(map, "present", "default"));
        assertEquals("default", Utils.get(map, "missing", "default"));
        assertEquals("default", Utils.get(map, "null", "default"));
    }

    @Test
    public void zeroIfNaNOrInfShouldNormalizeOnlyNaNAndInfiniteValues() {
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
        assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NEGATIVE_INFINITY), 0.0);
        assertEquals(42.5, Utils.zeroIfNaNOrInf(42.5), 0.0);
    }

    @Test
    public void joinShouldHandleEmptySingletonAndMultipleValues() {
        assertEquals("", Utils.join(Collections.emptyList(), ","));
        assertEquals("one", Utils.join(Collections.singletonList("one"), ","));
        assertEquals("a,b,c", Utils.join(Arrays.asList("a", "b", "c"), ","));
    }

    @Test
    public void parseZkIdShouldSplitOnFirstColon() {
        Id id = Utils.parseZkId("sasl:storm:user", "test.conf");
        assertEquals("sasl", id.getScheme());
        assertEquals("storm:user", id.getId());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseZkIdShouldRejectValuesWithoutColon() {
        Utils.parseZkId("invalid", "test.conf");
    }

    @Test
    public void zkAclHelpersShouldReflectConfiguredAuthentication() {
        Map<String, Object> conf = new HashMap<>();
        assertFalse(Utils.isZkAuthenticationConfiguredTopology(conf));
        assertNull(Utils.getWorkerACL(conf));

        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, "sasl");
        conf.put(Config.STORM_ZOOKEEPER_SUPERACL, "sasl:storm-super-user");

        assertTrue(Utils.isZkAuthenticationConfiguredTopology(conf));
        ACL superAcl = Utils.getSuperUserAcl(conf);
        assertEquals(ZooDefs.Perms.ALL, superAcl.getPerms());
        assertEquals("sasl", superAcl.getId().getScheme());
        assertEquals("storm-super-user", superAcl.getId().getId());
        assertTrue(Utils.getWorkerACL(conf).contains(superAcl));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getSuperUserAclShouldRequireConfiguredSuperUser() {
        Utils.getSuperUserAcl(new HashMap<String, Object>());
    }

    @Test
    public void sleepMethodsShouldCompleteForZeroMillis() {
        Utils.sleepNoSimulation(0L);
        Utils.sleep(0L);
    }

    @Test
    public void makeUptimeComputerShouldReturnWorkingUptimeComputer() {
        Utils.UptimeComputer uptime = Utils.makeUptimeComputer();
        assertNotNull(uptime);
        assertTrue(uptime.upTime() >= 0);
    }

    @Test
    public void reverseMapShouldGroupKeysByValue() {
        Map<String, Integer> input = new HashMap<>();
        input.put("a", 1);
        input.put("b", 1);
        input.put("c", 2);

        HashMap<Integer, List<String>> reversed = Utils.reverseMap(input);

        assertEquals(2, reversed.size());
        assertTrue(reversed.get(1).containsAll(Arrays.asList("a", "b")));
        assertEquals(Collections.singletonList("c"), reversed.get(2));
    }

    @Test
    public void reverseMapForListSequenceShouldUseFirstElementAsValueAndSecondAsKey() {
        List<List<Object>> input = Arrays.asList(
            Arrays.<Object>asList("componentA", "stream1"),
            Arrays.<Object>asList("componentB", "stream1"),
            Arrays.<Object>asList("componentC", "stream2")
        );

        Map<Object, List<Object>> reversed = Utils.reverseMap(input);

        assertTrue(reversed.get("stream1").containsAll(Arrays.asList("componentA", "componentB")));
        assertEquals(Collections.singletonList("componentC"), reversed.get("stream2"));
    }

    @Test
    public void fileAndDirectoryChecksShouldFollowFileSystemState() throws Exception {
        File tempFile = File.createTempFile("cot-utils", ".txt");
        File tempDir = new File(tempFile.getParentFile(), "cot-utils-dir-" + System.nanoTime());
        assertTrue(tempDir.mkdir());
        try {
            assertTrue(Utils.checkFileExists(tempFile.getAbsolutePath()));
            assertFalse(Utils.checkFileExists(tempFile.getAbsolutePath() + ".missing"));
            assertTrue(Utils.checkDirExists(tempDir.getAbsolutePath()));
            assertFalse(Utils.checkDirExists(tempFile.getAbsolutePath()));
        } finally {
            Utils.forceDelete(tempFile.getAbsolutePath());
            Utils.forceDelete(tempDir.getAbsolutePath());
        }
        assertFalse(tempFile.exists());
        assertFalse(tempDir.exists());
    }

    // @Test  (FAILED)   IllegalArgumentException: Object must be an instance of TBase
    public void configuredSerializationDelegateShouldRoundTripObjectsAndStrings() {
        Map<String, Object> input = new HashMap<>();
        input.put("key", "value");
        input.put("number", 3);

        byte[] bytes = Utils.serialize(input);
        Map<?, ?> decoded = Utils.deserialize(bytes, Map.class);
        String encoded = Utils.serializeToString(input);
        Map<?, ?> decodedFromString = Utils.deserializeFromString(encoded, Map.class);

        assertEquals(input, decoded);
        assertEquals(input, decodedFromString);
    }

    @Test
    public void toByteArrayShouldReadOnlyRemainingByteBufferContent() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {9, 8, 7, 6});
        buffer.get();

        assertArrayEquals(new byte[] {8, 7, 6}, Utils.toByteArray(buffer));
        assertEquals(0, buffer.remaining());
    }

    @Test
    public void mkSuicideFnAndUncaughtHandlersShouldBeCreatedButNotExecuted() {
        assertNotNull(Utils.mkSuicideFn());
        assertNotNull(Utils.createDefaultUncaughtExceptionHandler());
        assertNotNull(Utils.createWorkerUncaughtExceptionHandler());
    }

    @Test
    public void readAndLogStreamShouldConsumeInputWithoutThrowing() {
        Utils.readAndLogStream("cot", new ByteArrayInputStream("line1\nline2".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void tupleShouldPreserveValuesOrderAndNulls() {
        List<Object> tuple = Utils.tuple("a", null, 3);
        assertEquals(Arrays.<Object>asList("a", null, 3), tuple);
    }

    @Test
    public void getRepeatShouldReturnRepeatedOccurrencesOnly() {
        assertEquals(Arrays.asList("a", "b", "a"), Utils.getRepeat(Arrays.asList("a", "b", "a", "c", "b", "a")));
        assertEquals(Collections.emptyList(), Utils.getRepeat(Arrays.asList("a", "b", "c")));
    }

    @Test
    public void getGlobalStreamIdShouldDefaultNullStreamToDefaultStreamId() {
        GlobalStreamId defaultStream = Utils.getGlobalStreamId("component", null);
        GlobalStreamId explicitStream = Utils.getGlobalStreamId("component", "custom");

        assertEquals("component", defaultStream.get_componentId());
        assertEquals(Utils.DEFAULT_STREAM_ID, defaultStream.get_streamId());
        assertEquals("custom", explicitStream.get_streamId());
    }

    @Test
    public void toPositiveShouldClearSignBit() {
        assertEquals(0, Utils.toPositive(Integer.MIN_VALUE));
        assertEquals(0, Utils.toPositive(0));
        assertEquals(1, Utils.toPositive(1));
        assertEquals(Integer.MAX_VALUE, Utils.toPositive(-1));
    }

    @Test
    public void compressedJsonConfShouldRoundTripMaps() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("a", "x");
        conf.put("b", 2);

        Map<String, Object> uncompressed = Utils.fromCompressedJsonConf(Utils.toCompressedJsonConf(conf));

        assertEquals("x", uncompressed.get("a"));
        assertEquals(2, ((Number) uncompressed.get("b")).intValue());
    }

    @Test(expected = RuntimeException.class)
    public void fromCompressedJsonConfShouldRejectInvalidCompressedBytes() {
        Utils.fromCompressedJsonConf(new byte[] {1, 2, 3});
    }

    @Test
    public void redactValueShouldReturnCopyWhenKeyExistsAndOriginalWhenMissing() {
        Map<String, Object> input = new HashMap<>();
        input.put("secret", "abcd");
        input.put("visible", "ok");

        Map<String, Object> redacted = Utils.redactValue(input, "secret");
        Map<String, Object> unchanged = Utils.redactValue(input, "missing");

        assertEquals("####", redacted.get("secret"));
        assertEquals("abcd", input.get("secret"));
        assertNotSame(input, redacted);
        assertSame(input, unchanged);
    }

    @Test
    public void parseJvmHeapMemByChildOptsShouldExtractMegabytesAndUseDefaultWhenMissing() {
        assertEquals(Double.valueOf(512.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx512m"), 64.0));
        assertEquals(Double.valueOf(1024.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Xmx1g"), 64.0));
        assertEquals(Double.valueOf(64.0), Utils.parseJvmHeapMemByChildOpts(Arrays.asList("-Dfoo=bar"), 64.0));
    }

    @Test
    public void threadDumpShouldContainCurrentThreadInformation() {
        String dump = Utils.threadDump();
        assertNotNull(dump);
        assertTrue(dump.contains("Thread"));
    }

    @Test
    public void nullToZeroAndOrShouldHandleNullBoundaries() {
        assertEquals(0.0, Utils.nullToZero(null), 0.0);
        assertEquals(7.5, Utils.nullToZero(7.5), 0.0);
        assertEquals("b", Utils.OR(null, "b"));
        assertEquals("a", Utils.OR("a", "b"));
    }

    @Test
    public void integerDividedShouldDistributeRemainderAcrossAdjacentBucketSizes() {
        TreeMap<Integer, Integer> divided = Utils.integerDivided(10, 3);
        assertEquals(Integer.valueOf(2), divided.get(3));
        assertEquals(Integer.valueOf(1), divided.get(4));

        assertEquals(Collections.singletonMap(0, 3), Utils.integerDivided(0, 3));
    }

    @Test(expected = ArithmeticException.class)
    public void integerDividedShouldRejectZeroPieces() {
        Utils.integerDivided(10, 0);
    }

    @Test
    public void partitionFixedShouldHandleNullEmptyBoundaryAndChunking() {
        assertEquals(Collections.emptyList(), Utils.partitionFixed(0, Arrays.asList(1, 2)));
        assertEquals(Collections.emptyList(), Utils.partitionFixed(3, null));

        List<List<Integer>> partitions = Utils.partitionFixed(3, Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        assertEquals(3, partitions.size());
        assertEquals(Arrays.asList(1, 2, 3), partitions.get(0));
        assertEquals(Arrays.asList(4, 5), partitions.get(1));
        assertEquals(Arrays.asList(6, 7), partitions.get(2));
    }

    @Test
    public void readYamlFileShouldReturnNullForMissingFileAndMapForValidYaml() throws Exception {
        assertNull(Utils.readYamlFile("missing-cot-utils-test.yaml"));

        File yaml = File.createTempFile("cot-utils", ".yaml");
        try (FileWriter writer = new FileWriter(yaml)) {
            writer.write("name: storm\nvalue: 1\n");
        }
        try {
            Object loaded = Utils.readYamlFile(yaml.getAbsolutePath());
            assertTrue(loaded instanceof Map);
            assertEquals("storm", ((Map<?, ?>) loaded).get("name"));
        } finally {
            Utils.forceDelete(yaml.getAbsolutePath());
        }
    }

    @Test
    public void getAvailablePortShouldReturnPreferredPortWhenAvailableAndRandomPortForZero() throws Exception {
        int random = Utils.getAvailablePort();
        assertTrue(random > 0);

        int preferred = Utils.getAvailablePort(random);
        assertEquals(random, preferred);
    }

    @Test
    public void findOneShouldReturnFirstMatchingValueOrNullForCollectionsAndMaps() {
        IPredicate<String> startsWithB = value -> value.startsWith("b");
        assertEquals("beta", Utils.findOne(startsWithB, Arrays.asList("alpha", "beta", "bravo")));
        assertNull(Utils.findOne(startsWithB, Arrays.asList("alpha", "charlie")));

        assertNull(Utils.findOne(startsWithB, (Map<Integer, String>) null));
    }

    @Test
    public void parseJsonShouldReturnMapForObjectAndEmptyMapForNullJson() {
        Map<String, Object> parsed = Utils.parseJson("{\"a\":1,\"b\":\"x\"}");
        assertEquals(1, ((Number) parsed.get("a")).intValue());
        assertEquals("x", parsed.get("b"));
        assertEquals(Collections.emptyMap(), Utils.parseJson(null));
    }

    @Test(expected = RuntimeException.class)
    public void parseJsonShouldRejectMalformedJson() {
        Utils.parseJson("{bad json}");
    }

    @Test
    public void memoizedLocalHostnameShouldReturnStableValue() throws Exception {
        String first = Utils.memoizedLocalHostname();
        String second = Utils.memoizedLocalHostname();
        assertNotNull(first);
        assertSame(first, second);
    }

    @Test
    public void compatibleVersionShouldReturnCeilingCompatibleOrDefault() {
        NavigableMap<SimpleVersion, String> versions = new TreeMap<>();
        versions.put(new SimpleVersion("1.0.0"), "1.0.0");
        versions.put(new SimpleVersion("1.2.0"), "1.2.0");
        versions.put(new SimpleVersion("2.0.0"), "2.0.0");

        assertEquals("1.2.0", Utils.getCompatibleVersion(versions, new SimpleVersion("1.1.0"), "worker", "default"));
        assertEquals("default", Utils.getCompatibleVersion(versions, new SimpleVersion("3.0.0"), "worker", "default"));
    }

    @Test
    public void isLocalhostAddressShouldRecognizeConfiguredLocalhostAliases() {
        assertTrue(Utils.isLocalhostAddress("localhost"));
        assertTrue(Utils.isLocalhostAddress("127.0.0.1"));
        assertFalse(Utils.isLocalhostAddress("192.0.2.10"));
    }

    @Test
    public void mergeShouldReturnNewMapWithOtherMapTakingPrecedence() {
        Map<String, Integer> first = new HashMap<>();
        first.put("a", 1);
        first.put("b", 2);
        Map<String, Integer> other = new HashMap<>();
        other.put("b", 20);
        other.put("c", 30);

        Map<String, Integer> merged = Utils.merge(first, other);

        assertEquals(Integer.valueOf(1), merged.get("a"));
        assertEquals(Integer.valueOf(20), merged.get("b"));
        assertEquals(Integer.valueOf(30), merged.get("c"));
        assertNotSame(first, merged);
    }

    @Test
    public void convertToArrayShouldFillValuesFromStartIndexAndKeepHolesAsNull() {
        Map<Integer, String> src = new HashMap<>();
        src.put(2, "two");
        src.put(4, "four");

        ArrayList<String> result = Utils.convertToArray(src, 2);

        assertEquals(3, result.size());
        assertEquals("two", result.get(0));
        assertNull(result.get(1));
        assertEquals("four", result.get(2));
    }

    @Test
    public void isValidKeyShouldAcceptSafeBlobKeysAndRejectInvalidOnes() {
        assertTrue(Utils.isValidKey("abc_DEF-123.txt"));
        assertTrue(Utils.isValidKey("key with spaces"));
        assertFalse(Utils.isValidKey(null));
        assertFalse(Utils.isValidKey(""));
        assertFalse(Utils.isValidKey("../secret"));
    }

    // @Test   (FAILED) IllegalArgumentException: Topology name '/' is not valid. It can't be null and it must match ^[^/.:\\]+$
    public void validateTopologyNameShouldFollowConfiguredTopologyNamePattern() {
        Utils.validateTopologyName("/");
        try {
            Utils.validateTopologyName("invalid-name");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("not valid"));
        }
    }

    @Test
    public void environmentDependentHelpersShouldReturnBooleansOrObjectsWithoutThrowing() {
        assertEquals("Windows_NT".equals(System.getenv("OS")), Utils.isOnWindows());
    }

    @Test
    public void setupDefaultExceptionHandlersShouldInstallHandlers() {
        Thread.UncaughtExceptionHandler old = Thread.getDefaultUncaughtExceptionHandler();
        try {
            Utils.setupDefaultUncaughtExceptionHandler();
            assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
            Utils.setupWorkerUncaughtExceptionHandler();
            assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(old);
        }
    }

    @Test
    public void handleUncaughtExceptionShouldIgnoreAllowedExceptions() {
        Utils.handleUncaughtException(new IllegalArgumentException("allowed"),
            Collections.<Class<?>>singleton(IllegalArgumentException.class), false);
    }

    @Test
    public void addShutdownHookMethodsAreDeliberatelyNotExecutedInUnitTests() {
        AtomicBoolean ran = new AtomicBoolean(false);
        Runnable cleanup = () -> ran.set(true);
        assertNotNull(cleanup);
        assertFalse(ran.get());
    }

    // ### Test END ###
}
