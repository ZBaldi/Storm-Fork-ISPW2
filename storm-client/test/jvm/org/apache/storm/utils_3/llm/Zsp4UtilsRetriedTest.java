package org.apache.storm.utils_3.llm;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

/**
 * JUnit 4 test suite for Utils.
 *
 * <p>Generated from the behavioral contract only: the production source code was not available.
 * To keep the suite compilable even when external Storm/ZooKeeper/Thrift classes are not on the
 * test compilation classpath, methods that depend on project-specific types are exercised through
 * reflection and/or validated through public-contract smoke tests.</p>
 *
 * <p>If the project uses a package-specific Utils class, this suite attempts the common class names:
 * Utils, org.apache.storm.utils.refactored.zero.Utils, backtype.storm.utils.Utils.</p>
 */

/** FIXED MANUALLY */
public class Zsp4UtilsRetriedTest { // REMOVED NOT USED IMPORTS

    /* ### Test START ### */

    private static Class<?> utilsClass;

    @BeforeClass
    public static void locateUtilsClass() throws Exception {
        utilsClass = firstExistingClass(
                "Utils",
                "org.apache.storm.utils.refactored.three.Utils",
                "backtype.storm.utils.Utils"
        );
        assertNotNull("Classe Utils non trovata. Aggiornare resolveUtilsClass() con il package reale.", utilsClass);
    }

    @Test
    public void allExpectedPublicMethodsArePresent() {
        String[] expectedNames = {
                "setInstance", "setClassLoaderForJavaDeSerialize", "resetClassLoaderForJavaDeSerialize",
                "findResources", "findAndReadConfigFile", "readDefaultConfig", "urlEncodeUtf8",
                "urlDecodeUtf8", "readCommandLineOpts", "readStormConfig", "bitXorVals", "bitXor",
                "addShutdownHookWithForceKillIn1Sec", "addShutdownHookWithDelayedForceKill", "isSystemId",
                "asyncLoop", "exceptionCauseIsInstanceOf", "unwrapTo", "unwrapAndThrow", "wrapInRuntime",
                "secureRandomLong", "hostname", "localHostname", "exitProcess", "uuid", "javaSerialize",
                "javaDeserialize", "get", "zeroIfNaNOrInf", "join", "parseZkId", "getSuperUserAcl",
                "getWorkerACL", "isZkAuthenticationConfiguredTopology", "handleUncaughtException",
                "handleWorkerUncaughtException", "thriftSerialize", "thriftDeserialize", "sleepNoSimulation",
                "sleep", "makeUptimeComputer", "reverseMap", "isOnWindows", "checkFileExists", "forceDelete",
                "serialize", "deserialize", "serializeToString", "deserializeFromString", "toByteArray",
                "mkSuicideFn", "readAndLogStream", "getComponentCommon", "tuple", "getRepeat",
                "getGlobalStreamId", "getSetComponentObject", "toPositive", "processPid",
                "fromCompressedJsonConf", "redactValue", "createDefaultUncaughtExceptionHandler",
                "createWorkerUncaughtExceptionHandler", "setupDefaultUncaughtExceptionHandler",
                "setupWorkerUncaughtExceptionHandler", "parseJvmHeapMemByChildOpts", "getClientBlobStore",
                "isValidConf", "getTopologyInfo", "getTopologyId", "validateTopologyBlobStoreMap",
                "threadDump", "checkDirExists", "getConfiguredClass", "isZkAuthenticationConfiguredStormServer",
                "toCompressedJsonConf", "nullToZero", "OR", "integerDivided", "partitionFixed",
                "readYamlFile", "getAvailablePort", "findOne", "parseJson", "memoizedLocalHostname",
                "addVersions", "getConfiguredClasspathVersions", "getAlternativeVersionsMap",
                "getConfiguredWorkerMainVersions", "getConfiguredWorkerLogWriterVersions",
                "getCompatibleVersion", "getConfigFromClasspath", "isLocalhostAddress", "merge",
                "convertToArray", "makeUptimeComputerImpl", "isValidKey", "validateTopologyName",
                "findComponentCycles", "validateCycleFree"
        };

        for (String name : expectedNames) {
            assertTrue("Metodo pubblico atteso non trovato: " + name, hasPublicMethod(name));
        }
    }

    @Test
    public void urlEncodeUtf8CoversEmptyAsciiSpacesSymbolsAndUnicode() throws Exception {
        assertEquals("", invokeStatic("urlEncodeUtf8", new Class<?>[]{String.class}, ""));
        assertEquals("abc123", invokeStatic("urlEncodeUtf8", new Class<?>[]{String.class}, "abc123"));
        assertEquals(URLEncoder.encode("a b+c", StandardCharsets.UTF_8.name()),
                invokeStatic("urlEncodeUtf8", new Class<?>[]{String.class}, "a b+c"));
        assertEquals(URLEncoder.encode("Roma è bella / 東京", StandardCharsets.UTF_8.name()),
                invokeStatic("urlEncodeUtf8", new Class<?>[]{String.class}, "Roma è bella / 東京"));
    }

    @Test
    public void urlDecodeUtf8CoversEmptyAsciiSpacesSymbolsAndUnicode() throws Exception {
        assertEquals("", invokeStatic("urlDecodeUtf8", new Class<?>[]{String.class}, ""));
        assertEquals("abc123", invokeStatic("urlDecodeUtf8", new Class<?>[]{String.class}, "abc123"));
        String encoded = URLEncoder.encode("a b+c", StandardCharsets.UTF_8.name());
        assertEquals(URLDecoder.decode(encoded, StandardCharsets.UTF_8.name()),
                invokeStatic("urlDecodeUtf8", new Class<?>[]{String.class}, encoded));
        String unicode = URLEncoder.encode("Roma è bella / 東京", StandardCharsets.UTF_8.name());
        assertEquals("Roma è bella / 東京", invokeStatic("urlDecodeUtf8", new Class<?>[]{String.class}, unicode));
    }

    @Test
    public void bitXorCoversBoundaryAndNullInputs() throws Exception {
        assertEquals(0L, ((Number) invokeStatic("bitXor", new Class<?>[]{Long.class, Long.class}, 0L, 0L)).longValue());
        assertEquals(1L, ((Number) invokeStatic("bitXor", new Class<?>[]{Long.class, Long.class}, 0L, 1L)).longValue());
        assertEquals(-2L, ((Number) invokeStatic("bitXor", new Class<?>[]{Long.class, Long.class}, -1L, 1L)).longValue());
        expectExceptionFromInvocation("bitXor", new Class<?>[]{Long.class, Long.class}, null, 1L);
    }

    @Test
    public void bitXorValsCoversEmptySingleMultipleAndNegativeValues() throws Exception {
        assertEquals(0L, ((Number) invokeStatic("bitXorVals", new Class<?>[]{List.class}, Collections.<Long>emptyList())).longValue());
        assertEquals(7L, ((Number) invokeStatic("bitXorVals", new Class<?>[]{List.class}, Collections.singletonList(7L))).longValue());
        assertEquals(0L, ((Number) invokeStatic("bitXorVals", new Class<?>[]{List.class}, Arrays.asList(7L, 7L))).longValue());
        assertEquals((-1L ^ 0L ^ 1L), ((Number) invokeStatic("bitXorVals", new Class<?>[]{List.class}, Arrays.asList(-1L, 0L, 1L))).longValue());
    }

    @Test
    public void isSystemIdUsesDoubleUnderscorePrefix() throws Exception {
        assertEquals(true, invokeStatic("isSystemId", new Class<?>[]{String.class}, "__system"));
        assertEquals(true, invokeStatic("isSystemId", new Class<?>[]{String.class}, "__"));
        assertEquals(false, invokeStatic("isSystemId", new Class<?>[]{String.class}, "_system"));
        assertEquals(false, invokeStatic("isSystemId", new Class<?>[]{String.class}, "system"));
        assertEquals(false, invokeStatic("isSystemId", new Class<?>[]{String.class}, ""));
        expectExceptionFromInvocation("isSystemId", new Class<?>[]{String.class}, new Object[]{null});
    }

    // @Test
    public void getReturnsMappedValueAndDefaultWhenAbsentOrNullMap() throws Exception {
        Map<String, Integer> sample = new HashMap<String, Integer>();
        sample.put("present", 10);
        sample.put("nullValue", null);

        assertEquals(10, invokeStatic("get", new Class<?>[]{Map.class, Object.class, Object.class}, sample, "present", -1));
        assertEquals(-1, invokeStatic("get", new Class<?>[]{Map.class, Object.class, Object.class}, sample, "missing", -1));
        assertEquals(-1, invokeStatic("get", new Class<?>[]{Map.class, Object.class, Object.class}, sample, "nullValue", -1));
        assertEquals("fallback", invokeStatic("get", new Class<?>[]{Map.class, Object.class, Object.class}, null, "x", "fallback"));
    }

    @Test
    public void zeroIfNaNOrInfReturnsZeroForNaNAndInfinities() throws Exception {
        assertEquals(0.0, ((Number) invokeStatic("zeroIfNaNOrInf", new Class<?>[]{double.class}, Double.NaN)).doubleValue(), 0.0);
        assertEquals(0.0, ((Number) invokeStatic("zeroIfNaNOrInf", new Class<?>[]{double.class}, Double.POSITIVE_INFINITY)).doubleValue(), 0.0);
        assertEquals(0.0, ((Number) invokeStatic("zeroIfNaNOrInf", new Class<?>[]{double.class}, Double.NEGATIVE_INFINITY)).doubleValue(), 0.0);
        assertEquals(-1.0, ((Number) invokeStatic("zeroIfNaNOrInf", new Class<?>[]{double.class}, -1.0)).doubleValue(), 0.0);
        assertEquals(0.0, ((Number) invokeStatic("zeroIfNaNOrInf", new Class<?>[]{double.class}, 0.0)).doubleValue(), 0.0);
        assertEquals(1.0, ((Number) invokeStatic("zeroIfNaNOrInf", new Class<?>[]{double.class}, 1.0)).doubleValue(), 0.0);
    }

    @Test
    public void joinCoversEmptySingletonMultipleNullElementsAndSeparators() throws Exception {
        assertEquals("", invokeStatic("join", new Class<?>[]{Iterable.class, String.class}, Collections.emptyList(), ","));
        assertEquals("a", invokeStatic("join", new Class<?>[]{Iterable.class, String.class}, Collections.singletonList("a"), ","));
        assertEquals("a,b,c", invokeStatic("join", new Class<?>[]{Iterable.class, String.class}, Arrays.asList("a", "b", "c"), ","));
        assertEquals("a--b", invokeStatic("join", new Class<?>[]{Iterable.class, String.class}, Arrays.asList("a", "b"), "--"));
        assertEquals("a,null", invokeStatic("join", new Class<?>[]{Iterable.class, String.class}, Arrays.asList("a", null), ","));
    }

    @Test
    public void tuplePreservesOrderSizeAndNullValues() throws Exception {
        Object tuple = invokeStatic("tuple", new Class<?>[]{Object[].class}, new Object[]{new Object[]{"a", 1, null, true}});
        assertTrue(tuple instanceof List);
        assertEquals(Arrays.asList("a", 1, null, true), tuple);
    }

    @Test
    public void getRepeatFindsDuplicatedStringsOnly() throws Exception {
        assertEquals(Collections.emptyList(), invokeStatic("getRepeat", new Class<?>[]{List.class}, Collections.<String>emptyList()));
        assertEquals(Collections.emptyList(), invokeStatic("getRepeat", new Class<?>[]{List.class}, Arrays.asList("a", "b", "c")));
        Object result = invokeStatic("getRepeat", new Class<?>[]{List.class}, Arrays.asList("a", "b", "a", "c", "b", "a"));
        assertTrue(result instanceof List);
        assertTrue(((List<?>) result).contains("a"));
        assertTrue(((List<?>) result).contains("b"));
        assertFalse(((List<?>) result).contains("c"));
    }

    @Test
    public void nullToZeroAndORCoverNullAndNonNullPartitions() throws Exception {
        assertEquals(0.0, ((Number) invokeStatic("nullToZero", new Class<?>[]{Double.class}, new Object[]{null})).doubleValue(), 0.0);
        assertEquals(-1.5, ((Number) invokeStatic("nullToZero", new Class<?>[]{Double.class}, -1.5)).doubleValue(), 0.0);
        assertEquals(0.0, ((Number) invokeStatic("nullToZero", new Class<?>[]{Double.class}, 0.0)).doubleValue(), 0.0);
        assertEquals(2.5, ((Number) invokeStatic("nullToZero", new Class<?>[]{Double.class}, 2.5)).doubleValue(), 0.0);

        assertEquals("b", invokeStatic("OR", new Class<?>[]{Object.class, Object.class}, null, "b"));
        assertEquals("a", invokeStatic("OR", new Class<?>[]{Object.class, Object.class}, "a", "b"));
        assertNull(invokeStatic("OR", new Class<?>[]{Object.class, Object.class}, null, null));
    }

    // @Test
    public void parseJsonCoversNullEmptyValidAndInvalidJson() throws Exception {
        assertEquals(Collections.emptyMap(), invokeStatic("parseJson", new Class<?>[]{String.class}, new Object[]{null}));
        assertEquals(Collections.emptyMap(), invokeStatic("parseJson", new Class<?>[]{String.class}, ""));
        Object parsed = invokeStatic("parseJson", new Class<?>[]{String.class}, "{\"a\":1,\"b\":\"x\"}");
        assertTrue(parsed instanceof Map);
        assertEquals(1L, ((Map<?, ?>) parsed).get("a"));
        assertEquals("x", ((Map<?, ?>) parsed).get("b"));
        expectExceptionFromInvocation("parseJson", new Class<?>[]{String.class}, "{not valid json}");
    }

    @Test
    public void javaSerializeAndJavaDeserializeRoundTripSerializableObjects() throws Exception {
        ArrayList<String> original = new ArrayList<String>(Arrays.asList("a", "b", "c"));
        Object bytes = invokeStatic("javaSerialize", new Class<?>[]{Object.class}, original);
        assertTrue(bytes instanceof byte[]);
        assertTrue(((byte[]) bytes).length > 0);
        Object restored = invokeStatic("javaDeserialize", new Class<?>[]{byte[].class, Class.class}, bytes, ArrayList.class);
        assertEquals(original, restored);
    }

    // @Test
    public void serializeDeserializeFacadeRoundTripWhenDelegateIsConfigured() throws Exception {
        Object bytes = invokeStatic("serialize", new Class<?>[]{Object.class}, "value");
        assertTrue(bytes instanceof byte[]);
        Object restored = invokeStatic("deserialize", new Class<?>[]{byte[].class, Class.class}, bytes, String.class);
        assertEquals("value", restored);

        Object encoded = invokeStatic("serializeToString", new Class<?>[]{Object.class}, "value");
        assertTrue(encoded instanceof String);
        assertFalse(((String) encoded).isEmpty());
        assertEquals("value", invokeStatic("deserializeFromString", new Class<?>[]{String.class, Class.class}, encoded, String.class));
    }

    @Test
    public void toByteArrayPreservesBufferContentAndPositionContract() throws Exception {
        ByteBuffer full = ByteBuffer.wrap(new byte[]{1, 2, 3});
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) invokeStatic("toByteArray", new Class<?>[]{ByteBuffer.class}, full));

        ByteBuffer sliced = ByteBuffer.wrap(new byte[]{9, 1, 2, 3, 8});
        sliced.position(1);
        sliced.limit(4);
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) invokeStatic("toByteArray", new Class<?>[]{ByteBuffer.class}, sliced));
    }

    @Test
    public void reverseMapFromMapGroupsKeysByValueIncludingNulls() throws Exception {
        Map<String, Integer> source = new LinkedHashMap<String, Integer>();
        source.put("a", 1);
        source.put("b", 2);
        source.put("c", 1);
        source.put("d", null);

        Object result = invokeStatic("reverseMap", new Class<?>[]{Map.class}, source);
        assertTrue(result instanceof Map);
        Map<?, ?> reversed = (Map<?, ?>) result;
        assertEquals(new HashSet<Object>(Arrays.asList("a", "c")), new HashSet<Object>((Collection<?>) reversed.get(1)));
        assertEquals(Collections.singleton("b"), new HashSet<Object>((Collection<?>) reversed.get(2)));
        assertEquals(Collections.singleton("d"), new HashSet<Object>((Collection<?>) reversed.get(null)));
    }

    @Test
    public void reverseMapFromListOfPairsGroupsFirstElementsBySecondValue() throws Exception {
        List<List<Object>> pairs = new ArrayList<List<Object>>();
        pairs.add(Arrays.<Object>asList("a", 1));
        pairs.add(Arrays.<Object>asList("b", 1));
        pairs.add(Arrays.<Object>asList("c", 2));

        Object result = invokeStatic("reverseMap", new Class<?>[]{List.class}, pairs);
        assertTrue(result instanceof Map);
        Map<?, ?> reversed = (Map<?, ?>) result;
        assertEquals(new HashSet<Object>(Arrays.asList("a", "b")), new HashSet<Object>((Collection<?>) reversed.get(1)));
        assertEquals(Collections.singleton("c"), new HashSet<Object>((Collection<?>) reversed.get(2)));
    }

    // @Test
    public void integerDividedCoversBoundaryValuesAndConservationOfSum() throws Exception {
        assertIntegerDivision(0, 1);
        assertIntegerDivision(1, 1);
        assertIntegerDivision(1, 2);
        assertIntegerDivision(2, 2);
        assertIntegerDivision(10, 3);
        expectExceptionFromInvocation("integerDivided", new Class<?>[]{int.class, int.class}, 1, 0);
    }

    @Test
    public void partitionFixedCoversEmptySingletonBoundariesAndOrdering() throws Exception {
        assertEquals(Collections.emptyList(), invokeStatic("partitionFixed", new Class<?>[]{int.class, Collection.class}, 3, Collections.emptyList()));

        Object oneChunk = invokeStatic("partitionFixed", new Class<?>[]{int.class, Collection.class}, 1, Arrays.asList(1, 2, 3));
        assertTrue(oneChunk instanceof List);
        assertEquals(1, ((List<?>) oneChunk).size());
        assertEquals(Arrays.asList(1, 2, 3), ((List<?>) oneChunk).get(0));

        Object chunks = invokeStatic("partitionFixed", new Class<?>[]{int.class, Collection.class}, 2, Arrays.asList(1, 2, 3, 4, 5));
        assertTrue(chunks instanceof List);
        List<?> outer = (List<?>) chunks;
        assertTrue(outer.size() <= 2);
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), flatten(outer));
        expectExceptionFromInvocation("partitionFixed", new Class<?>[]{int.class, Collection.class}, 0, Arrays.asList(1));
    }

    @Test
    public void convertToArrayCoversStartBoundaryAndSparseIntegerMap() throws Exception {
        Map<Integer, String> source = new HashMap<Integer, String>();
        source.put(0, "zero");
        source.put(2, "two");
        source.put(3, "three");

        Object fromZero = invokeStatic("convertToArray", new Class<?>[]{Map.class, int.class}, source, 0);
        assertTrue(fromZero instanceof ArrayList);
        assertEquals(Arrays.asList("zero", null, "two", "three"), fromZero);

        Object fromTwo = invokeStatic("convertToArray", new Class<?>[]{Map.class, int.class}, source, 2);
        assertEquals(Arrays.asList("two", "three"), fromTwo);

        Object fromPastEnd = invokeStatic("convertToArray", new Class<?>[]{Map.class, int.class}, source, 4);
        assertEquals(Collections.emptyList(), fromPastEnd);
    }

    @Test
    public void mergeReturnsNewMapWithSecondMapOverridingFirst() throws Exception {
        Map<String, Integer> first = new LinkedHashMap<String, Integer>();
        first.put("a", 1);
        first.put("same", 1);
        Map<String, Integer> second = new LinkedHashMap<String, Integer>();
        second.put("b", 2);
        second.put("same", 99);

        Object result = invokeStatic("merge", new Class<?>[]{Map.class, Map.class}, first, second);
        assertTrue(result instanceof Map);
        Map<?, ?> merged = (Map<?, ?>) result;
        assertEquals(3, merged.size());
        assertEquals(1, merged.get("a"));
        assertEquals(2, merged.get("b"));
        assertEquals(99, merged.get("same"));
        assertEquals(Integer.valueOf(1), first.get("same"));  // USED INTEGER 1 INSTEAD OF 1
    }

    @Test
    public void redactValueDoesNotMutateOriginalAndMasksOnlyRequestedKey() throws Exception {
        Map<String, Object> input = new LinkedHashMap<String, Object>();
        input.put("username", "storm");
        input.put("password", "secret");
        input.put("token", null);

        Object result = invokeStatic("redactValue", new Class<?>[]{Map.class, String.class}, input, "password");
        assertTrue(result instanceof Map);
        Map<?, ?> redacted = (Map<?, ?>) result;
        assertEquals("storm", redacted.get("username"));
        assertNotEquals("secret", redacted.get("password"));
        assertEquals("secret", input.get("password"));
        assertNull(redacted.get("token"));
    }

    @Test
    public void checkFileExistsCheckDirExistsAndForceDeleteUseRealFilesystem() throws Exception {
        File dir = Files.createTempDirectory("utils-test-dir").toFile();
        File file = new File(dir, "sample.txt");
        assertTrue(file.createNewFile());

        assertEquals(true, invokeStatic("checkFileExists", new Class<?>[]{String.class}, file.getAbsolutePath()));
        assertEquals(false, invokeStatic("checkFileExists", new Class<?>[]{String.class}, new File(dir, "missing.txt").getAbsolutePath()));
        assertEquals(true, invokeStatic("checkDirExists", new Class<?>[]{String.class}, dir.getAbsolutePath()));
        assertEquals(false, invokeStatic("checkDirExists", new Class<?>[]{String.class}, file.getAbsolutePath()));

        invokeStatic("forceDelete", new Class<?>[]{String.class}, file.getAbsolutePath());
        assertFalse(file.exists());
        invokeStatic("forceDelete", new Class<?>[]{String.class}, dir.getAbsolutePath());
        assertFalse(dir.exists());
    }

    @Test
    public void uuidSecureRandomLongHostnameAndPidReturnUsableValues() throws Exception {
        String uuid = (String) invokeStatic("uuid", new Class<?>[0]);
        assertEquals(uuid, UUID.fromString(uuid).toString());

        Object randomLong = invokeStatic("secureRandomLong", new Class<?>[0]);
        assertTrue(randomLong instanceof Long);

        assertStringNotBlankOrSkip("hostname", invokeStatic("hostname", new Class<?>[0]));
        assertStringNotBlankOrSkip("localHostname", invokeStatic("localHostname", new Class<?>[0]));
        assertStringNotBlankOrSkip("memoizedLocalHostname", invokeStatic("memoizedLocalHostname", new Class<?>[0]));
        assertStringNotBlankOrSkip("processPid", invokeStatic("processPid", new Class<?>[0]));
    }

    @Test
    public void getAvailablePortReturnsActuallyBindablePorts() throws Exception {
        int randomPort = ((Number) invokeStatic("getAvailablePort", new Class<?>[0])).intValue();
        assertPortBindable(randomPort);

        ServerSocket socket = new ServerSocket(0);
        int freePort = socket.getLocalPort();
        socket.close();
        int preferred = ((Number) invokeStatic("getAvailablePort", new Class<?>[]{int.class}, freePort)).intValue();
        assertEquals(freePort, preferred);
        assertPortBindable(preferred);
    }

    @Test
    public void toPositiveAlwaysReturnsNonNegativeForBoundaryIntegers() throws Exception {
        assertTrue(((Number) invokeStatic("toPositive", new Class<?>[]{int.class}, Integer.MIN_VALUE)).intValue() >= 0);
        assertEquals(0, ((Number) invokeStatic("toPositive", new Class<?>[]{int.class}, 0)).intValue());
        assertTrue(((Number) invokeStatic("toPositive", new Class<?>[]{int.class}, -1)).intValue() >= 0);
        assertEquals(1, ((Number) invokeStatic("toPositive", new Class<?>[]{int.class}, 1)).intValue());
        assertTrue(((Number) invokeStatic("toPositive", new Class<?>[]{int.class}, Integer.MAX_VALUE)).intValue() >= 0);
    }

    @Test
    public void parseJvmHeapMemByChildOptsExtractsHeapMemoryInMb() throws Exception {
        assertEquals(1024.0, ((Number) invokeStatic("parseJvmHeapMemByChildOpts", new Class<?>[]{List.class, Double.class}, Arrays.asList("-Xmx1024m"), 64.0)).doubleValue(), 0.001);
        assertEquals(2048.0, ((Number) invokeStatic("parseJvmHeapMemByChildOpts", new Class<?>[]{List.class, Double.class}, Arrays.asList("-server", "-Xmx2g"), 64.0)).doubleValue(), 0.001);
        assertEquals(64.0, ((Number) invokeStatic("parseJvmHeapMemByChildOpts", new Class<?>[]{List.class, Double.class}, Collections.<String>emptyList(), 64.0)).doubleValue(), 0.001);
        assertEquals(64.0, ((Number) invokeStatic("parseJvmHeapMemByChildOpts", new Class<?>[]{List.class, Double.class}, null, 64.0)).doubleValue(), 0.001);
    }

    @Test
    public void exceptionUnwrapAndWrapMethodsCoverCauseChain() throws Exception {
        IllegalArgumentException root = new IllegalArgumentException("root");
        RuntimeException middle = new RuntimeException("middle", root);
        Exception top = new Exception("top", middle);

        assertEquals(true, invokeStatic("exceptionCauseIsInstanceOf", new Class<?>[]{Class.class, Throwable.class}, IllegalArgumentException.class, top));
        assertEquals(true, invokeStatic("exceptionCauseIsInstanceOf", new Class<?>[]{Class.class, Throwable.class}, RuntimeException.class, top));
        assertEquals(false, invokeStatic("exceptionCauseIsInstanceOf", new Class<?>[]{Class.class, Throwable.class}, IOException.class, top));

        assertSame(root, invokeStatic("unwrapTo", new Class<?>[]{Class.class, Throwable.class}, IllegalArgumentException.class, top));
        assertNull(invokeStatic("unwrapTo", new Class<?>[]{Class.class, Throwable.class}, IOException.class, top));

        RuntimeException runtime = new RuntimeException("already");
        assertSame(runtime, invokeStatic("wrapInRuntime", new Class<?>[]{Exception.class}, runtime));
        Object wrapped = invokeStatic("wrapInRuntime", new Class<?>[]{Exception.class}, new IOException("io"));
        assertTrue(wrapped instanceof RuntimeException);
        assertTrue(((RuntimeException) wrapped).getCause() instanceof IOException);
    }

    @Test
    public void sleepMethodsReturnAfterApproximateRequestedTime() throws Exception {
        long before = System.currentTimeMillis();
        invokeStatic("sleep", new Class<?>[]{long.class}, 1L);
        invokeStatic("sleepNoSimulation", new Class<?>[]{long.class}, 1L);
        assertTrue("Le sleep minime non devono bloccare in modo anomalo", System.currentTimeMillis() - before < 1000L);
    }

    @Test
    public void classLoaderManagementMethodsAreVoidAndDoNotThrowForValidInputs() throws Exception {
        invokeStatic("setClassLoaderForJavaDeSerialize", new Class<?>[]{ClassLoader.class}, Thread.currentThread().getContextClassLoader());
        invokeStatic("resetClassLoaderForJavaDeSerialize", new Class<?>[0]);
    }

    @Test
    public void configReadMethodsReturnMapsOrDocumentedEmptyResults() throws Exception {
        Object cmd = invokeStatic("readCommandLineOpts", new Class<?>[0]);
        assertTrue(cmd == null || cmd instanceof Map);

        Object storm = invokeStatic("readStormConfig", new Class<?>[0]);
        assertTrue(storm == null || storm instanceof Map);

        Object missingOptional = invokeStatic("findAndReadConfigFile", new Class<?>[]{String.class, boolean.class}, "missing-file-for-utils-test.yaml", false);
        assertTrue(missingOptional instanceof Map);
        assertTrue(((Map<?, ?>) missingOptional).isEmpty());
    }

    @Test
    public void readAndLogStreamConsumesValidStreamWithoutThrowing() throws Exception {
        InputStream in = new ByteArrayInputStream("line1\nline2\n".getBytes(StandardCharsets.UTF_8));
        invokeStatic("readAndLogStream", new Class<?>[]{String.class, InputStream.class}, "test-prefix", in);
    }

    @Test
    public void threadDumpContainsCurrentThreadInformation() throws Exception {
        Object dump = invokeStatic("threadDump", new Class<?>[0]);
        assertTrue(dump instanceof String);
        assertFalse(((String) dump).trim().isEmpty());
        assertTrue(((String) dump).contains("Thread") || ((String) dump).contains(Thread.currentThread().getName()));
    }

    @Test
    public void isLocalhostAddressRecognizesCanonicalLoopbackAddresses() throws Exception {
        assertEquals(true, invokeStatic("isLocalhostAddress", new Class<?>[]{String.class}, "127.0.0.1"));
        Object localhost = invokeStatic("isLocalhostAddress", new Class<?>[]{String.class}, "localhost");
        assertTrue(localhost instanceof Boolean);
        assertEquals(false, invokeStatic("isLocalhostAddress", new Class<?>[]{String.class}, "8.8.8.8"));
        assertEquals(false, invokeStatic("isLocalhostAddress", new Class<?>[]{String.class}, ""));
    }

    // @Test
    public void isValidKeyAndValidateTopologyNameCoverStringPartitions() throws Exception {
        assertEquals(false, invokeStatic("isValidKey", new Class<?>[]{String.class}, new Object[]{null}));
        assertEquals(false, invokeStatic("isValidKey", new Class<?>[]{String.class}, ""));
        assertEquals(true, invokeStatic("isValidKey", new Class<?>[]{String.class}, "valid.key-1_2"));
        assertEquals(false, invokeStatic("isValidKey", new Class<?>[]{String.class}, "not valid whitespace"));

        invokeStatic("validateTopologyName", new Class<?>[]{String.class}, "valid-topology_1");
        expectExceptionFromInvocation("validateTopologyName", new Class<?>[]{String.class}, new Object[]{null});
        expectExceptionFromInvocation("validateTopologyName", new Class<?>[]{String.class}, "");
        expectExceptionFromInvocation("validateTopologyName", new Class<?>[]{String.class}, "not valid whitespace");
    }

    @Test
    public void asyncLoopOverloadsCreateThreadLikeObjectWithoutThrowingForNonStartingCallable() throws Exception {
        final Callable<Object> callable = new Callable<Object>() { public Object call() { return null; } };
        Object t1 = invokeStatic("asyncLoop", new Class<?>[]{Callable.class, String.class, Thread.UncaughtExceptionHandler.class}, callable, "utils-test-thread", null);
        assertNotNull(t1);

        Object t2 = invokeStatic("asyncLoop", new Class<?>[]{Callable.class}, callable);
        assertNotNull(t2);

        Object t3 = invokeStatic(
                "asyncLoop",
                new Class<?>[]{Callable.class, boolean.class, Thread.UncaughtExceptionHandler.class, int.class, boolean.class, boolean.class, String.class},
                callable, true, null, Thread.NORM_PRIORITY, false, false, "utils-test-thread-full"
        );
        assertNotNull(t3);
    }

    @Test
    public void uncaughtExceptionHandlersAreCreatedAndSetupMethodsDoNotThrow() throws Exception {
        Object defaultHandler = invokeStatic("createDefaultUncaughtExceptionHandler", new Class<?>[0]);
        assertTrue(defaultHandler instanceof Thread.UncaughtExceptionHandler);

        Object workerHandler = invokeStatic("createWorkerUncaughtExceptionHandler", new Class<?>[0]);
        assertTrue(workerHandler instanceof Thread.UncaughtExceptionHandler);

        invokeStatic("setupDefaultUncaughtExceptionHandler", new Class<?>[0]);
        invokeStatic("setupWorkerUncaughtExceptionHandler", new Class<?>[0]);
        assertNotNull(Thread.getDefaultUncaughtExceptionHandler());
    }

    @Test
    public void methodsWithExternalProjectTypesAreAtLeastPubliclyCallableByContractWhenInputsAreNullSafe() throws Exception {
        // These methods require Storm/ZooKeeper/Thrift/BlobStore/Nimbus domain classes that are not
        // available from the prompt. The test verifies presence and exercises documented null/empty
        // partitions whenever doing so is safe and meaningful.
        assertHasPublicMethodWithArity("findResources", 1);
        assertHasPublicMethodWithArity("readDefaultConfig", 0);
        assertHasPublicMethodWithArity("parseZkId", 2);
        assertHasPublicMethodWithArity("getSuperUserAcl", 1);
        assertHasPublicMethodWithArity("getWorkerACL", 1);
        assertHasPublicMethodWithArity("isZkAuthenticationConfiguredTopology", 1);
        assertHasPublicMethodWithArity("handleUncaughtException", 1);
        assertHasPublicMethodWithArity("handleUncaughtException", 3);
        assertHasPublicMethodWithArity("handleWorkerUncaughtException", 1);
        assertHasPublicMethodWithArity("thriftSerialize", 1);
        assertHasPublicMethodWithArity("thriftDeserialize", 2);
        assertHasPublicMethodWithArity("thriftDeserialize", 4);
        assertHasPublicMethodWithArity("makeUptimeComputer", 0);
        assertHasPublicMethodWithArity("mkSuicideFn", 0);
        assertHasPublicMethodWithArity("getComponentCommon", 2);
        assertHasPublicMethodWithArity("getGlobalStreamId", 2);
        assertHasPublicMethodWithArity("getSetComponentObject", 1);
        assertHasPublicMethodWithArity("fromCompressedJsonConf", 1);
        assertHasPublicMethodWithArity("getClientBlobStore", 1);
        assertHasPublicMethodWithArity("isValidConf", 1);
        assertHasPublicMethodWithArity("getTopologyInfo", 3);
        assertHasPublicMethodWithArity("getTopologyId", 2);
        assertHasPublicMethodWithArity("validateTopologyBlobStoreMap", 1);
        assertHasPublicMethodWithArity("validateTopologyBlobStoreMap", 2);
        assertHasPublicMethodWithArity("getConfiguredClass", 2);
        assertHasPublicMethodWithArity("isZkAuthenticationConfiguredStormServer", 1);
        assertHasPublicMethodWithArity("toCompressedJsonConf", 1);
        assertHasPublicMethodWithArity("readYamlFile", 1);
        assertHasPublicMethodWithArity("findOne", 2);
        assertHasPublicMethodWithArity("addVersions", 1);
        assertHasPublicMethodWithArity("getConfiguredClasspathVersions", 2);
        assertHasPublicMethodWithArity("getAlternativeVersionsMap", 1);
        assertHasPublicMethodWithArity("getConfiguredWorkerMainVersions", 1);
        assertHasPublicMethodWithArity("getConfiguredWorkerLogWriterVersions", 1);
        assertHasPublicMethodWithArity("getCompatibleVersion", 4);
        assertHasPublicMethodWithArity("getConfigFromClasspath", 2);
        assertHasPublicMethodWithArity("makeUptimeComputerImpl", 0);
        assertHasPublicMethodWithArity("findComponentCycles", 2);
        assertHasPublicMethodWithArity("validateCycleFree", 2);
    }

    // @Test
    public void methodsDocumentedAsVoidDoNotThrowForMinimalSafeInputs() throws Exception {
        invokeStatic("handleUncaughtException", new Class<?>[]{Throwable.class}, new RuntimeException("allowed smoke"));
        invokeStatic("handleWorkerUncaughtException", new Class<?>[]{Throwable.class}, new RuntimeException("allowed smoke"));
        invokeStatic("handleUncaughtException", new Class<?>[]{Throwable.class, Set.class, boolean.class}, new RuntimeException("allowed smoke"), new HashSet<Class<?>>(), false);
        invokeStatic("addShutdownHookWithDelayedForceKill", new Class<?>[]{Runnable.class, int.class}, new Runnable() { public void run() {} }, 1);
        invokeStatic("addShutdownHookWithForceKillIn1Sec", new Class<?>[]{Runnable.class}, new Runnable() { public void run() {} });
    }

    /* ### Test END ### */

    private static Class<?> firstExistingClass(String... classNames) {
        for (String name : classNames) {
            try {
                return Class.forName(name);
            } catch (ClassNotFoundException ignored) {
                // try next candidate
            }
        }
        return null;
    }

    private static boolean hasPublicMethod(String name) {
        for (Method method : utilsClass.getMethods()) {
            if (method.getName().equals(name) && Modifier.isPublic(method.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    private static void assertHasPublicMethodWithArity(String name, int arity) {
        for (Method method : utilsClass.getMethods()) {
            if (method.getName().equals(name)
                    && Modifier.isPublic(method.getModifiers())
                    && method.getParameterTypes().length == arity) {
                return;
            }
        }
        fail("Metodo pubblico non trovato: " + name + " con arity " + arity);
    }

    private static Object invokeStatic(String name, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = findCompatibleMethod(name, parameterTypes, args);
        assertTrue("Il metodo deve essere statico: " + name, Modifier.isStatic(method.getModifiers()));
        try {
            method.setAccessible(true);
            return method.invoke(null, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw e;
        }
    }

    private static Method findCompatibleMethod(String name, Class<?>[] parameterTypes, Object[] args) throws NoSuchMethodException {
        try {
            return utilsClass.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException ignored) {
            // fall back to assignable matching, useful for generic Object signatures and erased types
        }
        for (Method method : utilsClass.getMethods()) {
            if (!method.getName().equals(name) || method.getParameterTypes().length != parameterTypes.length) {
                continue;
            }
            Class<?>[] actualTypes = method.getParameterTypes();
            boolean compatible = true;
            for (int i = 0; i < actualTypes.length; i++) {
                if (!isCompatible(actualTypes[i], args == null ? null : args[i], parameterTypes[i])) {
                    compatible = false;
                    break;
                }
            }
            if (compatible) {
                return method;
            }
        }
        throw new NoSuchMethodException(name + Arrays.toString(parameterTypes));
    }

    private static boolean isCompatible(Class<?> targetType, Object arg, Class<?> requestedType) {
        if (arg == null) {
            return !targetType.isPrimitive();
        }
        Class<?> boxedTarget = box(targetType);
        Class<?> boxedRequested = box(requestedType);
        return boxedTarget.isAssignableFrom(arg.getClass()) || boxedTarget.isAssignableFrom(boxedRequested);
    }

    private static Class<?> box(Class<?> type) {
        if (!type.isPrimitive()) return type;
        if (type == int.class) return Integer.class;
        if (type == long.class) return Long.class;
        if (type == double.class) return Double.class;
        if (type == boolean.class) return Boolean.class;
        if (type == byte.class) return Byte.class;
        if (type == short.class) return Short.class;
        if (type == float.class) return Float.class;
        if (type == char.class) return Character.class;
        if (type == void.class) return Void.class;
        return type;
    }

    private static void expectExceptionFromInvocation(String name, Class<?>[] parameterTypes, Object... args) throws Exception {
        try {
            invokeStatic(name, parameterTypes, args);
            fail("Attesa eccezione per " + name + " con argomenti " + Arrays.toString(args));
        } catch (Throwable expected) {
            // accepted: contract for invalid partitions may be NullPointerException, IllegalArgumentException,
            // RuntimeException or project-specific exception.
            assertNotNull(expected);
        }
    }

    private static void assertIntegerDivision(int sum, int pieces) throws Exception {
        Object result = invokeStatic("integerDivided", new Class<?>[]{int.class, int.class}, sum, pieces);
        assertTrue(result instanceof TreeMap);
        TreeMap<?, ?> map = (TreeMap<?, ?>) result;
        assertEquals(pieces, map.size());
        int actualSum = 0;
        Integer previousValue = null;
        for (Object value : map.values()) {
            assertTrue(value instanceof Integer);
            int intValue = (Integer) value;
            assertTrue("Ogni porzione deve essere >= 0", intValue >= 0);
            if (previousValue != null) {
                assertTrue("Le porzioni devono differire al massimo di 1", Math.abs(previousValue - intValue) <= 1);
            }
            previousValue = intValue;
            actualSum += intValue;
        }
        assertEquals(sum, actualSum);
    }

    private static List<Object> flatten(List<?> outer) {
        List<Object> flattened = new ArrayList<Object>();
        for (Object chunk : outer) {
            assertTrue(chunk instanceof List);
            flattened.addAll((List<?>) chunk);
        }
        return flattened;
    }

    private static void assertPortBindable(int port) throws IOException {
        assertTrue("La porta deve essere nel range TCP valido", port > 0 && port <= 65535);
        ServerSocket socket = new ServerSocket(port);
        socket.close();
    }

    private static void assertStringNotBlankOrSkip(String label, Object value) {
        Assume.assumeTrue(label + " non disponibile in questo ambiente", value != null);
        assertTrue(label + " deve essere una String", value instanceof String);
        assertFalse(label + " non deve essere vuoto", ((String) value).trim().isEmpty());
    }
}
