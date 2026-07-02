package org.apache.storm.utils;

import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.thrift.TBase;
import org.apache.storm.thrift.TDeserializer;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/** Test class which runs unit test on Utils class */
@RunWith(MockitoJUnitRunner.class)
public class UtilsTest {

    //CHECK DIR EXISTS TESTS

    /** Setting up the test environment */
    @Before
    public void setUp() throws IOException {
        File dir = new File("correctDir");
        dir.mkdir();
        File file = new File("correctFile");
        file.createNewFile();

        File yamlFile = new File("correctYamlFile");
        yamlFile.createNewFile();

        try (FileWriter writer = new FileWriter(yamlFile)) {
            writer.write("key1: value1\n");
            writer.write("key2: value2\n");
        }

    }

    /** Tearing down the test environment */
    @After
    public void tearDown() {
        File dir = new File("correctDir");
        dir.delete();
        File file = new File("correctFile");
        file.delete();
        File yamlFile = new File("correctYamlFile.yaml");
        yamlFile.delete();
    }

    /** Test checkDirExists method with null directory. Expected = throws NullPointerException */
    @Test
    public void checkDirExistsNullDirThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> {
            boolean exists = Utils.checkDirExists(null);
        });
    }

    /** Test checkDirExists method with empty string directory. Expected = returns false */
    @Test
    public void checkDirExistsEmptyDirShouldPass() {

        Assert.assertFalse(Utils.checkDirExists(""));
    }

    /** Test checkDirExists method with non-existent directory. Expected = returns false */
    @Test
    public void checkDirExistsNotCorrectDirShouldPass() {

        Assert.assertFalse(Utils.checkDirExists("notCorrectDir"));
    }

    /** Test checkDirExists method with existing directory. Expected = returns true */
    @Test
    public void checkDirExistsCorrectDirShouldPass() {

        Assert.assertTrue(Utils.checkDirExists("correctDir"));
    }

    // CHECK FILE EXISTS TESTS

    /** Test checkFileExists method with null path. Expected = throws NullPointerException */
    @Test
    public void checkFileExistsNullPathThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> {
            boolean exists = Utils.checkFileExists(null);
        });
    }

    /** Test checkFileExists method with empty string path. Expected = returns true */
    @Test
    public void checkFileExistsEmptyPathShouldPass() {  // CURRENT DIRECTORY CONTAINS OBJECTS

        Assert.assertTrue(Utils.checkFileExists(""));
    }

    /** Test checkFileExists method with non-existent file. Expected = returns false */
    @Test
    public void checkFileExistsNotCorrectPathShouldPass() {

        Assert.assertFalse(Utils.checkFileExists("notCorrectFile"));
    }

    /** Test checkFileExists method with existing file. Expected = returns true */
    @Test
    public void checkFileExistsCorrectPathShouldPass() {

        Assert.assertTrue(Utils.checkFileExists("correctFile"));
    }

    //CONVERT TO ARRAY TESTS

    /** Test convertToArray method with null source map and negative start index. Expected = throws NullPointerException */
    @Test
    public void convertToArrayNullSrcMapSmallerStartThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.convertToArray(null, -1));
    }

    /** Test convertToArray method with empty source map and negative start index. Expected = throws NoSuchElementException */
    @Test
    public void convertToArrayEmptySrcMapSmallerThrowsNoSuchElementException() {    //GET ON A EMPTY OPTIONAL THROWS EXCEPTION

        Assert.assertThrows(NoSuchElementException.class, () -> Utils.convertToArray(Collections.emptyMap(), -1));
    }

    /** Test convertToArray method with valid source map and start index smaller than max key. Expected = returns list with size 2 */
    @Test
    public void convertToArrayValidSrcMapSmallerStartShouldPass() {

        Map<Integer, Integer> map =  Map.of(1,1,2,2,3,3);
        List<Integer> list = Utils.convertToArray(map, 2);
        Assert.assertEquals(List.of(2,3), list);
    }

    /** Test convertToArray method with valid source map and start index equal to max key. Expected = returns list with size 1 */
    @Test
    public void convertToArrayValidSrcMapEqualStartShouldPass() {

        Map<Integer, Integer> map =  Map.of(1,1,2,2,3,3);
        List<Integer> list = Utils.convertToArray(map, 3);
        Assert.assertEquals(List.of(3), list);
    }

    /** Test convertToArray method with valid source map and start index greater than max key. Expected = returns empty list */
    @Test
    public void convertToArrayValidSrcMapBiggerStartShouldPass() {

        Map<Integer, Integer> map =  Map.of(1,1,2,2,3,3);
        List<Integer> list = Utils.convertToArray(map, 4);
        Assert.assertEquals(List.of(), list);
    }

    // DESERIALIZE TESTS

    /** Test deserialize method with null byte array, not correct class. Expected = throws NullPointerException */
    @Test
    public void deserializeNullByteArrayThrowsNullPointerException() {   //T BASE CLASS IS REQUIRED

        Assert.assertThrows(NullPointerException.class, () -> Utils.deserialize(null, String.class));
    }

    /** Test deserialize method with empty byte array, correct class. Expected = throws RuntimeException */
    @Test
    public void deserializeEmptyByteArrayThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.deserialize(new byte[0], AccessControl.class));
    }

    /** Test deserialize method with not valid byte array (not valid class), correct class. Expected = throws RuntimeException */
    @Test
    public void deserializePartialByteArrayThrowsRuntimeException() {  //OBJECT HAS EMPTY VALUES

        Assert.assertThrows(RuntimeException.class, () -> {
            AccessControl expected = new AccessControl();
            byte[] serialized = Utils.serialize(expected);
            Utils.deserialize(serialized, AccessControl.class);
        });
    }

    /** Test deserialize method with valid byte array (valid class), correct class. Expected = returns the deserialized class */
    @Test
    public void deserializeValidByteArrayShouldPass() {
        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(0);
        byte[] serialized = Utils.serialize(expected);
        AccessControl deserialized = Utils.deserialize(serialized, AccessControl.class);
        Assert.assertEquals(expected, deserialized);
    }

    // DESERIALIZE FROM STRING TESTS

    /** Test deserializeFromString method with str = null, not correct class. Expected = throws NullPointerException */
    @Test
    public void deserializeFromStringNullStrThrowsNullPointerException() {   //T BASE CLASS IS REQUIRED

        Assert.assertThrows(NullPointerException.class, () -> Utils.deserializeFromString(null, String.class));
    }

    /** Test deserializeFromString method with str = "", correct class. Expected = throws RuntimeException */
    @Test
    public void deserializeFromStringEmptyStrThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.deserializeFromString("", AccessControl.class));
    }

    /** Test deserializeFromString method with not valid str (not valid class), correct class. Expected = throws RuntimeException */
    @Test
    public void deserializeFromStringPartialStrThrowsRuntimeException() {  //OBJECT HAS EMPTY VALUES

        Assert.assertThrows(RuntimeException.class, () -> {
            AccessControl expected = new AccessControl();
            String serialized = Utils.serializeToString(expected);
            Utils.deserializeFromString(serialized, AccessControl.class);
        });
    }

    /** Test deserializeFromString method with valid str (valid class), correct class. Expected = returns the deserialized class */
    @Test
    public void deserializeFromStringValidStrShouldPass() {

        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(0);
        String serialized = Utils.serializeToString(expected);
        AccessControl deserialized = Utils.deserializeFromString(serialized, AccessControl.class);
        Assert.assertEquals(expected, deserialized);
    }

    //SERIALIZE AND SERIALIZE TO STRING ARE IMPLICIT TESTED WITH PREVIOUS TESTS (LITTLE PROBABILTY OF COMPENSATION, BUT I DON'T WORRY ABOUT)

    // FIND AND READ CONFIG FILE TESTS

    /** Test findAndReadConfigFile method with name = null, mustExists = true. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileNullNameTrueMustExistsThrowsRuntimeException(){

        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile(null, true));
    }

    /** Test findAndReadConfigFile method with name = "", mustExists = true. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileEmptyNameTrueMustExistsThrowsRuntimeException(){

        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("", true));
    }

    /** Test findAndReadConfigFile method with name = "notCorrectYamlFile", mustExists = false. Expected = Empty Map */
    @Test
    public void findAndReadConfigFileNotValidNameFalseMustExistsShouldPass(){

        Map<String, Object> map = Utils.findAndReadConfigFile("notCorrectYamlFile", false);
        Assert.assertEquals(Map.of(), map);
    }

    /** Test findAndReadConfigFile method with name = "correctYamlFile", mustExists = true. Expected = Map containing key-value pairs of file yaml */
    @Test
    public void findAndReadConfigFileValidNameTrueMustExistsShouldPass(){

        Map<String, Object> map = Utils.findAndReadConfigFile("correctYamlFile", false);
        Assert.assertEquals(Map.of("key1", "value1", "key2", "value2"), map);
    }

    // FIND ONE TESTS

    /** Test findOne method with null IPredicate, empty Collection. Expected = null value */
    @Test
    public void findOneNullIPredicateEmptyCollectionShouldPass() {

        Assert.assertNull(Utils.findOne(null, Set.of()));
    }

    /** Test findOne method with not valid IPredicate (throws RuntimeException), null Collection. Expected = null value */
    @SuppressWarnings("unchecked")
    @Test
    public void findOneNotCorrectIPredicateNullCollectionShouldPass() {

        IPredicate<Integer> mockPredicate = Mockito.mock(IPredicate.class);
        Mockito.lenient().when(mockPredicate.test(Mockito.any())).thenThrow(new RuntimeException());
        Assert.assertNull(Utils.findOne(mockPredicate, (Set<Integer>) null));
    }

    /** Test findOne method with correct IPredicate (first element in the collection is coherent with check) , one value Collection. Expected = first element */
    @SuppressWarnings("unchecked")
    @Test
    public void findOneValidIPredicateValidCollectionShouldPass() {

        IPredicate<Integer> mockPredicate = Mockito.mock(IPredicate.class);
        Mockito.when(mockPredicate.test(Mockito.any())).thenReturn(true);
        Set<Integer> set = Set.of(1);
        Integer integer = 1;
        Assert.assertEquals(integer, Utils.findOne(mockPredicate, set));
    }

    /** Test findOne method with correct IPredicate (second element in the collection is coherent with check) , two values Collection. Expected = second element*/
    @SuppressWarnings("unchecked")
    @Test
    public void findOneValidIPredicateValidCollection2ShouldPass() {

        IPredicate<Integer> mockPredicate = Mockito.mock(IPredicate.class);
        Integer integer = 1;
        Integer integer2 = 2;
        Mockito.lenient().when(mockPredicate.test(integer)).thenReturn(false);
        Mockito.lenient().when(mockPredicate.test(integer2)).thenReturn(true);
        Set<Integer> set = Set.of(1,2);
        Assert.assertEquals(integer2, Utils.findOne(mockPredicate, set));
    }

    /** Test findOne method with not correct IPredicate (no element in the collection is coherent with check) , one value Collection. Expected = null*/
    @SuppressWarnings("unchecked")
    @Test
    public void findOneNotCorrectIPredicateValidCollectionShouldPass() {

        IPredicate<Integer> mockPredicate = Mockito.mock(IPredicate.class);
        Mockito.when(mockPredicate.test(Mockito.any())).thenReturn(false);
        Set<Integer> set = Set.of(1);
        Assert.assertNull(Utils.findOne(mockPredicate, set));
    }

    // FROM COMPRESSED JSON CONF TESTS

    /** Test fromCompressedJsonConf method with serialized = null. Expected = Throws RuntimeException */
    @Test
    public void fromCompressedJsonConfNullSerializedThrowsRuntimeException(){

        Assert.assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(null));
    }

    /** Test fromCompressedJsonConf method with empty serialized. Expected = Throws RuntimeException */
    @Test
    public void fromCompressedJsonConfEmptySerializedThrowsRuntimeException(){

        Assert.assertThrows(RuntimeException.class, () -> Utils.fromCompressedJsonConf(new byte[0]));
    }

    /** Test fromCompressedJsonConf method with not valid serialized (json not formatted properly --> key = null). Expected = Throws RuntimeException */
    @Test
    public void fromCompressedJsonConfNotValidSerializedThrowsRuntimeException(){ //KEY NULL --> JSON DOESN'T ALLOW KEY = NULL

        Map<String, Object> map = new HashMap<>();
        map.put(null, "value");
        Assert.assertThrows(RuntimeException.class, () -> {
            byte[] serialized = Utils.toCompressedJsonConf(map);
            Utils.fromCompressedJsonConf(serialized);
        });
    }

    /** Test fromCompressedJsonConf method with valid serialized (json formatted properly). Expected = Map containing ("name", "value") */
    @Test
    public void fromCompressedJsonConfValidSerializedThrowsRuntimeException(){

        Map<String, Object> map = new HashMap<>();
        map.put("name", "pippo");
        byte[] serialized = Utils.toCompressedJsonConf(map);
        Assert.assertEquals(map, Utils.fromCompressedJsonConf(serialized));
    }

    // JAVA DESERIALIZE TESTS

    /** Test javaDeserialize method with null serialized array, correct class. Expected = throws NullPointerException */
    @Test
    public void javaDeserializeNullSerializedThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.javaDeserialize(null, AccessControl.class));
    }

    /** Test javaDeserialize method with empty serialized array, not correct class. Expected = throws RuntimeException */
    @Test
    public void javaDeserializeEmptySerializedNotCorrectClassThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.javaDeserialize(new byte[0], String.class));
    }

    /** Test javaDeserialize method with valid serialized array, correct class. Expected = returns the deserialized object */
    @Test
    public void javaDeserializeSerializedCorrectClassShouldPass() {

        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(0);
        byte[] serialized = Utils.javaSerialize(expected);
        AccessControl deserialized = Utils.javaDeserialize(serialized, AccessControl.class);
        Assert.assertEquals(expected, deserialized);
    }

    /** Test javaDeserialize method with partially corrupted serialized array, correct class. Expected = throws RuntimeException */
    @Test
    public void javaDeserializePartialSerializedCorrectClassThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            AccessControl expected = new AccessControl();
            expected.set_type(AccessControlType.USER);
            expected.set_access(0);
            byte[] serialized = Utils.javaSerialize(expected);
            // Corrupt the serialized data by removing the last byte
            byte[] truncated = java.util.Arrays.copyOf(serialized, serialized.length - 1);
            Utils.javaDeserialize(truncated, AccessControl.class);
        });
    }

    // JAVA SERIALIZE IS IMPLICIT TESTED WITH PREVIOUS TESTS (LITTLE PROBABILITY OF COMPENSATION, BUT I DON'T WORRY ABOUT)

    // JOIN TESTS

    /** Test join method with null collection and null separator. Expected = throws NullPointerException */
    @Test
    public void joinNullCollectionNullSeparatorThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> {
            Utils.join(null, null);
        });
    }

    /** Test join method with empty collection and valid separator. Expected = returns empty string */
    @Test
    public void joinEmptyCollectionValidSeparatorShouldPass() {

        Assert.assertEquals("", Utils.join(List.of(), ","));
    }

    /** Test join method with single element collection and empty separator. Expected = returns element as string */
    @Test
    public void joinSingleElementCollectionEmptySeparatorShouldPass() {

        Assert.assertEquals("hello", Utils.join(List.of("hello"), ""));
    }

    /** Test join method with single element collection and valid separator. Expected = returns element as string */
    @Test
    public void joinSingleElementCollectionValidSeparatorShouldPass() {

        Assert.assertEquals("hello", Utils.join(List.of("hello"), ","));
    }

    /** Test join method with two element collection and valid separator. Expected = returns joined string */
    @Test
    public void joinTwoElementCollectionValidSeparatorShouldPass() {

        Assert.assertEquals("hello,world", Utils.join(List.of("hello", "world"), ","));
    }

    // THRIFT DESERIALIZE TESTS

    /** Test thriftDeserialize method with correct class, null byte array, offset 0, length 1. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeCorrectClassNullByteZeroOffsetLengthOneThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            Utils.thriftDeserialize(AccessControl.class, null, 0, 1);
        });
    }

    /** Test thriftDeserialize method with not correct class, empty byte array, offset 0, length 0. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeNotCorrectClassEmptyByteZeroOffsetZeroLengthThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            Utils.thriftDeserialize(String.class, new byte[0], 0, 0);
        });
    }

    /** Test thriftDeserialize method with correct class, partial byte array, offset 0, length partial.length. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeCorrectClassPartialByteZeroOffsetPartialLengthThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            AccessControl expected = new AccessControl();
            expected.set_type(AccessControlType.USER);
            expected.set_access(0);
            byte[] serialized = Utils.thriftSerialize(expected);
            byte[] partial = java.util.Arrays.copyOf(serialized, serialized.length - 1);
            Utils.thriftDeserialize(AccessControl.class, partial, 0, partial.length);
        });
    }

    /** Test thriftDeserialize method with correct class, full byte array, offset 0, length full.length. Expected = returns deserialized object */
    @Test
    public void thriftDeserializeCorrectClassFullByteZeroOffsetFullLengthShouldPass() {

        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(0);
        byte[] serialized = Utils.thriftSerialize(expected);
        AccessControl deserialized = Utils.thriftDeserialize(AccessControl.class, serialized, 0, serialized.length);
        Assert.assertEquals(expected, deserialized);
    }

    /** Test thriftDeserialize method with correct class, full byte array, offset full.length, length full.length - 1. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeCorrectClassFullByteFullOffsetLengthMinusOneThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            AccessControl expected = new AccessControl();
            expected.set_type(AccessControlType.USER);
            expected.set_access(0);
            byte[] serialized = Utils.thriftSerialize(expected);
            Utils.thriftDeserialize(AccessControl.class, serialized, serialized.length, serialized.length - 1);
        });
    }

    // REVERSE MAP TESTS

    /** Test reverseMap method with null map. Expected = returns empty map */
    @Test
    public void reverseMapNullMapShouldPass() {

        HashMap<Integer, List<String>> result = Utils.reverseMap((Map<String, Integer>) null);
        Assert.assertEquals(new HashMap<>(), result);
    }

    /** Test reverseMap method with empty map. Expected = returns empty map */
    @Test
    public void reverseMapEmptyMapShouldShouldPass() {

        Map<String, Integer> map = Collections.emptyMap();
        HashMap<Integer, List<String>> result = Utils.reverseMap(map);
        Assert.assertEquals(new HashMap<>(), result);
    }

    /** Test reverseMap method with single element map. Expected = reversed map with one entry */
    @Test
    public void reverseMapSingleElementShouldPass() {

        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        HashMap<Integer, List<String>> expected = new HashMap<>();
        expected.put(1, new ArrayList<>(List.of("a")));
        HashMap<Integer, List<String>> result = Utils.reverseMap(map);
        Assert.assertEquals(expected, result);
    }

    /** Test reverseMap method with two elements having same value. Expected = single key mapping to list of both keys */
    @Test
    public void reverseMapTwoElementsSameValueShouldPAss() {

        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", 1);
        HashMap<Integer, List<String>> expected = new HashMap<>();
        expected.put(1, new ArrayList<>(List.of("a", "b")));
        HashMap<Integer, List<String>> result = Utils.reverseMap(map);
        Assert.assertEquals(expected, result);
    }

    // TUPLE TESTS

    /** Test tuple method with null varargs. Expected = throws NullPointerException */
    @Test
    public void tupleNullVarargsThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> {
            Utils.tuple((Object[]) null);
        });
    }

    /** Test tuple method with empty varargs. Expected = returns empty list */
    @Test
    public void tupleEmptyVarargsShouldPass() {

        List<Object> result = Utils.tuple();
        Assert.assertEquals(List.of(), result);
    }

    /** Test tuple method with single object. Expected = returns list with that object */
    @Test
    public void tupleSingleObjectShouldReturnSingleElementList() {

        Object obj = "hello";
        List<Object> result = Utils.tuple(obj);
        Assert.assertEquals(List.of("hello"), result);
    }

    /** Test tuple method with two objects. Expected = returns list with both objects in order */
    @Test
    public void tupleTwoObjectsShouldPass() {

        Object a = "first";
        Object b = 42;
        List<Object> result = Utils.tuple(a, b);
        Assert.assertEquals(List.of("first", 42), result);
    }

    // GET REPEAT TESTS

    /** Test getRepeat method with null list. Expected = throws NullPointerException */
    @Test
    public void getRepeatNullListThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> {
            Utils.getRepeat(null);
        });
    }

    /** Test getRepeat method with empty list. Expected = returns empty list */
    @Test
    public void getRepeatEmptyListShouldPass() {

        List<String> result = Utils.getRepeat(List.of());
        Assert.assertEquals(List.of(), result);
    }

    /** Test getRepeat method with no repeated values. Expected = returns empty list */
    @Test
    public void getRepeatNoDuplicatesShouldPass() {

        List<String> input = List.of("a", "b", "c");
        List<String> result = Utils.getRepeat(input);
        Assert.assertEquals(List.of(), result);
    }

    /** Test getRepeat method with one repeated value. Expected = returns list with that value */
    @Test
    public void getRepeatOneDuplicateShouldPass() {

        List<String> input = List.of("x", "y", "x");
        List<String> result = Utils.getRepeat(input);
        Assert.assertEquals(List.of("x"), result);
    }

    /** Test getRepeat method with two repeated values. Expected = returns list with both repeated values in order of their second occurrence */
    @Test
    public void getRepeatTwoDuplicatesShouldReturnBothValuesInEncounterOrder() {

        List<String> input = List.of("a", "b", "a", "c", "b");
        List<String> result = Utils.getRepeat(input);
        Assert.assertEquals(List.of("a", "b"), result);
    }

    // REDACT VALUE TESTS

    /** Test redactValue method with null map, empty key. Expected = throws NullPointerException */
    @Test
    public void redactValueNullMapEmptyKeyThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> {
            Utils.redactValue(null, "");
        });
    }

    /** Test redactValue method with empty map and null key. Expected = returns  empty map */
    @Test
    public void redactValueEmptyMapNullKeyShouldPass() {

        Map<String, Object> empty = new HashMap<>();
        Map<String, Object> result = Utils.redactValue(empty, null);
        Assert.assertEquals(empty, result);
    }

    /** Test redactValue method with single entry, correct key (exists). Expected = returns new map with redacted value, original not modified */
    @Test
    public void redactValueSingleEntryKeyExistsShouldPass() {

        HashMap<String, Object> original = new HashMap<>();
        original.put("secret", "password");
        Map<String, Object> result = Utils.redactValue(original, "secret");
        Assert.assertEquals("password", original.get("secret"));
        Assert.assertEquals("########", result.get("secret"));
    }

    /** Test redactValue method with two entries, not correct key (not exists). Expected = returns an equal map */
    @Test
    public void redactValueTwoEntriesKeyNotExistsShouldPass() {

        HashMap<String, Object> original = new HashMap<>();
        original.put("one", 1);
        original.put("two", 2);
        Map<String, Object> result = Utils.redactValue(original, "key");
        Assert.assertEquals(original, result);
    }

    // INTEGER DIVIDED TESTS

    /** Test integerDivided method with sum = -1 and numPieces = -2. Expected = {0=-1, 1=-1} */
    @Test
    public void integerDividedNegativeSumNegativePiecesShouldPass() {  // COMPUTATIONALLY IT IS CORRECT, BUT THEORETICALLY IT DOES NOT MAKE SENSE BECAUSE ALLOCATION TO A NEGATIVE NUMBER OF PEOPLE IS UNDEFINED (E.G., DISTRIBUTING 0 OR 1 ITEMS TO -1 PEOPLE).

        TreeMap<Integer, Integer> result = Utils.integerDivided(-1, -2);
        TreeMap<Integer, Integer> expected = new TreeMap<>();
        expected.put(0, -1);
        expected.put(1, -1);
        Assert.assertEquals(expected, result);
    }

    /** Test integerDivided method with sum = 0 and numPieces = 0. Expected = throws ArithmeticException */
    @Test
    public void integerDividedZeroSumZeroPiecesThrowsArithmeticException() {

        Assert.assertThrows(ArithmeticException.class, () -> Utils.integerDivided(0, 0));
    }

    /** Test integerDivided method with sum = 1 and numPieces = 0. Expected = throws ArithmeticException*/
    @Test
    public void integerDividedPositiveSumZeroPiecesThrowsArithmeticException() {

        Assert.assertThrows(ArithmeticException.class, () -> Utils.integerDivided(1, 0));
    }

    // PARTITION FIXED TESTS

    /** Test partitionFixed with maxNumChunks = -1 and collection with one element. Expected = returns empty list */
    @Test
    public void partitionFixedNegativeChunksOneValueShouldPass() {

        List<Integer> coll = List.of(1);
        List<List<Integer>> result = Utils.partitionFixed(-1, coll);
        Assert.assertEquals(List.of(), result);
    }

    /** Test partitionFixed with maxNumChunks = 0 and collection = null. Expected = returns empty list */
    @Test
    public void partitionFixedZeroChunksNullCollectionShouldPass() {

        List<List<Object>> result = Utils.partitionFixed(0, null);
        Assert.assertEquals(List.of(), result);
    }

    /** Test partitionFixed with maxNumChunks = 1 and empty collection. Expected = returns empty list */
    @Test
    public void partitionFixedOneChunksEmptyCollectionShouldPass() {

        List<Integer> coll = List.of();
        List<List<Integer>> result = Utils.partitionFixed(1, coll);
        Assert.assertEquals(List.of(), result);
    }

    /** Test partitionFixed with maxNumChunks = 0 and collection with one element. Expected = returns empty list */
    @Test
    public void partitionFixedZeroChunksOneValueShouldReturnEmpty() {

        List<String> coll = List.of("x");
        List<List<String>> result = Utils.partitionFixed(0, coll);
        Assert.assertEquals(List.of(), result);
    }

    /** Test partitionFixed with maxNumChunks = 1 and collection with two elements. Expected = single chunk containing both elements */
    @Test
    public void partitionFixedOneChunkTwoValuesShouldReturnSingleChunkWithBoth() {

        List<String> coll = List.of("a", "b");
        List<List<String>> result = Utils.partitionFixed(1, coll);
        Assert.assertEquals(List.of(List.of("a", "b")), result);
    }

    // PARSE JSON TESTS

    /** Test parseJson with null string. Expected = returns empty map */
    @Test
    public void parseJsonNullShouldPass() {

        Map<String, Object> result = Utils.parseJson(null);
        Assert.assertEquals(new HashMap<>(), result);
    }

    /** Test parseJson with empty string. Expected = throws RuntimeException */
    @Test
    public void parseJsonEmptyStringShouldThrowRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.parseJson(""));
    }

    /** Test parseJson with valid JSON string. Expected = returns parsed map */
    @Test
    public void parseJsonValidJsonShouldPass() {

        String json = "{\"key1\":\"value1\",\"key2\":2}";
        Map<String, Object> result = Utils.parseJson(json);
        Map<String, Object> expected = new HashMap<>();
        expected.put("key1", "value1");
        expected.put("key2", 2);
        Assert.assertEquals(expected, result);
    }

    /** Test parseJson with invalid JSON string. Expected = throws RuntimeException */
    @Test
    public void parseJsonInvalidJsonShouldThrowRuntimeException() {  // USING "{KEY : PIPPO}" WITHOUT BRACKETS IT DOESN'T THROW EXCEPTION BECAUSE PARSER USES PERMISSIVE MODE

        String invalid = "{key: }";
        Assert.assertThrows(RuntimeException.class, () -> Utils.parseJson(invalid));
    }


}
