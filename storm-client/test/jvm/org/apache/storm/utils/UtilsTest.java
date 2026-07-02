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
        Mockito.when(mockPredicate.test(Mockito.any())).thenThrow(new RuntimeException());
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
        Mockito.when(mockPredicate.test(integer)).thenReturn(false);
        Mockito.when(mockPredicate.test(integer2)).thenReturn(true);
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

    

}
