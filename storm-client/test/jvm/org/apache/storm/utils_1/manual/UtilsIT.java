package org.apache.storm.utils_1.manual;

import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.utils.refactored.one.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Test class which runs integration test on Utils class */
public class UtilsIT {

    /** Setting up the test environment */
    @Before
    public void setUp() throws IOException {

        File dir = new File("correctDir");
        dir.mkdir();
        File file = new File("correctFile");
        file.createNewFile();
        File yamlFile = new File("correctYamlFile.yaml");
        yamlFile.createNewFile();
        File propertiesFile = new File("notYamlFile.properties");
        propertiesFile.createNewFile();
        try (FileWriter writer = new FileWriter(yamlFile)) {
            writer.write("key1: value1\n");
            writer.write("key2: value2\n");
        }
        try (FileWriter writer = new FileWriter(propertiesFile)) {
            writer.write("key1=value1\n");
            writer.write("key2=value2\n");
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
        File propertiesFile = new File("notYamlFile.properties");
        propertiesFile.delete();
    }

    //CHECK DIR EXISTS TESTS

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

    /** Test findAndReadConfigFile method with name = "notCorrectYamlFile.yaml", mustExists = false. Expected = Empty Map */
    @Test
    public void findAndReadConfigFileNotValidNameFalseMustExistsShouldPass(){

        Map<String, Object> map = Utils.findAndReadConfigFile("notCorrectYamlFile.yaml", false);
        Assert.assertEquals(Map.of(), map);
    }

    /** Test findAndReadConfigFile method with name = "correctYamlFile.yaml", mustExists = true. Expected = Map containing key-value pairs of file yaml */
    @Test
    public void findAndReadConfigFileValidNameTrueMustExistsShouldPass(){

        Map<String, Object> map = Utils.findAndReadConfigFile("correctYamlFile.yaml", true);
        Assert.assertEquals(Map.of("key1", "value1", "key2", "value2"), map);
    }

    /** Test findAndReadConfigFile method with name = "notYamlFile.properties", mustExists = true. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileNotCorrectNameTrueMustExistsThrowsRuntimeException(){

        Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("notYamlFile.properties", true));
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
    public void deserializePartialByteArrayThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            AccessControl expected = new AccessControl();
            expected.set_type(AccessControlType.USER);
            expected.set_access(0);
            byte[] serialized = Utils.serialize(expected);
            byte[] truncated = Arrays.copyOf(serialized, serialized.length - 1);
            Utils.deserialize(truncated, AccessControl.class);
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
            expected.set_type(AccessControlType.USER);
            expected.set_access(0);
            String serialized = Utils.serializeToString(expected);
            Utils.deserializeFromString(serialized.substring(0, serialized.length() - 1), AccessControl.class);
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

    // THRIFT DESERIALIZE TESTS

    /** Test thriftDeserialize method with correct class, null byte array, offset 0, length 1. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeCorrectClassNullByteZeroOffsetLengthOneThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftDeserialize(AccessControl.class, null, 0, 1));
    }

    /** Test thriftDeserialize method with not correct class, empty byte array, offset 0, length 0. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeNotCorrectClassEmptyByteZeroOffsetZeroLengthThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftDeserialize(String.class, new byte[0], 0, 0));
    }

    /** Test thriftDeserialize method with correct class, partial byte array, offset 0, length partial.length. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeCorrectClassPartialByteZeroOffsetPartialLengthThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            AccessControl expected = new AccessControl();
            expected.set_type(AccessControlType.USER);
            expected.set_access(0);
            byte[] serialized = Utils.thriftSerialize(expected);
            byte[] partial = Arrays.copyOf(serialized, serialized.length - 1);
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

    //THRIFT SERIALIZE TESTS ARE IMPLICIT TESTED WITH PREVIOUS TESTS (LITTLE PROBABILTY OF COMPENSATION, BUT I DON'T WORRY ABOUT)

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
            byte[] truncated = Arrays.copyOf(serialized, serialized.length - 1);
            Utils.javaDeserialize(truncated, AccessControl.class);
        });
    }

    // JAVA SERIALIZE IS IMPLICIT TESTED WITH PREVIOUS TESTS (LITTLE PROBABILITY OF COMPENSATION, BUT I DON'T WORRY ABOUT)

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

    /** Test fromCompressedJsonConf method with valid serialized (json formatted properly). Expected = Map containing ("name", "pippo") */
    @Test
    public void fromCompressedJsonConfValidSerializedShouldPass(){

        Map<String, Object> map = new HashMap<>();
        map.put("name", "pippo");
        byte[] serialized = Utils.toCompressedJsonConf(map);
        Assert.assertEquals(map, Utils.fromCompressedJsonConf(serialized));
    }
}
