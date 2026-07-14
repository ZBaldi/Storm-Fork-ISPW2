package org.apache.storm.utils_2.manual;

import org.apache.storm.common.NotSerializableClass;
import org.apache.storm.common.SerializableClass;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AccessControlType;
import org.apache.storm.shade.net.minidev.json.JSONValue;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.utils.refactored.two.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.mockito.Mockito.CALLS_REAL_METHODS;

/** Test class which runs integration test on Utils class */
public class UtilsEvolvedCFMTIT {

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
    public void tearDown() throws InterruptedException {

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

    //SERIALIZE AND SERIALIZE TO STRING ARE IMPLICIT TESTED WITH PREVIOUS TESTS (LITTLE PROBABILITY OF COMPENSATION, BUT I DON'T WORRY ABOUT)

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

    //THRIFT SERIALIZE TESTS ARE IMPLICIT TESTED WITH PREVIOUS TESTS (LITTLE PROBABILITY OF COMPENSATION, BUT I DON'T WORRY ABOUT)

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

    //--------------------------------//
    //EVOLVED TESTS WITH CF APPROACHES//
    //--------------------------------//

    // TO COMPRESSED JSON CONF TESTS

    /** Test toCompressedJsonConf method with null configuration. Expected = Throws RuntimeException */
    @Test
    public void toCompressedJsonConfEmptySerializedThrowsRuntimeException(){

        Assert.assertThrows(RuntimeException.class, () -> {

            try(MockedStatic<JSONValue> jsonValueMockedStatic = Mockito.mockStatic(JSONValue.class, Mockito.CALLS_REAL_METHODS)) {
                jsonValueMockedStatic.when(() -> JSONValue.writeJSONString(Mockito.any(), Mockito.any())).thenThrow(new RuntimeException());
                Utils.toCompressedJsonConf(null);
            }
        });
    }

    // JAVA SERIALIZE TESTS

    /** Test javaSerialize method with null object. Expected = throws RuntimeException */
    @Test
    public void javaSerializeNullObjectThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.javaSerialize(new NotSerializableClass()));
    }

    // FIND AND READ CONFIG FILE TESTS

    /** Test findAndReadConfigFile method with name = "notCorrectYamlFile.yaml", mustExists = true. Expected = throws Throwable */
    @Test
    public void findAndReadConfigFileValidNameTrueMustExistsThrowsThrowable(){

        Assert.assertThrows(Throwable.class, () -> {  // WHEN LAUNCHED ALL TOGETHER THERE'S A CONFLICT DIFFERENT PATHS

            try (MockedConstruction<Yaml> ignored = Mockito.mockConstruction(Yaml.class, (mock, context) -> Mockito.when(mock.load(Mockito.any(InputStream.class))).thenReturn(null))) {
                Utils.findAndReadConfigFile("notCorrectYamlFile.yaml", true);
            }
        });
    }

    /** Test findAndReadConfigFile method with name = "notCorrectYamlFile.yaml", mustExists = true. Expected = throws RuntimeException */
    @Test
    public void findAndReadConfigFileValidNameTrueMustExistsThrowsRuntimeException(){

        Assert.assertThrows(RuntimeException.class, () -> {  // I CHANGED THE SOURCE CODE TO MAKE THE METHOD PUBLIC TO MOCK ITS FUNCTIONALITY
            try (MockedStatic<Utils> mocked = Mockito.mockStatic(Utils.class, CALLS_REAL_METHODS)) {
                mocked.when(() -> Utils.getConfigFileInputStream(Mockito.anyString())).thenThrow(new IOException());
                Utils.findAndReadConfigFile("notCorrectYamlFile.yaml", true);
            }
        });
    }

    // JAVA DESERIALIZE TESTS

    /** Test javaDeserialize method with valid serialized array, correct class and state disabled. Expected = throws AssertionError */
    @Test
    public void javaDeserializeSerializedCorrectClassThrowsAssertionError() {

        Assert.assertThrows(AssertionError.class, () -> {
            try {
                AccessControl expected = new AccessControl();
                expected.set_type(AccessControlType.USER);
                expected.set_access(0);
                byte[] serialized = Utils.javaSerialize(expected);
                System.setProperty("java.deserialization.disabled", "true");
                Utils.javaDeserialize(serialized, AccessControl.class);
            }finally {
                System.setProperty("java.deserialization.disabled", "false");
            }
        });
    }

    /** Test javaDeserialize method with not valid serialized array, correct class. Expected = throws RuntimeException */
    // @Test
    public void javaDeserializeSerializedCorrectClassThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            Field clField = Utils.class.getDeclaredField("cl");
            clField.setAccessible(true);
            ClassLoader originalCl = (ClassLoader) clField.get(null);
            try {
                byte[] invalidBytes = new byte[]{1, 2, 3};
                clField.set(null, new ClassLoader() {
                    @Override
                    public Class<?> loadClass(String name) throws ClassNotFoundException {
                        throw new ClassNotFoundException();
                    }
                });
                Utils.javaDeserialize(invalidBytes, AccessControl.class);
            }catch(RuntimeException e){
                throw e;
            }finally {
                clField.set(null, originalCl);
            }
        });
    }

    // THRIFT SERIALIZE TESTS

    /** Test thriftSerialize method with not correct class (empty). Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeNullClassPartialByteZeroOffsetPartialLengthThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            byte[] serialized = Utils.thriftSerialize(new AccessControl());
        });
    }

    // READ YAML FILE TESTS

    /**
     * Test readYamlFile method with null file path.
     * Expected = Returns null.
     */
    @Test
    public void readYamlFileNullFilePathShouldPass() {

        Assert.assertNull(Utils.readYamlFile(null));
    }

    /** Test readYamlFile method with empty file path. Expected = Returns null. */
    @Test
    public void readYamlFileEmptyFilePathShouldPass() {

        Assert.assertNull(Utils.readYamlFile(""));
    }

    /** Test readYamlFile method with non existing yaml file. Expected = Returns null. */
    @Test
    public void readYamlFileNotExistingYamlFileShouldPass() {

        Assert.assertNull(Utils.readYamlFile("notCorrectYamlFile.yaml"));
    }

    /** Test readYamlFile method with properties file. Expected = Returns key1=value1 key2=value2. */
    @Test
    public void readYamlFilePropertiesFileShouldPass() {  //THIS METHOD IS NOT ONLY FOR YAML (ALSO PROPERTIES)!

        Assert.assertEquals("key1=value1 key2=value2", Utils.readYamlFile("notYamlFile.properties"));
    }

    /** Test readYamlFile method with valid yaml file. Expected = Returns parsed yaml object. */
    @Test
    public void readYamlFileValidYamlFileShouldPass() throws Exception {

        Assert.assertEquals("{key1=value1, key2=value2}", Objects.requireNonNull(Utils.readYamlFile("correctYamlFile.yaml")).toString());
    }

    // FORCE DELETE TESTS

    /** Test forceDelete method with null path. Expected = throws NullPointerException. */
    @Test
    public void forceDeleteNullPathShouldPass() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.forceDelete(null));
    }

    /** Test forceDelete method with empty path. Expected = No exception thrown. */
    // @Test (DELETE ALL THE LOCAL PROJECT !!!)
    public void forceDeleteEmptyPathShouldPass() throws IOException {

        boolean thrown;

        try {
            thrown = false;
            Utils.forceDelete("");

        }catch(Exception e){
            thrown = true;
        }
        Assert.assertFalse(thrown);
    }

    /** Test forceDelete method with existing path. Expected = false. */
    @Test
    public void forceDeleteCorrectPathShouldPass() throws IOException {

        Utils.forceDelete("correctFile");
        Assert.assertFalse(Utils.checkFileExists("correctFile"));
    }

    /** Test forceDelete method with non existing path. Expected = false. */
    @Test
    public void forceDeleteNotCorrectPathShouldPass() throws IOException {

        boolean thrown;

        try {
            thrown = false;
            Utils.forceDelete("notCorrectFile");

        }catch(Exception e){
            thrown = true;
        }
        Assert.assertFalse(thrown);
    }

    // THRIFT DESERIALIZE TESTS

    /** Test thriftDeserialize method with correct class, null byte array Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeCorrectClassNullByteThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftDeserialize(AccessControl.class, null));
    }

    /** Test thriftDeserialize method with not correct class, empty byte array. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeNotCorrectClassEmptyByteThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> Utils.thriftDeserialize(String.class, new byte[0]));
    }

    /** Test thriftDeserialize method with correct class, partial byte array. Expected = throws RuntimeException */
    @Test
    public void thriftDeserializeCorrectClassPartialByteThrowsRuntimeException() {

        Assert.assertThrows(RuntimeException.class, () -> {
            AccessControl expected = new AccessControl();
            expected.set_type(AccessControlType.USER);
            expected.set_access(0);
            byte[] serialized = Utils.thriftSerialize(expected);
            byte[] partial = Arrays.copyOf(serialized, serialized.length - 1);
            Utils.thriftDeserialize(AccessControl.class, partial);
        });
    }

    /** Test thriftDeserialize method with correct class, full byte array. Expected = returns deserialized object */
    @Test
    public void thriftDeserializeCorrectClassFullByteShouldPass() {

        AccessControl expected = new AccessControl();
        expected.set_type(AccessControlType.USER);
        expected.set_access(0);
        byte[] serialized = Utils.thriftSerialize(expected);
        AccessControl deserialized = Utils.thriftDeserialize(AccessControl.class, serialized);
        Assert.assertEquals(expected, deserialized);
    }

    // URL DECODE UTF8 TESTS

    /** Test urlDecodeUtf8 methods with null value. Expected = Throws NullPointerException. */
    @Test
    public void urlEncodeDecodeUtf8NullValueThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.urlEncodeUtf8(null));
    }

    /** Test urlDecodeUtf8 methods with empty value. Expected = Returns empty string. */
    @Test
    public void urlEncodeDecodeUtf8EmptyValueShouldPass() {

        String encoded = Utils.urlEncodeUtf8("");
        String decoded = Utils.urlDecodeUtf8(encoded);
        Assert.assertEquals("", decoded);
    }

    /** Test urlDecodeUtf8 methods with valid value. Expected = Returns original decoded string. */
    @Test
    public void urlEncodeDecodeUtf8CorrectValueShouldPass() {

        String original = "hello world";
        String encoded = Utils.urlEncodeUtf8(original);
        String decoded = Utils.urlDecodeUtf8(encoded);
        Assert.assertEquals(original, decoded);
    }

    /** Test urlDecodeUtf8 method with invalid encoded value. Expected = Throws IllegalArgumentException. */
    @Test
    public void urlDecodeUtf8NotCorrectEncodedValueThrowsIllegalArgumentException() {

        Assert.assertThrows(IllegalArgumentException.class, () -> Utils.urlDecodeUtf8("%ZZ"));
    }

    //URL ENCODE UTF8 IS IMPLICIT TESTED WITH PREVIOUS TESTS (LITTLE PROBABILITY OF COMPENSATION, BUT I DON'T WORRY ABOUT)

    //--------------------------------//
    //EVOLVED TESTS WITH MUTATION TESTING//
    //--------------------------------//

    // PARSE JSON TESTS

    /** Test parseJson method with null string. Expected = returns mutable empty map. */
    @Test
    public void parseJsonNull2ShouldPass() {

        Map<String, Object> result = Utils.parseJson(null);
        Assert.assertTrue(result.isEmpty());
        result.put("added", "value");
        Assert.assertEquals("value", result.get("added"));
    }

    /** Test javaDeserialize method with valid serialized object and rejecting custom class loader. Expected = false. */
    // @Test
    public void javaDeserializeWithCustomRejectingClassLoaderShouldPass() throws Exception {

        byte[] serialized = Utils.javaSerialize(new SerializableClass("value"));
        Field clField = Utils.class.getDeclaredField("cl");
        clField.setAccessible(true);
        ClassLoader originalClassLoader = (ClassLoader) clField.get(null);
        ClassLoader rejectingClassLoader = new ClassLoader(UtilsEvolvedCFTest.class.getClassLoader()) {

            @Override
            public Class<?> loadClass(String name) throws ClassNotFoundException {

                if (name.equals(SerializableClass.class.getName())) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name);
            }
        };
        boolean thrown;
        try {
            thrown = false;
            clField.set(null, rejectingClassLoader);
            Utils.javaDeserialize(serialized, SerializableClass.class);

        }catch (Exception e){
            thrown = true;
        }finally {
            clField.set(null, originalClassLoader);
        }
        Assert.assertFalse(thrown);
    }

    // FIND AND READ CONFIG FILE TESTS

    /** Test findAndReadConfigFile method with empty yaml file and mustExists true. Expected = throws RuntimeException. */
    @Test
    public void findAndReadConfigFileEmptyYamlMustExistsThrowsRuntimeException() throws Exception {

        File emptyYaml = new File("emptyYamlForPit.yaml");
        Assert.assertTrue(emptyYaml.createNewFile());
        try {
            RuntimeException ex = Assert.assertThrows(RuntimeException.class, () -> Utils.findAndReadConfigFile("emptyYamlForPit.yaml", true));
            Assert.assertTrue(ex.getMessage().contains("doesn't have any valid storm configs"));
        } finally {
            emptyYaml.delete();
        }
    }

    /** Test findAndReadConfigFile single argument method with valid yaml file. Expected = returns parsed config map. */
    @Test
    public void findAndReadConfigFileSingleArgumentShouldPass() throws Exception {  // OVERLOAD COVERED

        Map<String, Object> result = Utils.findAndReadConfigFile("correctYamlFile.yaml");
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("value1", result.get("key1"));
        Assert.assertEquals("value2", result.get("key2"));
    }

}
