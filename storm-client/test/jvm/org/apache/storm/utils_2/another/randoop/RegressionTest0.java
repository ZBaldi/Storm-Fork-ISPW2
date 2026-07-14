package org.apache.storm.utils_2.another.randoop;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RegressionTest0 {

    public static boolean debug = false;

    public void assertBooleanArrayEquals(boolean[] expectedArray, boolean[] actualArray) {
        if (expectedArray.length != actualArray.length) {
            throw new AssertionError("Array lengths differ: " + expectedArray.length + " != " + actualArray.length);
        }
        for (int i = 0; i < expectedArray.length; i++) {
            if (expectedArray[i] != actualArray[i]) {
                throw new AssertionError("Arrays differ at index " + i + ": " + expectedArray[i] + " != " + actualArray[i]);
            }
        }
    }

    @Test
    public void test001() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test001");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("hi!");
    }

    @Test
    public void test002() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test002");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 1L, (java.lang.Long) 10L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 11L + "'", long2 == 11L);
    }

    @Test
    public void test003() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test003");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) (-1.0d));
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-1.0d) + "'", double1 == (-1.0d));
    }

    @Test
    public void test004() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test004");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test005() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test005");
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.utils.refactored.two.Utils.validateTopologyName("");
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: Topology name '' is not valid. It can't be null and it must match ^[^/.:\\\\]+$");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
    }

    @Test
    public void test006() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test006");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) 1.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 1.0d + "'", double1 == 1.0d);
    }

    @Test
    public void test007() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test007");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) (-1.0f));
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-1.0d) + "'", double1 == (-1.0d));
    }

    @Test
    public void test008() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test008");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) 10.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 10.0d + "'", double1 == 10.0d);
    }

    @Test
    public void test009() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test009");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test010() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test010");
        java.nio.ByteBuffer byteBuffer0 = null;
        // The following exception was thrown during execution in test generation
        try {
            byte[] byteArray1 = org.apache.storm.utils.refactored.two.Utils.toByteArray(byteBuffer0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"java.nio.ByteBuffer.remaining()\" because \"buffer\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test011() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test011");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("0hi!100");
    }

    @Test
    public void test012() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test012");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.Object obj4 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Object) exception0, (java.lang.Object) '#');
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertEquals("'" + obj4 + "' != '" + '#' + "'", obj4, '#');
    }

    @Test
    public void test013() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test013");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("hi!");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "hi!" + "'", str1, "hi!");
    }

    @Test
    public void test014() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test014");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 0L, (java.lang.Long) 0L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 0L + "'", long2 == 0L);
    }

    @Test
    public void test015() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test015");
        java.lang.CharSequence charSequence2 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.CharSequence) "hi!", (java.lang.CharSequence) "");
        org.junit.Assert.assertEquals("'" + charSequence2 + "' != '" + "hi!" + "'", charSequence2, "hi!");
    }

    @Test
    public void test016() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test016");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 0);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 0.0d + "'", double1 == 0.0d);
    }

    @Test
    public void test017() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test017");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) ' ');
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 32.0d + "'", double1 == 32.0d);
    }

    @Test
    public void test018() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test018");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("0hi!100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test019() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test019");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) 32.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 32.0d + "'", double1 == 32.0d);
    }

    @Test
    public void test020() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test020");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive(10);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 10 + "'", int1 == 10);
    }

    @Test
    public void test021() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test021");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (short) 0);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 0 + "'", int1 == 0);
    }

    @Test
    public void test022() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test022");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 11L, (java.lang.Long) 10L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 1L + "'", long2 == 1L);
    }

    @Test
    public void test023() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test023");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("hi!");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "hi%21" + "'", str1, "hi%21");
    }

    @Test
    public void test024() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test024");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "" + "'", str1, "");
    }

    @Test
    public void test025() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test025");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 1L, (java.lang.Long) 0L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 1L + "'", long2 == 1L);
    }

    @Test
    public void test026() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test026");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (short) 1);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 1 + "'", int1 == 1);
    }

    @Test
    public void test027() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test027");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive(0);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 0 + "'", int1 == 0);
    }

    @Test
    public void test028() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test028");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String[] strArray13 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList14 = new java.util.ArrayList<java.lang.String>();
        boolean boolean15 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList14, strArray13);
        java.util.List<java.lang.String> strList16 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList14);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) strList14);
        long long18 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertNotNull(strArray13);
        org.junit.Assert.assertArrayEquals(strArray13, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean15 + "' != '" + true + "'", boolean15 == true);
        org.junit.Assert.assertNotNull(strList16);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 100L + "'", long18 == 100L);
    }

    @Test
    public void test029() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test029");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) -1, (int) 'a');
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test030() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test030");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String[] strArray13 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList14 = new java.util.ArrayList<java.lang.String>();
        boolean boolean15 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList14, strArray13);
        java.util.List<java.lang.String> strList16 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList14);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) strList14);
        java.lang.Object obj19 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Object) strList14, (java.lang.Object) 0L);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertNotNull(strArray13);
        org.junit.Assert.assertArrayEquals(strArray13, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean15 + "' != '" + true + "'", boolean15 == true);
        org.junit.Assert.assertNotNull(strList16);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertNotNull(obj19);
        org.junit.Assert.assertEquals(obj19.toString(), "[, hi!, 0hi!100]");
        org.junit.Assert.assertEquals(java.lang.String.valueOf(obj19), "[, hi!, 0hi!100]");
        org.junit.Assert.assertEquals(java.util.Objects.toString(obj19), "[, hi!, 0hi!100]");
    }

    @Test
    public void test031() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test031");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.lang.Object[] objArray8 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList9 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray8);
        java.lang.Object[] objArray13 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList14 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray13);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList15 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean16 = objListList15.add(objList4);
        boolean boolean17 = objListList15.add(objList9);
        boolean boolean18 = objListList15.add(objList14);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap19 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList15);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objArray8);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray8), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray8), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList9);
        org.junit.Assert.assertNotNull(objArray13);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray13), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray13), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList14);
        org.junit.Assert.assertTrue("'" + boolean16 + "' != '" + true + "'", boolean16 == true);
        org.junit.Assert.assertTrue("'" + boolean17 + "' != '" + true + "'", boolean17 == true);
        org.junit.Assert.assertTrue("'" + boolean18 + "' != '" + true + "'", boolean18 == true);
        org.junit.Assert.assertNotNull(objMap19);
    }

    @Test
    public void test032() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test032");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, (int) '#');
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test033() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test033");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("hi!");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test034() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test034");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((-1));
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 2147483647 + "'", int1 == 2147483647);
    }

    @Test
    public void test035() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test035");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(1, (int) (byte) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test036() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test036");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String[] strArray13 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList14 = new java.util.ArrayList<java.lang.String>();
        boolean boolean15 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList14, strArray13);
        java.util.List<java.lang.String> strList16 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList14);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) strList14);
        java.util.List<java.lang.String> strList18 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList14);
        java.util.List<java.lang.String> strList19 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList14);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertNotNull(strArray13);
        org.junit.Assert.assertArrayEquals(strArray13, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean15 + "' != '" + true + "'", boolean15 == true);
        org.junit.Assert.assertNotNull(strList16);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertNotNull(strList18);
        org.junit.Assert.assertNotNull(strList19);
    }

    @Test
    public void test037() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test037");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (byte) -1);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 2147483647 + "'", int1 == 2147483647);
    }

    @Test
    public void test038() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test038");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("hi%21");
    }

    @Test
    public void test039() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test039");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(10, (int) (byte) -1);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test040() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test040");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("hi!");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test041() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test041");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("0hi!100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test042() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test042");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("0hi!100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0hi%21100" + "'", str1, "0hi%21100");
    }

    @Test
    public void test043() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test043");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) 0.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 0.0d + "'", double1 == 0.0d);
    }

    @Test
    public void test044() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test044");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 1);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 1.0d + "'", double1 == 1.0d);
    }

    @Test
    public void test045() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test045");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("00hi!100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi!100100" + "'", str1, "00hi!100100");
    }

    @Test
    public void test046() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test046");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("hi!");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test047() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test047");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) (short) 100);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 100.0d + "'", double1 == 100.0d);
    }

    @Test
    public void test048() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test048");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("00hi!100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test049() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test049");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("0hi%21100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0hi%2521100" + "'", str1, "0hi%2521100");
    }

    @Test
    public void test050() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test050");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("0hi%2521100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test051() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test051");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String[] strArray13 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList14 = new java.util.ArrayList<java.lang.String>();
        boolean boolean15 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList14, strArray13);
        java.util.List<java.lang.String> strList16 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList14);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) strList14);
        java.lang.String str19 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi!100");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertNotNull(strArray13);
        org.junit.Assert.assertArrayEquals(strArray13, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean15 + "' != '" + true + "'", boolean15 == true);
        org.junit.Assert.assertNotNull(strList16);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "00hi!100100" + "'", str19, "00hi!100100");
    }

    @Test
    public void test052() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test052");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("0hi%21100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0hi!100" + "'", str1, "0hi!100");
    }

    @Test
    public void test053() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test053");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("0hi%21100");
    }

    @Test
    public void test054() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test054");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) 'a', (int) (byte) 1);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test055() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test055");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) '4');
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 52.0d + "'", double1 == 52.0d);
    }

    @Test
    public void test056() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test056");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("0hi%2521100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test057() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test057");
        java.lang.Comparable<java.lang.String> strComparable2 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Comparable<java.lang.String>) "", (java.lang.Comparable<java.lang.String>) "0hi%21100");
        org.junit.Assert.assertEquals("'" + strComparable2 + "' != '" + "" + "'", strComparable2, "");
    }

    @Test
    public void test058() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test058");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("hi%21");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test059() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test059");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (byte) 0);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 0 + "'", int1 == 0);
    }

    @Test
    public void test060() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test060");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-1L), (java.lang.Long) 100L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-101L) + "'", long2 == (-101L));
    }

    @Test
    public void test061() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test061");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("000hi!100100100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test062() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test062");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        long long17 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 100, (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
        org.junit.Assert.assertNotNull(longListList18);
    }

    @Test
    public void test063() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test063");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 10L);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 10.0d + "'", double1 == 10.0d);
    }

    @Test
    public void test064() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test064");
        java.lang.Long[] longArray6 = new java.lang.Long[] { 10L, 100L, 10L, 1L, (-101L), (-1L) };
        java.util.ArrayList<java.lang.Long> longList7 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean8 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList7, longArray6);
        long long9 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList7);
        org.junit.Assert.assertNotNull(longArray6);
        org.junit.Assert.assertArrayEquals(longArray6, new java.lang.Long[] { 10L, 100L, 10L, 1L, (-101L), (-1L) });
        org.junit.Assert.assertTrue("'" + boolean8 + "' != '" + true + "'", boolean8 == true);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 1L + "'", long9 == 1L);
    }

    @Test
    public void test065() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test065");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test066() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test066");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive(1);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 1 + "'", int1 == 1);
    }

    @Test
    public void test067() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test067");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (byte) 10);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 10 + "'", int1 == 10);
    }

    @Test
    public void test068() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test068");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) '4');
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 52 + "'", int1 == 52);
    }

    @Test
    public void test069() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test069");
        java.lang.constant.Constable constable2 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.constant.Constable) (short) 10, (java.lang.constant.Constable) 100.0d);
        org.junit.Assert.assertEquals("'" + constable2 + "' != '" + (short) 10 + "'", constable2, (short) 10);
    }

    @Test
    public void test070() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test070");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(10, 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test071() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test071");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("00hi!100100");
    }

    @Test
    public void test072() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test072");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 11L, (java.lang.Long) (-1L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-12L) + "'", long2 == (-12L));
    }

    @Test
    public void test073() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test073");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException3);
        java.lang.Exception exception5 = null;
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException10 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException9);
        java.lang.RuntimeException runtimeException11 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException10);
        java.lang.RuntimeException runtimeException12 = org.apache.storm.utils.refactored.two.Utils.OR(runtimeException4, runtimeException10);
        java.lang.RuntimeException runtimeException13 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException10);
        java.lang.RuntimeException runtimeException14 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException10);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
        org.junit.Assert.assertNotNull(runtimeException10);
        org.junit.Assert.assertNotNull(runtimeException11);
        org.junit.Assert.assertNotNull(runtimeException12);
        org.junit.Assert.assertNotNull(runtimeException13);
        org.junit.Assert.assertNotNull(runtimeException14);
    }

    @Test
    public void test074() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test074");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("0hi%21100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test075() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test075");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, (int) (short) -1);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap5 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (short) 1);
        java.util.SequencedMap<java.lang.Integer, java.lang.Integer> intMap6 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap2, (java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap5);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap9 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, (int) (short) -1);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap12 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (short) 1);
        java.util.SequencedMap<java.lang.Integer, java.lang.Integer> intMap13 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap9, (java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap12);
        java.util.SortedMap<java.lang.Integer, java.lang.Integer> intMap14 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SortedMap<java.lang.Integer, java.lang.Integer>) intMap2, (java.util.SortedMap<java.lang.Integer, java.lang.Integer>) intMap9);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap17 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) '#', (int) (short) 100);
        java.util.AbstractMap<java.lang.Integer, java.lang.Integer> intMap18 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.AbstractMap<java.lang.Integer, java.lang.Integer>) intMap9, (java.util.AbstractMap<java.lang.Integer, java.lang.Integer>) intMap17);
        org.junit.Assert.assertNotNull(intMap2);
        org.junit.Assert.assertNotNull(intMap5);
        org.junit.Assert.assertNotNull(intMap6);
        org.junit.Assert.assertNotNull(intMap9);
        org.junit.Assert.assertNotNull(intMap12);
        org.junit.Assert.assertNotNull(intMap13);
        org.junit.Assert.assertNotNull(intMap14);
        org.junit.Assert.assertNotNull(intMap17);
        org.junit.Assert.assertNotNull(intMap18);
    }

    @Test
    public void test076() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test076");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 100L, (java.lang.Long) (-12L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-112L) + "'", long2 == (-112L));
    }

    @Test
    public void test077() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test077");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 2147483647);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 2.147483647E9d + "'", double1 == 2.147483647E9d);
    }

    @Test
    public void test078() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test078");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        java.util.List<java.util.List<java.lang.Long>> longListList17 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) '4', (java.util.Collection<java.lang.Long>) longList10);
        java.lang.String str19 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "0hi!100");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertNotNull(longListList17);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "00hi!100100" + "'", str19, "00hi!100100");
    }

    @Test
    public void test079() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test079");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("0hi%2521100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0hi%252521100" + "'", str1, "0hi%252521100");
    }

    @Test
    public void test080() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test080");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 1L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.util.List<java.util.List<java.lang.Long>> longListList6 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) -1, (java.util.Collection<java.lang.Long>) longList3);
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 1L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 1L + "'", long5 == 1L);
        org.junit.Assert.assertNotNull(longListList6);
    }

    @Test
    public void test081() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test081");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("00hi!100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test082() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test082");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 11L, (java.lang.Long) (-101L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-112L) + "'", long2 == (-112L));
    }

    @Test
    public void test083() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test083");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(0, 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test084() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test084");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) 'a', (int) (short) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test085() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test085");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 10);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 10.0d + "'", double1 == 10.0d);
    }

    @Test
    public void test086() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test086");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("0hi%21100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test087() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test087");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) (-112L));
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-112.0d) + "'", double1 == (-112.0d));
    }

    @Test
    public void test088() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test088");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) 52.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 52.0d + "'", double1 == 52.0d);
    }

    @Test
    public void test089() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test089");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(2147483647, (int) '#');
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test090() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test090");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 0L, (java.lang.Long) 1L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 1L + "'", long2 == 1L);
    }

    @Test
    public void test091() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test091");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.Long[] longArray10 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList11 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean12 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList11, longArray10);
        long long13 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList11);
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList11, "hi!");
        long long16 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList11);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList5, (java.util.RandomAccess) longList11);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 100, (java.util.Collection<java.lang.Long>) longList5);
        java.util.List<java.util.List<java.lang.Long>> longListList19 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(1, (java.util.Collection<java.lang.Long>) longList5);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertNotNull(longArray10);
        org.junit.Assert.assertArrayEquals(longArray10, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean12 + "' != '" + true + "'", boolean12 == true);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "0hi!100" + "'", str15, "0hi!100");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 100L + "'", long16 == 100L);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertNotNull(longListList18);
        org.junit.Assert.assertNotNull(longListList19);
    }

    @Test
    public void test092() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test092");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (short) 10);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 10 + "'", int1 == 10);
    }

    @Test
    public void test093() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test093");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("hi%21");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test094() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test094");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.Long[] longArray10 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList11 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean12 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList11, longArray10);
        long long13 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList11);
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList11, "hi!");
        long long16 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList11);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList5, (java.util.RandomAccess) longList11);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 100, (java.util.Collection<java.lang.Long>) longList5);
        java.util.List<java.util.List<java.lang.Long>> longListList19 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) '#', (java.util.Collection<java.lang.Long>) longList5);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertNotNull(longArray10);
        org.junit.Assert.assertArrayEquals(longArray10, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean12 + "' != '" + true + "'", boolean12 == true);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "0hi!100" + "'", str15, "0hi!100");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 100L + "'", long16 == 100L);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertNotNull(longListList18);
        org.junit.Assert.assertNotNull(longListList19);
    }

    @Test
    public void test095() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test095");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("00hi%21100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%2521100100" + "'", str1, "00hi%2521100100");
    }

    @Test
    public void test096() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test096");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((-1.0d));
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-1.0d) + "'", double1 == (-1.0d));
    }

    @Test
    public void test097() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test097");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive(100);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 100 + "'", int1 == 100);
    }

    @Test
    public void test098() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test098");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (byte) 1);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 1 + "'", int1 == 1);
    }

    @Test
    public void test099() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test099");
        java.lang.Comparable<java.lang.String> strComparable2 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Comparable<java.lang.String>) "0hi!100", (java.lang.Comparable<java.lang.String>) "00hi%21100100");
        org.junit.Assert.assertEquals("'" + strComparable2 + "' != '" + "0hi!100" + "'", strComparable2, "0hi!100");
    }

    @Test
    public void test100() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test100");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        long long17 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(1, (java.util.Collection<java.lang.Long>) longList10);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
        org.junit.Assert.assertNotNull(longListList18);
    }

    @Test
    public void test101() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test101");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("00hi!100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%21100100" + "'", str1, "00hi%21100100");
    }

    @Test
    public void test102() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test102");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("00hi%21100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi!100100" + "'", str1, "00hi!100100");
    }

    @Test
    public void test103() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test103");
        java.lang.constant.Constable constable2 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.constant.Constable) (-101L), (java.lang.constant.Constable) 1.0f);
        org.junit.Assert.assertEquals("'" + constable2 + "' != '" + (-101L) + "'", constable2, (-101L));
    }

    @Test
    public void test104() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test104");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.lang.Object[] objArray9 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList10 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray9);
        java.util.List<java.lang.Object> objList11 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray9);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList12 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean13 = objListList12.add(objList5);
        boolean boolean14 = objListList12.add(objList11);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap15 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap16 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap17 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap16);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap18 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap16);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap19 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap16);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertNotNull(objArray9);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray9), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray9), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList10);
        org.junit.Assert.assertNotNull(objList11);
        org.junit.Assert.assertTrue("'" + boolean13 + "' != '" + true + "'", boolean13 == true);
        org.junit.Assert.assertTrue("'" + boolean14 + "' != '" + true + "'", boolean14 == true);
        org.junit.Assert.assertNotNull(objMap15);
        org.junit.Assert.assertNotNull(objMap16);
        org.junit.Assert.assertNotNull(objListMap17);
        org.junit.Assert.assertNotNull(objListMap18);
        org.junit.Assert.assertNotNull(objListMap19);
    }

    @Test
    public void test105() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test105");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.Long[] longArray8 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList9 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean10 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList9, longArray8);
        long long11 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList9);
        java.lang.String str13 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList9, "hi!");
        long long14 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList9);
        java.util.RandomAccess randomAccess15 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList3, (java.util.RandomAccess) longList9);
        java.lang.String str17 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList9, "0hi!100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertNotNull(longArray8);
        org.junit.Assert.assertArrayEquals(longArray8, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean10 + "' != '" + true + "'", boolean10 == true);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "0hi!100" + "'", str13, "0hi!100");
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 100L + "'", long14 == 100L);
        org.junit.Assert.assertNotNull(randomAccess15);
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "00hi!100100" + "'", str17, "00hi!100100");
    }

    @Test
    public void test106() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test106");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String[] strArray13 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList14 = new java.util.ArrayList<java.lang.String>();
        boolean boolean15 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList14, strArray13);
        java.util.List<java.lang.String> strList16 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList14);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) strList14);
        java.util.List<java.lang.String> strList18 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList14);
        java.util.List<java.lang.String> strList19 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList18);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertNotNull(strArray13);
        org.junit.Assert.assertArrayEquals(strArray13, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean15 + "' != '" + true + "'", boolean15 == true);
        org.junit.Assert.assertNotNull(strList16);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertNotNull(strList18);
        org.junit.Assert.assertNotNull(strList19);
    }

    @Test
    public void test107() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test107");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((-1), (int) (short) 100);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test108() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test108");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("0hi%2521100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0hi%21100" + "'", str1, "0hi%21100");
    }

    @Test
    public void test109() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test109");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("00hi%2521100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%21100100" + "'", str1, "00hi%21100100");
    }

    @Test
    public void test110() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test110");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-101L), (java.lang.Long) (-101L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 0L + "'", long2 == 0L);
    }

    @Test
    public void test111() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test111");
        java.lang.Object[] objArray1 = new java.lang.Object[] { "0hi!100" };
        java.util.List<java.lang.Object> objList2 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.util.List<java.lang.Object> objList3 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        org.junit.Assert.assertNotNull(objArray1);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray1), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray1), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList2);
        org.junit.Assert.assertNotNull(objList3);
        org.junit.Assert.assertNotNull(objList4);
    }

    @Test
    public void test112() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test112");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("0hi%252521100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test113() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test113");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("00hi%2521100100");
    }

    @Test
    public void test114() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test114");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("0hi!100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test115() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test115");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf(0.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 0.0d + "'", double1 == 0.0d);
    }

    @Test
    public void test116() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test116");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList10 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList5);
        java.lang.String[] strArray14 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList15 = new java.util.ArrayList<java.lang.String>();
        boolean boolean16 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList15, strArray14);
        java.util.List<java.lang.String> strList17 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList15);
        java.util.RandomAccess randomAccess18 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList5, (java.util.RandomAccess) strList15);
        java.util.List<java.util.List<java.lang.Long>> longListList19 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 0, (java.util.Collection<java.lang.Long>) longList5);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "0hi!100" + "'", str9, "0hi!100");
        org.junit.Assert.assertNotNull(longListList10);
        org.junit.Assert.assertNotNull(strArray14);
        org.junit.Assert.assertArrayEquals(strArray14, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean16 + "' != '" + true + "'", boolean16 == true);
        org.junit.Assert.assertNotNull(strList17);
        org.junit.Assert.assertNotNull(randomAccess18);
        org.junit.Assert.assertNotNull(longListList19);
    }

    @Test
    public void test117() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test117");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.Long[] longArray8 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList9 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean10 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList9, longArray8);
        long long11 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList9);
        java.lang.String str13 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList9, "hi!");
        long long14 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList9);
        java.util.RandomAccess randomAccess15 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList3, (java.util.RandomAccess) longList9);
        long long16 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str18 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "0hi%252521100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertNotNull(longArray8);
        org.junit.Assert.assertArrayEquals(longArray8, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean10 + "' != '" + true + "'", boolean10 == true);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "0hi!100" + "'", str13, "0hi!100");
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 100L + "'", long14 == 100L);
        org.junit.Assert.assertNotNull(randomAccess15);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 100L + "'", long16 == 100L);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "00hi%252521100100" + "'", str18, "00hi%252521100100");
    }

    @Test
    public void test118() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test118");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("000hi%2521100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi%21100100100" + "'", str1, "000hi%21100100100");
    }

    @Test
    public void test119() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test119");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("00hi%2521100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test120() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test120");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("000hi%21100100100");
    }

    @Test
    public void test121() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test121");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("000hi%2521100100100");
    }

    @Test
    public void test122() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test122");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("0100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0100" + "'", str1, "0100");
    }

    @Test
    public void test123() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test123");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00hi!100100");
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "000hi!100100100");
        long long10 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "000hi!100100100" + "'", str7, "000hi!100100100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "0000hi!100100100100" + "'", str9, "0000hi!100100100100");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
    }

    @Test
    public void test124() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test124");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("0hi%252521100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test125() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test125");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, (int) (short) -1);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap5 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (short) 1);
        java.util.SequencedMap<java.lang.Integer, java.lang.Integer> intMap6 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap2, (java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap5);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap9 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, (int) (short) -1);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap12 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (short) 1);
        java.util.SequencedMap<java.lang.Integer, java.lang.Integer> intMap13 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap9, (java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap12);
        java.util.SortedMap<java.lang.Integer, java.lang.Integer> intMap14 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SortedMap<java.lang.Integer, java.lang.Integer>) intMap2, (java.util.SortedMap<java.lang.Integer, java.lang.Integer>) intMap9);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap17 = org.apache.storm.utils.refactored.two.Utils.integerDivided(2147483647, (int) (byte) 1);
        java.util.SequencedMap<java.lang.Integer, java.lang.Integer> intMap18 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap14, (java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap17);
        org.junit.Assert.assertNotNull(intMap2);
        org.junit.Assert.assertNotNull(intMap5);
        org.junit.Assert.assertNotNull(intMap6);
        org.junit.Assert.assertNotNull(intMap9);
        org.junit.Assert.assertNotNull(intMap12);
        org.junit.Assert.assertNotNull(intMap13);
        org.junit.Assert.assertNotNull(intMap14);
        org.junit.Assert.assertNotNull(intMap17);
        org.junit.Assert.assertNotNull(intMap18);
    }

    @Test
    public void test126() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test126");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("0100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + true + "'", boolean1 == true);
    }

    @Test
    public void test127() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test127");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.lang.Object[] objArray9 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList10 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray9);
        java.util.List<java.lang.Object> objList11 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray9);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList12 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean13 = objListList12.add(objList5);
        boolean boolean14 = objListList12.add(objList11);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap15 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap16 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap17 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap18 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap19 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertNotNull(objArray9);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray9), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray9), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList10);
        org.junit.Assert.assertNotNull(objList11);
        org.junit.Assert.assertTrue("'" + boolean13 + "' != '" + true + "'", boolean13 == true);
        org.junit.Assert.assertTrue("'" + boolean14 + "' != '" + true + "'", boolean14 == true);
        org.junit.Assert.assertNotNull(objMap15);
        org.junit.Assert.assertNotNull(objMap16);
        org.junit.Assert.assertNotNull(objMap17);
        org.junit.Assert.assertNotNull(objMap18);
        org.junit.Assert.assertNotNull(objMap19);
    }

    @Test
    public void test128() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test128");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) 100.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 100.0d + "'", double1 == 100.0d);
    }

    @Test
    public void test129() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test129");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) (short) 1);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 1.0d + "'", double1 == 1.0d);
    }

    @Test
    public void test130() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test130");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        java.util.List<java.util.List<java.lang.Long>> longListList17 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 100, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str19 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "00hi%21100100");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertNotNull(longListList17);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "000hi%21100100100" + "'", str19, "000hi%21100100100");
    }

    @Test
    public void test131() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test131");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList10 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(100, (java.util.Collection<java.lang.Long>) longList5);
        long long11 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.util.List<java.util.List<java.lang.Long>> longListList12 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 10, (java.util.Collection<java.lang.Long>) longList5);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "0hi!100" + "'", str9, "0hi!100");
        org.junit.Assert.assertNotNull(longListList10);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
        org.junit.Assert.assertNotNull(longListList12);
    }

    @Test
    public void test132() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test132");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("hi%21");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "hi%2521" + "'", str1, "hi%2521");
    }

    @Test
    public void test133() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test133");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(2147483647, 10);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test134() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test134");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.lang.Object[] objArray9 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList10 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray9);
        java.util.List<java.lang.Object> objList11 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray9);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList12 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean13 = objListList12.add(objList5);
        boolean boolean14 = objListList12.add(objList11);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap15 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap16 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap17 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap18 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap19 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap18);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertNotNull(objArray9);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray9), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray9), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList10);
        org.junit.Assert.assertNotNull(objList11);
        org.junit.Assert.assertTrue("'" + boolean13 + "' != '" + true + "'", boolean13 == true);
        org.junit.Assert.assertTrue("'" + boolean14 + "' != '" + true + "'", boolean14 == true);
        org.junit.Assert.assertNotNull(objMap15);
        org.junit.Assert.assertNotNull(objMap16);
        org.junit.Assert.assertNotNull(objMap17);
        org.junit.Assert.assertNotNull(objMap18);
        org.junit.Assert.assertNotNull(objListMap19);
    }

    @Test
    public void test135() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test135");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("hi%21");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test136() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test136");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.Long[] longArray10 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList11 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean12 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList11, longArray10);
        long long13 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList11);
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList11, "hi!");
        long long16 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList11);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList5, (java.util.RandomAccess) longList11);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) '4', (java.util.Collection<java.lang.Long>) longList11);
        java.util.List<java.util.List<java.lang.Long>> longListList19 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 0, (java.util.Collection<java.lang.Long>) longList11);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertNotNull(longArray10);
        org.junit.Assert.assertArrayEquals(longArray10, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean12 + "' != '" + true + "'", boolean12 == true);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "0hi!100" + "'", str15, "0hi!100");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 100L + "'", long16 == 100L);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertNotNull(longListList18);
        org.junit.Assert.assertNotNull(longListList19);
    }

    @Test
    public void test137() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test137");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap10 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap11 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap10);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objMap9);
        org.junit.Assert.assertNotNull(objMap10);
        org.junit.Assert.assertNotNull(objListMap11);
    }

    @Test
    public void test138() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test138");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 1L, (java.lang.Long) (-112L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-111L) + "'", long2 == (-111L));
    }

    @Test
    public void test139() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test139");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) (-112.0d));
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-112.0d) + "'", double1 == (-112.0d));
    }

    @Test
    public void test140() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test140");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("0hi%2521100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test141() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test141");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) '#', (int) (byte) 100);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test142() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test142");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (short) -1);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 2147483647 + "'", int1 == 2147483647);
    }

    @Test
    public void test143() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test143");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("000hi!100100100");
    }

    @Test
    public void test144() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test144");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) ' ');
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 32 + "'", int1 == 32);
    }

    @Test
    public void test145() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test145");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 0L, (java.lang.Long) (-1L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-1L) + "'", long2 == (-1L));
    }

    @Test
    public void test146() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test146");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("1");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + true + "'", boolean1 == true);
    }

    @Test
    public void test147() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test147");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) '4', (int) (byte) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test148() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test148");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("0hi%2521100");
    }

    @Test
    public void test149() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test149");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.Long[] longArray8 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList9 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean10 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList9, longArray8);
        long long11 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList9);
        java.lang.String str13 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList9, "hi!");
        long long14 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList9);
        java.util.RandomAccess randomAccess15 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList3, (java.util.RandomAccess) longList9);
        long long16 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList9);
        java.lang.String str18 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList9, "0000hi!100100100100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertNotNull(longArray8);
        org.junit.Assert.assertArrayEquals(longArray8, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean10 + "' != '" + true + "'", boolean10 == true);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "0hi!100" + "'", str13, "0hi!100");
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 100L + "'", long14 == 100L);
        org.junit.Assert.assertNotNull(randomAccess15);
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 100L + "'", long16 == 100L);
        org.junit.Assert.assertEquals("'" + str18 + "' != '" + "00000hi!100100100100100" + "'", str18, "00000hi!100100100100100");
    }

    @Test
    public void test150() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test150");
        java.lang.Object[] objArray1 = new java.lang.Object[] { "0hi!100" };
        java.util.List<java.lang.Object> objList2 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.lang.Object[] objArray5 = new java.lang.Object[] { "0hi!100", (-1) };
        java.lang.Object[] objArray6 = org.apache.storm.utils.refactored.two.Utils.OR(objArray1, objArray5);
        java.util.List<java.lang.Object> objList7 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray6);
        java.util.List<java.lang.Object> objList8 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray6);
        org.junit.Assert.assertNotNull(objArray1);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray1), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray1), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList2);
        org.junit.Assert.assertNotNull(objArray5);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertNotNull(objArray6);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray6), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray6), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList7);
        org.junit.Assert.assertNotNull(objList8);
    }

    @Test
    public void test151() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test151");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf(100.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 100.0d + "'", double1 == 100.0d);
    }

    @Test
    public void test152() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test152");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("00hi%252521100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%25252521100100" + "'", str1, "00hi%25252521100100");
    }

    @Test
    public void test153() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test153");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (short) 1);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap5 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) 1, 10);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap6 = org.apache.storm.utils.refactored.two.Utils.OR(intMap2, intMap5);
        org.junit.Assert.assertNotNull(intMap2);
        org.junit.Assert.assertNotNull(intMap5);
        org.junit.Assert.assertNotNull(intMap6);
    }

    @Test
    public void test154() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test154");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "00hi!100100");
        long long9 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList10 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 100, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str12 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "000hi!100100100");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "000hi!100100100" + "'", str8, "000hi!100100100");
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 100L + "'", long9 == 100L);
        org.junit.Assert.assertNotNull(longListList10);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "0000hi!100100100100" + "'", str12, "0000hi!100100100100");
    }

    @Test
    public void test155() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test155");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("hi%2521");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "hi%21" + "'", str1, "hi%21");
    }

    @Test
    public void test156() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test156");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 0.0f);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 0.0d + "'", double1 == 0.0d);
    }

    @Test
    public void test157() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test157");
        java.lang.Long[] longArray1 = new java.lang.Long[] { 1L };
        java.util.ArrayList<java.lang.Long> longList2 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean3 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList2, longArray1);
        long long4 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList2);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList2, "0100");
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList2, "0000hi!100100100100");
        org.junit.Assert.assertNotNull(longArray1);
        org.junit.Assert.assertArrayEquals(longArray1, new java.lang.Long[] { 1L });
        org.junit.Assert.assertTrue("'" + boolean3 + "' != '" + true + "'", boolean3 == true);
        org.junit.Assert.assertTrue("'" + long4 + "' != '" + 1L + "'", long4 == 1L);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 1L + "'", long5 == 1L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "1" + "'", str7, "1");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "1" + "'", str9, "1");
    }

    @Test
    public void test158() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test158");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("00hi%25252521100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%2525252521100100" + "'", str1, "00hi%2525252521100100");
    }

    @Test
    public void test159() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test159");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "00hi!100100");
        long long9 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList10 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 100, (java.util.Collection<java.lang.Long>) longList4);
        long long11 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "000hi!100100100" + "'", str8, "000hi!100100100");
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 100L + "'", long9 == 100L);
        org.junit.Assert.assertNotNull(longListList10);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
    }

    @Test
    public void test160() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test160");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("hi%2521");
    }

    @Test
    public void test161() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test161");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("1");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test162() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test162");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("0000hi!100100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0000hi!100100100100" + "'", str1, "0000hi!100100100100");
    }

    @Test
    public void test163() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test163");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("00hi%21100100");
    }

    @Test
    public void test164() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test164");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("hi%2521");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test165() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test165");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.lang.Object[] objArray14 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList15 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray14);
        java.util.List<java.lang.Object> objList16 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray14);
        java.util.List<java.lang.Object> objList17 = org.apache.storm.utils.refactored.two.Utils.get(objMap9, (java.lang.Object) (byte) 100, objList16);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap18 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap9);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap19 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap9);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objMap9);
        org.junit.Assert.assertNotNull(objArray14);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray14), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray14), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList15);
        org.junit.Assert.assertNotNull(objList16);
        org.junit.Assert.assertNotNull(objList17);
        org.junit.Assert.assertNotNull(objListMap18);
        org.junit.Assert.assertNotNull(objListMap19);
    }

    @Test
    public void test166() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test166");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf(2.147483647E9d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 2.147483647E9d + "'", double1 == 2.147483647E9d);
    }

    @Test
    public void test167() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test167");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("00000hi!100100100100100");
    }

    @Test
    public void test168() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test168");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("00hi%21100100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test169() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test169");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 100L, (java.lang.Long) 1L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 101L + "'", long2 == 101L);
    }

    @Test
    public void test170() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test170");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) 'a');
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 97 + "'", int1 == 97);
    }

    @Test
    public void test171() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test171");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("0hi%252521100");
    }

    @Test
    public void test172() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test172");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("000hi%2521100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test173() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test173");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "00hi!100100");
        long long10 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.util.List<java.util.List<java.lang.Long>> longListList11 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 100, (java.util.Collection<java.lang.Long>) longList5);
        java.util.List<java.util.List<java.lang.Long>> longListList12 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(2147483647, (java.util.Collection<java.lang.Long>) longList5);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "000hi!100100100" + "'", str9, "000hi!100100100");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
        org.junit.Assert.assertNotNull(longListList11);
        org.junit.Assert.assertNotNull(longListList12);
    }

    @Test
    public void test174() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test174");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        long long17 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) '4', (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
        org.junit.Assert.assertNotNull(longListList18);
    }

    @Test
    public void test175() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test175");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) '#');
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 35.0d + "'", double1 == 35.0d);
    }

    @Test
    public void test176() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test176");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-112L), (java.lang.Long) (-111L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 1L + "'", long2 == 1L);
    }

    @Test
    public void test177() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test177");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("000hi%2521100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi%252521100100100" + "'", str1, "000hi%252521100100100");
    }

    @Test
    public void test178() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test178");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-101L), (java.lang.Long) (-1L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 100L + "'", long2 == 100L);
    }

    @Test
    public void test179() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test179");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00hi!100100");
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi!");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "000hi!100100100" + "'", str7, "000hi!100100100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "0hi!100" + "'", str9, "0hi!100");
    }

    @Test
    public void test180() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test180");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("00000hi!100100100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00000hi!100100100100100" + "'", str1, "00000hi!100100100100100");
    }

    @Test
    public void test181() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test181");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 11L, (java.lang.Long) 100L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 111L + "'", long2 == 111L);
    }

    @Test
    public void test182() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test182");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("000hi!100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test183() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test183");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("000000hi!100100100100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000000hi%21100100100100100100" + "'", str1, "000000hi%21100100100100100100");
    }

    @Test
    public void test184() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test184");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 10L, (java.lang.Long) 101L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 111L + "'", long2 == 111L);
    }

    @Test
    public void test185() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test185");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        java.util.List<java.util.List<java.lang.Long>> longListList17 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 100, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str19 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertNotNull(longListList17);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "0hi!100" + "'", str19, "0hi!100");
    }

    @Test
    public void test186() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test186");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException4);
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException5);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException6);
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException6);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
    }

    @Test
    public void test187() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test187");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-1L), (java.lang.Long) 101L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-102L) + "'", long2 == (-102L));
    }

    @Test
    public void test188() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test188");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 52);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 52.0d + "'", double1 == 52.0d);
    }

    @Test
    public void test189() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test189");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("00hi%2525252521100100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test190() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test190");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("000000hi%21100100100100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000000hi!100100100100100100" + "'", str1, "000000hi!100100100100100100");
    }

    @Test
    public void test191() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test191");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(100, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "");
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        long long13 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "0100" + "'", str11, "0100");
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
    }

    @Test
    public void test192() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test192");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.lang.Object[] objArray9 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList10 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray9);
        java.util.List<java.lang.Object> objList11 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray9);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList12 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean13 = objListList12.add(objList5);
        boolean boolean14 = objListList12.add(objList11);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap15 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList12);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap16 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap15);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap17 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap15);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertNotNull(objArray9);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray9), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray9), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList10);
        org.junit.Assert.assertNotNull(objList11);
        org.junit.Assert.assertTrue("'" + boolean13 + "' != '" + true + "'", boolean13 == true);
        org.junit.Assert.assertTrue("'" + boolean14 + "' != '" + true + "'", boolean14 == true);
        org.junit.Assert.assertNotNull(objMap15);
        org.junit.Assert.assertNotNull(objListMap16);
        org.junit.Assert.assertNotNull(objListMap17);
    }

    @Test
    public void test193() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test193");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("0000hi!100100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test194() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test194");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 1L, (java.lang.Long) (-12L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-11L) + "'", long2 == (-11L));
    }

    @Test
    public void test195() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test195");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("1");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "1" + "'", str1, "1");
    }

    @Test
    public void test196() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test196");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 'a');
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 97.0d + "'", double1 == 97.0d);
    }

    @Test
    public void test197() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test197");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("000000hi%21100100100100100100");
    }

    @Test
    public void test198() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test198");
        java.lang.CharSequence charSequence2 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.CharSequence) "hi%2521", (java.lang.CharSequence) "000hi%252521100100100");
        org.junit.Assert.assertEquals("'" + charSequence2 + "' != '" + "hi%2521" + "'", charSequence2, "hi%2521");
    }

    @Test
    public void test199() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test199");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) (-12L));
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-12.0d) + "'", double1 == (-12.0d));
    }

    @Test
    public void test200() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test200");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("00000hi!100100100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test201() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test201");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 10, 100);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test202() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test202");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList10 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList5);
        java.lang.String[] strArray14 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList15 = new java.util.ArrayList<java.lang.String>();
        boolean boolean16 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList15, strArray14);
        java.util.List<java.lang.String> strList17 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList15);
        java.util.RandomAccess randomAccess18 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList5, (java.util.RandomAccess) strList15);
        java.util.List<java.util.List<java.lang.Long>> longListList19 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(1, (java.util.Collection<java.lang.Long>) longList5);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "0hi!100" + "'", str9, "0hi!100");
        org.junit.Assert.assertNotNull(longListList10);
        org.junit.Assert.assertNotNull(strArray14);
        org.junit.Assert.assertArrayEquals(strArray14, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean16 + "' != '" + true + "'", boolean16 == true);
        org.junit.Assert.assertNotNull(strList17);
        org.junit.Assert.assertNotNull(randomAccess18);
        org.junit.Assert.assertNotNull(longListList19);
    }

    @Test
    public void test203() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test203");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) ' ', (int) (short) 1);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test204() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test204");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 100L, (java.lang.Long) 10L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 110L + "'", long2 == 110L);
    }

    @Test
    public void test205() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test205");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("00hi%2521100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test206() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test206");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) 2.147483647E9d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 2.147483647E9d + "'", double1 == 2.147483647E9d);
    }

    @Test
    public void test207() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test207");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        long long9 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        long long10 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList11 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) -1, (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 100L + "'", long9 == 100L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
        org.junit.Assert.assertNotNull(longListList11);
    }

    @Test
    public void test208() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test208");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("00hi%21100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test209() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test209");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi!");
        long long8 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str10 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00hi%2521100100");
        java.lang.String str12 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00000hi!100100100100100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "0hi!100" + "'", str7, "0hi!100");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "000hi%2521100100100" + "'", str10, "000hi%2521100100100");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "000000hi!100100100100100100" + "'", str12, "000000hi!100100100100100100");
    }

    @Test
    public void test210() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test210");
        java.lang.Object[] objArray1 = new java.lang.Object[] { "0hi!100" };
        java.util.List<java.lang.Object> objList2 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.lang.Object[] objArray5 = new java.lang.Object[] { "0hi!100", (-1) };
        java.lang.Object[] objArray6 = org.apache.storm.utils.refactored.two.Utils.OR(objArray1, objArray5);
        java.util.List<java.lang.Object> objList7 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray6);
        java.lang.Object[] objArray11 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList12 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray11);
        java.util.List<java.lang.Object> objList13 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray11);
        java.util.List<java.lang.Object> objList14 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray11);
        java.lang.Object[] objArray15 = org.apache.storm.utils.refactored.two.Utils.OR(objArray6, objArray11);
        java.util.List<java.lang.Object> objList16 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray6);
        org.junit.Assert.assertNotNull(objArray1);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray1), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray1), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList2);
        org.junit.Assert.assertNotNull(objArray5);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertNotNull(objArray6);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray6), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray6), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList7);
        org.junit.Assert.assertNotNull(objArray11);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray11), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray11), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList12);
        org.junit.Assert.assertNotNull(objList13);
        org.junit.Assert.assertNotNull(objList14);
        org.junit.Assert.assertNotNull(objArray15);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray15), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray15), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList16);
    }

    @Test
    public void test211() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test211");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap10 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap11 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap12 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap11);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objMap9);
        org.junit.Assert.assertNotNull(objMap10);
        org.junit.Assert.assertNotNull(objMap11);
        org.junit.Assert.assertNotNull(objListMap12);
    }

    @Test
    public void test212() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test212");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("000hi%21100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi!100100100" + "'", str1, "000hi!100100100");
    }

    @Test
    public void test213() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test213");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, (int) (short) -1);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap5 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (short) 1);
        java.util.SequencedMap<java.lang.Integer, java.lang.Integer> intMap6 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap2, (java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap5);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap9 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, (int) (short) -1);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap12 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (short) 1);
        java.util.SequencedMap<java.lang.Integer, java.lang.Integer> intMap13 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap9, (java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap12);
        java.util.SequencedMap<java.lang.Integer, java.lang.Integer> intMap14 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.SequencedMap<java.lang.Integer, java.lang.Integer>) intMap5, intMap13);
        org.junit.Assert.assertNotNull(intMap2);
        org.junit.Assert.assertNotNull(intMap5);
        org.junit.Assert.assertNotNull(intMap6);
        org.junit.Assert.assertNotNull(intMap9);
        org.junit.Assert.assertNotNull(intMap12);
        org.junit.Assert.assertNotNull(intMap13);
        org.junit.Assert.assertNotNull(intMap14);
    }

    @Test
    public void test214() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test214");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "" + "'", str1, "");
    }

    @Test
    public void test215() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test215");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-12L), (java.lang.Long) (-112L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 100L + "'", long2 == 100L);
    }

    @Test
    public void test216() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test216");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 101L);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 101.0d + "'", double1 == 101.0d);
    }

    @Test
    public void test217() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test217");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("0hi%252521100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test218() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test218");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList6 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList7 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList8 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList9 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertNotNull(objList6);
        org.junit.Assert.assertNotNull(objList7);
        org.junit.Assert.assertNotNull(objList8);
        org.junit.Assert.assertNotNull(objList9);
    }

    @Test
    public void test219() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test219");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("00000hi!100100100100100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test220() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test220");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("00hi%2525252521100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%25252521100100" + "'", str1, "00hi%25252521100100");
    }

    @Test
    public void test221() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test221");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("00hi%2525252521100100");
    }

    @Test
    public void test222() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test222");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%21100");
        java.io.Serializable serializable13 = org.apache.storm.utils.refactored.two.Utils.OR((java.io.Serializable) longList4, (java.io.Serializable) "000hi!100100100");
        long long14 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "00hi%21100100" + "'", str11, "00hi%21100100");
        org.junit.Assert.assertNotNull(serializable13);
        org.junit.Assert.assertTrue("'" + long14 + "' != '" + 100L + "'", long14 == 100L);
    }

    @Test
    public void test223() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test223");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(0, (int) '4');
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test224() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test224");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(0, 97);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test225() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test225");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) (short) 100);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 100 + "'", int1 == 100);
    }

    @Test
    public void test226() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test226");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) 0, (int) '#');
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test227() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test227");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        long long17 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(1, (java.util.Collection<java.lang.Long>) longList4);
        long long19 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
        org.junit.Assert.assertNotNull(longListList18);
        org.junit.Assert.assertTrue("'" + long19 + "' != '" + 100L + "'", long19 == 100L);
    }

    @Test
    public void test228() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test228");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive(97);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 97 + "'", int1 == 97);
    }

    @Test
    public void test229() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test229");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        java.util.List<java.util.List<java.lang.Long>> longListList17 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) '4', (java.util.Collection<java.lang.Long>) longList10);
        java.lang.String str19 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "00000hi!100100100100100");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertNotNull(longListList17);
        org.junit.Assert.assertEquals("'" + str19 + "' != '" + "000000hi!100100100100100100" + "'", str19, "000000hi!100100100100100100");
    }

    @Test
    public void test230() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test230");
        java.lang.Object[] objArray1 = new java.lang.Object[] { "0hi!100" };
        java.util.List<java.lang.Object> objList2 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.lang.Object[] objArray5 = new java.lang.Object[] { "0hi!100", (-1) };
        java.lang.Object[] objArray6 = org.apache.storm.utils.refactored.two.Utils.OR(objArray1, objArray5);
        java.lang.Object[] objArray8 = new java.lang.Object[] { "0hi!100" };
        java.util.List<java.lang.Object> objList9 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray8);
        java.lang.Object[] objArray12 = new java.lang.Object[] { "0hi!100", (-1) };
        java.lang.Object[] objArray13 = org.apache.storm.utils.refactored.two.Utils.OR(objArray8, objArray12);
        java.util.List<java.lang.Object> objList14 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray12);
        java.lang.Object[] objArray15 = org.apache.storm.utils.refactored.two.Utils.OR(objArray5, objArray12);
        java.util.List<java.lang.Object> objList16 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray15);
        java.util.List<java.lang.Object> objList17 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray15);
        java.util.List<java.lang.Object> objList18 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray15);
        org.junit.Assert.assertNotNull(objArray1);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray1), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray1), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList2);
        org.junit.Assert.assertNotNull(objArray5);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertNotNull(objArray6);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray6), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray6), "[0hi!100]");
        org.junit.Assert.assertNotNull(objArray8);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray8), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray8), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList9);
        org.junit.Assert.assertNotNull(objArray12);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray12), "[0hi!100, -1]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray12), "[0hi!100, -1]");
        org.junit.Assert.assertNotNull(objArray13);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray13), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray13), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList14);
        org.junit.Assert.assertNotNull(objArray15);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray15), "[0hi!100, -1]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray15), "[0hi!100, -1]");
        org.junit.Assert.assertNotNull(objList16);
        org.junit.Assert.assertNotNull(objList17);
        org.junit.Assert.assertNotNull(objList18);
    }

    @Test
    public void test231() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test231");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) 1, (int) (short) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test232() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test232");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(100, (int) (byte) -1);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test233() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test233");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(0, (int) (byte) -1);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test234() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test234");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 10, (int) (short) 100);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test235() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test235");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("000hi%21100100100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test236() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test236");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, 100);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test237() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test237");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-12L), (java.lang.Long) 1L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-11L) + "'", long2 == (-11L));
    }

    @Test
    public void test238() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test238");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("00hi%252521100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test239() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test239");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(97, 97);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test240() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test240");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi!");
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "0hi!100");
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi%21");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "0hi!100" + "'", str7, "0hi!100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "00hi!100100" + "'", str9, "00hi!100100");
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "0hi%21100" + "'", str11, "0hi%21100");
    }

    @Test
    public void test241() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test241");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("000000hi!100100100100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test242() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test242");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("000000hi%21100100100100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test243() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test243");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException5);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
    }

    @Test
    public void test244() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test244");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 111L, (java.lang.Long) 0L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 111L + "'", long2 == 111L);
    }

    @Test
    public void test245() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test245");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("000hi%252521100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi%2521100100100" + "'", str1, "000hi%2521100100100");
    }

    @Test
    public void test246() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test246");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "00hi!100100");
        long long9 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        long long10 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList11 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(52, (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "000hi!100100100" + "'", str8, "000hi!100100100");
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 100L + "'", long9 == 100L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
        org.junit.Assert.assertNotNull(longListList11);
    }

    @Test
    public void test247() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test247");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException3);
        java.lang.Exception exception5 = null;
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException10 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException9);
        java.lang.RuntimeException runtimeException11 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException10);
        java.lang.RuntimeException runtimeException12 = org.apache.storm.utils.refactored.two.Utils.OR(runtimeException4, runtimeException10);
        java.lang.RuntimeException runtimeException13 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException12);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
        org.junit.Assert.assertNotNull(runtimeException10);
        org.junit.Assert.assertNotNull(runtimeException11);
        org.junit.Assert.assertNotNull(runtimeException12);
        org.junit.Assert.assertNotNull(runtimeException13);
    }

    @Test
    public void test248() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test248");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.lang.String str10 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%21100");
        java.lang.String str12 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "");
        long long13 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList14 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((-1), (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "00hi%21100100" + "'", str10, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "0100" + "'", str12, "0100");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
        org.junit.Assert.assertNotNull(longListList14);
    }

    @Test
    public void test249() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test249");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) (-12.0d));
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-12.0d) + "'", double1 == (-12.0d));
    }

    @Test
    public void test250() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test250");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%21100");
        java.lang.String str13 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%2521100");
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%2521100");
        java.lang.String str17 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "00hi!100100");
        long long18 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "00hi%21100100" + "'", str11, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "00hi%2521100100" + "'", str13, "00hi%2521100100");
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "00hi%2521100100" + "'", str15, "00hi%2521100100");
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "000hi!100100100" + "'", str17, "000hi!100100100");
        org.junit.Assert.assertTrue("'" + long18 + "' != '" + 100L + "'", long18 == 100L);
    }

    @Test
    public void test251() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test251");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException4);
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException4);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
    }

    @Test
    public void test252() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test252");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.lang.String str10 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%21100");
        java.lang.String str12 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "");
        long long13 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList14 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 1, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str16 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "1");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "00hi%21100100" + "'", str10, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "0100" + "'", str12, "0100");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
        org.junit.Assert.assertNotNull(longListList14);
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "01100" + "'", str16, "01100");
    }

    @Test
    public void test253() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test253");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        long long17 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((-1), (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
        org.junit.Assert.assertNotNull(longListList18);
    }

    @Test
    public void test254() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test254");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("1");
    }

    @Test
    public void test255() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test255");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap12 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (byte) 10);
        java.lang.Object obj13 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Object) objListMap9, (java.lang.Object) (byte) 10);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objListMap9);
        org.junit.Assert.assertNotNull(intMap12);
        org.junit.Assert.assertNotNull(obj13);
        org.junit.Assert.assertEquals(obj13.toString(), "{[1.0]=[-1.0]}");
        org.junit.Assert.assertEquals(java.lang.String.valueOf(obj13), "{[1.0]=[-1.0]}");
        org.junit.Assert.assertEquals(java.util.Objects.toString(obj13), "{[1.0]=[-1.0]}");
    }

    @Test
    public void test256() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test256");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 100L, (java.lang.Long) 111L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 11L + "'", long2 == 11L);
    }

    @Test
    public void test257() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test257");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("00hi%2525252521100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test258() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test258");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-11L), (java.lang.Long) (-101L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 110L + "'", long2 == 110L);
    }

    @Test
    public void test259() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test259");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap10 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap9);
        java.lang.Object[] objArray15 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList16 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray15);
        java.util.List<java.lang.Object> objList17 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray15);
        java.util.List<java.lang.Object> objList18 = org.apache.storm.utils.refactored.two.Utils.get(objMap9, (java.lang.Object) 2.147483647E9d, objList17);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap19 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap9);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objMap9);
        org.junit.Assert.assertNotNull(objListMap10);
        org.junit.Assert.assertNotNull(objArray15);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray15), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray15), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList16);
        org.junit.Assert.assertNotNull(objList17);
        org.junit.Assert.assertNotNull(objList18);
        org.junit.Assert.assertNotNull(objListMap19);
    }

    @Test
    public void test260() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test260");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("000hi%2525252521100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi%25252521100100100" + "'", str1, "000hi%25252521100100100");
    }

    @Test
    public void test261() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test261");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) 10, (int) '#');
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test262() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test262");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 0L, (java.lang.Long) (-111L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-111L) + "'", long2 == (-111L));
    }

    @Test
    public void test263() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test263");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(2147483647, (int) (byte) 100);
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap5 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, (int) (byte) 10);
        java.util.AbstractMap<java.lang.Integer, java.lang.Integer> intMap6 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.AbstractMap<java.lang.Integer, java.lang.Integer>) intMap2, (java.util.AbstractMap<java.lang.Integer, java.lang.Integer>) intMap5);
        org.junit.Assert.assertNotNull(intMap2);
        org.junit.Assert.assertNotNull(intMap5);
        org.junit.Assert.assertNotNull(intMap6);
    }

    @Test
    public void test264() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test264");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("00hi%25252521100100");
    }

    @Test
    public void test265() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test265");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) -1, (-1));
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test266() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test266");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("000hi%25252521100100100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test267() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test267");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) (-102L));
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-102.0d) + "'", double1 == (-102.0d));
    }

    @Test
    public void test268() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test268");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 110L, (java.lang.Long) (-12L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-102L) + "'", long2 == (-102L));
    }

    @Test
    public void test269() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test269");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi!");
        long long8 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str10 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00000hi!100100100100100");
        java.lang.String str12 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00hi%25252521100100");
        long long13 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "0hi!100" + "'", str7, "0hi!100");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "000000hi!100100100100100100" + "'", str10, "000000hi!100100100100100100");
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "000hi%25252521100100100" + "'", str12, "000hi%25252521100100100");
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
    }

    @Test
    public void test270() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test270");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(10, (int) '#');
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test271() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test271");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        long long9 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        long long10 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList11 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(10, (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 100L + "'", long9 == 100L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
        org.junit.Assert.assertNotNull(longListList11);
    }

    @Test
    public void test272() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test272");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("000000hi!100100100100100100");
    }

    @Test
    public void test273() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test273");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap10 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap11 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap12 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap13 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap14 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap13);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objMap9);
        org.junit.Assert.assertNotNull(objMap10);
        org.junit.Assert.assertNotNull(objMap11);
        org.junit.Assert.assertNotNull(objMap12);
        org.junit.Assert.assertNotNull(objMap13);
        org.junit.Assert.assertNotNull(objListMap14);
    }

    @Test
    public void test274() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test274");
        java.lang.String[] strArray3 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList4 = new java.util.ArrayList<java.lang.String>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList4, strArray3);
        java.util.List<java.lang.String> strList6 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList4);
        java.util.List<java.lang.String> strList7 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList6);
        java.util.List<java.lang.String> strList8 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList7);
        java.util.List<java.lang.String> strList9 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList8);
        java.util.List<java.lang.String> strList10 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList9);
        org.junit.Assert.assertNotNull(strArray3);
        org.junit.Assert.assertArrayEquals(strArray3, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertNotNull(strList6);
        org.junit.Assert.assertNotNull(strList7);
        org.junit.Assert.assertNotNull(strList8);
        org.junit.Assert.assertNotNull(strList9);
        org.junit.Assert.assertNotNull(strList10);
    }

    @Test
    public void test275() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test275");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("000hi%25252521100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test276() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test276");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 110L, (java.lang.Long) 0L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 110L + "'", long2 == 110L);
    }

    @Test
    public void test277() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test277");
        java.lang.Comparable<java.lang.String> strComparable2 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Comparable<java.lang.String>) "00000hi!100100100100100", (java.lang.Comparable<java.lang.String>) "00hi%252521100100");
        org.junit.Assert.assertEquals("'" + strComparable2 + "' != '" + "00000hi!100100100100100" + "'", strComparable2, "00000hi!100100100100100");
    }

    @Test
    public void test278() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test278");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("000hi!100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi%21100100100" + "'", str1, "000hi%21100100100");
    }

    @Test
    public void test279() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test279");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("000hi%21100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test280() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test280");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.Long[] longArray10 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList11 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean12 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList11, longArray10);
        long long13 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList11);
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList11, "hi!");
        long long16 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList11);
        java.util.RandomAccess randomAccess17 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList5, (java.util.RandomAccess) longList11);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(2147483647, (java.util.Collection<java.lang.Long>) longList11);
        java.util.List<java.util.List<java.lang.Long>> longListList19 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(1, (java.util.Collection<java.lang.Long>) longList11);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertNotNull(longArray10);
        org.junit.Assert.assertArrayEquals(longArray10, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean12 + "' != '" + true + "'", boolean12 == true);
        org.junit.Assert.assertTrue("'" + long13 + "' != '" + 100L + "'", long13 == 100L);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "0hi!100" + "'", str15, "0hi!100");
        org.junit.Assert.assertTrue("'" + long16 + "' != '" + 100L + "'", long16 == 100L);
        org.junit.Assert.assertNotNull(randomAccess17);
        org.junit.Assert.assertNotNull(longListList18);
        org.junit.Assert.assertNotNull(longListList19);
    }

    @Test
    public void test281() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test281");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isLocalhostAddress("0100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test282() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test282");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 100, 97);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test283() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test283");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("0000hi!100100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0000hi%21100100100100" + "'", str1, "0000hi%21100100100100");
    }

    @Test
    public void test284() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test284");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("01100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "01100" + "'", str1, "01100");
    }

    @Test
    public void test285() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test285");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) 10, (int) (byte) -1);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test286() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test286");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("00hi%2525252521100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%252525252521100100" + "'", str1, "00hi%252525252521100100");
    }

    @Test
    public void test287() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test287");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("000hi%25252521100100100");
    }

    @Test
    public void test288() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test288");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-101L), (java.lang.Long) (-12L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 111L + "'", long2 == 111L);
    }

    @Test
    public void test289() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test289");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("00hi%21100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test290() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test290");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 1L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.util.List<java.util.List<java.lang.Long>> longListList7 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 0, (java.util.Collection<java.lang.Long>) longList3);
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 1L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 1L + "'", long5 == 1L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 1L + "'", long6 == 1L);
        org.junit.Assert.assertNotNull(longListList7);
    }

    @Test
    public void test291() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test291");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException3);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException3);
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException3);
        java.lang.Exception exception7 = null;
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception7);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException8);
        java.lang.Exception exception10 = null;
        java.lang.RuntimeException runtimeException11 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception10);
        java.lang.RuntimeException runtimeException12 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException11);
        java.lang.RuntimeException runtimeException13 = org.apache.storm.utils.refactored.two.Utils.OR(runtimeException8, runtimeException12);
        java.lang.RuntimeException runtimeException14 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException13);
        java.lang.Throwable throwable15 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Throwable) runtimeException3, (java.lang.Throwable) runtimeException14);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
        org.junit.Assert.assertNotNull(runtimeException11);
        org.junit.Assert.assertNotNull(runtimeException12);
        org.junit.Assert.assertNotNull(runtimeException13);
        org.junit.Assert.assertNotNull(runtimeException14);
        org.junit.Assert.assertNotNull(throwable15);
        org.junit.Assert.assertNull("throwable15.getLocalizedMessage() == null", throwable15.getLocalizedMessage());
        org.junit.Assert.assertNull("throwable15.getMessage() == null", throwable15.getMessage());
        org.junit.Assert.assertEquals(throwable15.toString(), "java.lang.RuntimeException");
    }

    @Test
    public void test292() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test292");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.Exception exception2 = null;
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception2);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception2);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception2);
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException5);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException5);
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.OR(runtimeException1, runtimeException5);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
    }

    @Test
    public void test293() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test293");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException3);
        java.lang.Exception exception5 = null;
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException10 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException9);
        java.lang.RuntimeException runtimeException11 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException10);
        java.lang.RuntimeException runtimeException12 = org.apache.storm.utils.refactored.two.Utils.OR(runtimeException4, runtimeException10);
        java.lang.RuntimeException runtimeException13 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException4);
        java.lang.RuntimeException runtimeException14 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException13);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
        org.junit.Assert.assertNotNull(runtimeException10);
        org.junit.Assert.assertNotNull(runtimeException11);
        org.junit.Assert.assertNotNull(runtimeException12);
        org.junit.Assert.assertNotNull(runtimeException13);
        org.junit.Assert.assertNotNull(runtimeException14);
    }

    @Test
    public void test294() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test294");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("0000hi%21100100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0000hi!100100100100" + "'", str1, "0000hi!100100100100");
    }

    @Test
    public void test295() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test295");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("01100");
    }

    @Test
    public void test296() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test296");
        double double1 = org.apache.storm.utils.refactored.two.Utils.nullToZero((java.lang.Double) 101.0d);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 101.0d + "'", double1 == 101.0d);
    }

    @Test
    public void test297() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test297");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "00hi!100100");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 1, (java.util.Collection<java.lang.Long>) longList4);
        long long10 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "000hi!100100100" + "'", str8, "000hi!100100100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
    }

    @Test
    public void test298() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test298");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(100, (int) (short) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test299() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test299");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("00hi%252521100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%2521100100" + "'", str1, "00hi%2521100100");
    }

    @Test
    public void test300() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test300");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("000hi%252521100100100");
    }

    @Test
    public void test301() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test301");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("000hi%25252521100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test302() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test302");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi!");
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "0hi%21100");
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "");
        java.lang.String str13 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00hi%252521100100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "0hi!100" + "'", str7, "0hi!100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "00hi%21100100" + "'", str9, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "0100" + "'", str11, "0100");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "000hi%252521100100100" + "'", str13, "000hi%252521100100100");
    }

    @Test
    public void test303() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test303");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
    }

    @Test
    public void test304() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test304");
        java.lang.Object[] objArray1 = new java.lang.Object[] { "0hi!100" };
        java.util.List<java.lang.Object> objList2 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.lang.Object[] objArray5 = new java.lang.Object[] { "0hi!100", (-1) };
        java.lang.Object[] objArray6 = org.apache.storm.utils.refactored.two.Utils.OR(objArray1, objArray5);
        java.util.List<java.lang.Object> objList7 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray5);
        java.util.List<java.lang.Object> objList8 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray5);
        java.util.List<java.lang.Object> objList9 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray5);
        org.junit.Assert.assertNotNull(objArray1);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray1), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray1), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList2);
        org.junit.Assert.assertNotNull(objArray5);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertNotNull(objArray6);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray6), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray6), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList7);
        org.junit.Assert.assertNotNull(objList8);
        org.junit.Assert.assertNotNull(objList9);
    }

    @Test
    public void test305() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test305");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("hi%2521");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "hi%252521" + "'", str1, "hi%252521");
    }

    @Test
    public void test306() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test306");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) '4', 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test307() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test307");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException1);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException2);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException2);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
    }

    @Test
    public void test308() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test308");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi!");
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "0hi%21100");
        long long10 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        long long11 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str13 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00000hi!100100100100100");
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "01100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "0hi!100" + "'", str7, "0hi!100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "00hi%21100100" + "'", str9, "00hi%21100100");
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 100L + "'", long10 == 100L);
        org.junit.Assert.assertTrue("'" + long11 + "' != '" + 100L + "'", long11 == 100L);
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "000000hi!100100100100100100" + "'", str13, "000000hi!100100100100100100");
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "001100100" + "'", str15, "001100100");
    }

    @Test
    public void test309() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test309");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.Long[] longArray9 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList10 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean11 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList10, longArray9);
        long long12 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList10, "hi!");
        long long15 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList10);
        java.util.RandomAccess randomAccess16 = org.apache.storm.utils.refactored.two.Utils.OR((java.util.RandomAccess) longList4, (java.util.RandomAccess) longList10);
        long long17 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList18 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertNotNull(longArray9);
        org.junit.Assert.assertArrayEquals(longArray9, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean11 + "' != '" + true + "'", boolean11 == true);
        org.junit.Assert.assertTrue("'" + long12 + "' != '" + 100L + "'", long12 == 100L);
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "0hi!100" + "'", str14, "0hi!100");
        org.junit.Assert.assertTrue("'" + long15 + "' != '" + 100L + "'", long15 == 100L);
        org.junit.Assert.assertNotNull(randomAccess16);
        org.junit.Assert.assertTrue("'" + long17 + "' != '" + 100L + "'", long17 == 100L);
        org.junit.Assert.assertNotNull(longListList18);
    }

    @Test
    public void test310() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test310");
        java.lang.String[] strArray6 = new java.lang.String[] { "00hi%252521100100", "0000hi!100100100100", "00hi%2521100100", "hi%2521", "00hi%2521100100", "00hi!100100" };
        java.util.ArrayList<java.lang.String> strList7 = new java.util.ArrayList<java.lang.String>();
        boolean boolean8 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList7, strArray6);
        java.util.List<java.lang.String> strList9 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList7);
        java.util.List<java.lang.String> strList10 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList7);
        java.util.List<java.lang.String> strList11 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList7);
        java.util.List<java.lang.String> strList12 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList11);
        java.util.List<java.lang.String> strList13 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList11);
        org.junit.Assert.assertNotNull(strArray6);
        org.junit.Assert.assertArrayEquals(strArray6, new java.lang.String[] { "00hi%252521100100", "0000hi!100100100100", "00hi%2521100100", "hi%2521", "00hi%2521100100", "00hi!100100" });
        org.junit.Assert.assertTrue("'" + boolean8 + "' != '" + true + "'", boolean8 == true);
        org.junit.Assert.assertNotNull(strList9);
        org.junit.Assert.assertNotNull(strList10);
        org.junit.Assert.assertNotNull(strList11);
        org.junit.Assert.assertNotNull(strList12);
        org.junit.Assert.assertNotNull(strList13);
    }

    @Test
    public void test311() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test311");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-111L), (java.lang.Long) 11L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-102L) + "'", long2 == (-102L));
    }

    @Test
    public void test312() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test312");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap10 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap11 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap12 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap13 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap14 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objListMap9);
        org.junit.Assert.assertNotNull(objListMap10);
        org.junit.Assert.assertNotNull(objListMap11);
        org.junit.Assert.assertNotNull(objListMap12);
        org.junit.Assert.assertNotNull(objListMap13);
        org.junit.Assert.assertNotNull(objListMap14);
    }

    @Test
    public void test313() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test313");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((-1), (int) (byte) -1);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test314() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test314");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) 1L);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + 1.0d + "'", double1 == 1.0d);
    }

    @Test
    public void test315() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test315");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("000hi%2521100100100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test316() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test316");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("000000hi%21100100100100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test317() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test317");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException4);
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException5);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException6);
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException6);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException6);
        java.lang.RuntimeException runtimeException10 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException9);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
        org.junit.Assert.assertNotNull(runtimeException10);
    }

    @Test
    public void test318() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test318");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap10 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap11 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap12 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap13 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap14 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objMap9);
        org.junit.Assert.assertNotNull(objMap10);
        org.junit.Assert.assertNotNull(objMap11);
        org.junit.Assert.assertNotNull(objMap12);
        org.junit.Assert.assertNotNull(objMap13);
        org.junit.Assert.assertNotNull(objMap14);
    }

    @Test
    public void test319() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test319");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided(2147483647, (int) (short) 1);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test320() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test320");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi!");
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "0hi!100");
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "000hi!100100100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "0hi!100" + "'", str7, "0hi!100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "00hi!100100" + "'", str9, "00hi!100100");
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "0000hi!100100100100" + "'", str11, "0000hi!100100100100");
    }

    @Test
    public void test321() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test321");
        java.lang.String[] strArray3 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList4 = new java.util.ArrayList<java.lang.String>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList4, strArray3);
        java.util.List<java.lang.String> strList6 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList4);
        java.util.List<java.lang.String> strList7 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList4);
        java.util.List<java.lang.String> strList8 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList7);
        java.util.List<java.lang.String> strList9 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList7);
        java.util.List<java.lang.String> strList10 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList9);
        org.junit.Assert.assertNotNull(strArray3);
        org.junit.Assert.assertArrayEquals(strArray3, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertNotNull(strList6);
        org.junit.Assert.assertNotNull(strList7);
        org.junit.Assert.assertNotNull(strList8);
        org.junit.Assert.assertNotNull(strList9);
        org.junit.Assert.assertNotNull(strList10);
    }

    @Test
    public void test322() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test322");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException1);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException2);
        java.lang.Exception exception4 = null;
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception4);
        java.lang.Exception exception6 = null;
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception6);
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception6);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception6);
        java.lang.RuntimeException runtimeException10 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException9);
        java.lang.RuntimeException runtimeException11 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException9);
        java.lang.RuntimeException runtimeException12 = org.apache.storm.utils.refactored.two.Utils.OR(runtimeException5, runtimeException9);
        java.lang.Throwable throwable13 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Throwable) runtimeException3, (java.lang.Throwable) runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException7);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
        org.junit.Assert.assertNotNull(runtimeException10);
        org.junit.Assert.assertNotNull(runtimeException11);
        org.junit.Assert.assertNotNull(runtimeException12);
        org.junit.Assert.assertNotNull(throwable13);
        org.junit.Assert.assertNull("throwable13.getLocalizedMessage() == null", throwable13.getLocalizedMessage());
        org.junit.Assert.assertNull("throwable13.getMessage() == null", throwable13.getMessage());
        org.junit.Assert.assertEquals(throwable13.toString(), "java.lang.RuntimeException");
    }

    @Test
    public void test323() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test323");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList10 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList5);
        java.lang.String str12 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "0hi%21100");
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "01100");
        java.util.List<java.util.List<java.lang.Long>> longListList15 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 10, (java.util.Collection<java.lang.Long>) longList5);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "0hi!100" + "'", str9, "0hi!100");
        org.junit.Assert.assertNotNull(longListList10);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "00hi%21100100" + "'", str12, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "001100100" + "'", str14, "001100100");
        org.junit.Assert.assertNotNull(longListList15);
    }

    @Test
    public void test324() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test324");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.lang.String str10 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi!100");
        java.util.List<java.util.List<java.lang.Long>> longListList11 = org.apache.storm.utils.refactored.two.Utils.partitionFixed(2147483647, (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "00hi!100100" + "'", str10, "00hi!100100");
        org.junit.Assert.assertNotNull(longListList11);
    }

    @Test
    public void test325() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test325");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) (-1L), (java.lang.Long) 10L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-11L) + "'", long2 == (-11L));
    }

    @Test
    public void test326() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test326");
        java.lang.Object[] objArray1 = new java.lang.Object[] { "0hi!100" };
        java.util.List<java.lang.Object> objList2 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.lang.Object[] objArray5 = new java.lang.Object[] { "0hi!100", (-1) };
        java.lang.Object[] objArray6 = org.apache.storm.utils.refactored.two.Utils.OR(objArray1, objArray5);
        java.util.List<java.lang.Object> objList7 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.util.List<java.lang.Object> objList8 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.util.List<java.lang.Object> objList9 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        java.util.List<java.lang.Object> objList10 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray1);
        org.junit.Assert.assertNotNull(objArray1);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray1), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray1), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList2);
        org.junit.Assert.assertNotNull(objArray5);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray5), "[0hi!100, -1]");
        org.junit.Assert.assertNotNull(objArray6);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray6), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray6), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList7);
        org.junit.Assert.assertNotNull(objList8);
        org.junit.Assert.assertNotNull(objList9);
        org.junit.Assert.assertNotNull(objList10);
    }

    @Test
    public void test327() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test327");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("001100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "001100100" + "'", str1, "001100100");
    }

    @Test
    public void test328() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test328");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("0hi!100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0hi!100" + "'", str1, "0hi!100");
    }

    @Test
    public void test329() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test329");
        java.lang.Object[] objArray3 = new java.lang.Object[] { 1.0f, (-1.0f), 10.0d };
        java.util.List<java.lang.Object> objList4 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.List<java.lang.Object> objList5 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray3);
        java.util.ArrayList<java.util.List<java.lang.Object>> objListList6 = new java.util.ArrayList<java.util.List<java.lang.Object>>();
        boolean boolean7 = objListList6.add(objList5);
        java.util.Map<java.lang.Object, java.util.List<java.lang.Object>> objMap8 = org.apache.storm.utils.refactored.two.Utils.reverseMap((java.util.List<java.util.List<java.lang.Object>>) objListList6);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap9 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        java.lang.Object[] objArray12 = new java.lang.Object[] { "0hi!100" };
        java.util.List<java.lang.Object> objList13 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray12);
        java.util.List<java.lang.Object> objList14 = org.apache.storm.utils.refactored.two.Utils.tuple(objArray12);
        java.util.List<java.lang.Object> objList15 = org.apache.storm.utils.refactored.two.Utils.get(objMap8, (java.lang.Object) 1, objList14);
        java.util.HashMap<java.util.List<java.lang.Object>, java.util.List<java.lang.Object>> objListMap16 = org.apache.storm.utils.refactored.two.Utils.reverseMap(objMap8);
        org.junit.Assert.assertNotNull(objArray3);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray3), "[1.0, -1.0, 10.0]");
        org.junit.Assert.assertNotNull(objList4);
        org.junit.Assert.assertNotNull(objList5);
        org.junit.Assert.assertTrue("'" + boolean7 + "' != '" + true + "'", boolean7 == true);
        org.junit.Assert.assertNotNull(objMap8);
        org.junit.Assert.assertNotNull(objListMap9);
        org.junit.Assert.assertNotNull(objArray12);
        org.junit.Assert.assertEquals(java.util.Arrays.deepToString(objArray12), "[0hi!100]");
        org.junit.Assert.assertEquals(java.util.Arrays.toString(objArray12), "[0hi!100]");
        org.junit.Assert.assertNotNull(objList13);
        org.junit.Assert.assertNotNull(objList14);
        org.junit.Assert.assertNotNull(objList15);
        org.junit.Assert.assertNotNull(objListMap16);
    }

    @Test
    public void test330() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test330");
        java.lang.Long[] longArray4 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList5 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean6 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList5, longArray4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList5);
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList10 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList5);
        java.lang.String str12 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "0hi%21100");
        java.lang.String str14 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "0hi%2521100");
        java.lang.String str16 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList5, "0hi%2521100");
        java.util.List<java.util.List<java.lang.Long>> longListList17 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) -1, (java.util.Collection<java.lang.Long>) longList5);
        org.junit.Assert.assertNotNull(longArray4);
        org.junit.Assert.assertArrayEquals(longArray4, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean6 + "' != '" + true + "'", boolean6 == true);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 100L + "'", long7 == 100L);
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "0hi!100" + "'", str9, "0hi!100");
        org.junit.Assert.assertNotNull(longListList10);
        org.junit.Assert.assertEquals("'" + str12 + "' != '" + "00hi%21100100" + "'", str12, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str14 + "' != '" + "00hi%2521100100" + "'", str14, "00hi%2521100100");
        org.junit.Assert.assertEquals("'" + str16 + "' != '" + "00hi%2521100100" + "'", str16, "00hi%2521100100");
        org.junit.Assert.assertNotNull(longListList17);
    }

    @Test
    public void test331() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test331");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00hi!100100");
        long long8 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str10 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "0hi!100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "000hi!100100100" + "'", str7, "000hi!100100100");
        org.junit.Assert.assertTrue("'" + long8 + "' != '" + 100L + "'", long8 == 100L);
        org.junit.Assert.assertEquals("'" + str10 + "' != '" + "00hi!100100" + "'", str10, "00hi!100100");
    }

    @Test
    public void test332() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test332");
        java.lang.String[] strArray6 = new java.lang.String[] { "00hi%252521100100", "0000hi!100100100100", "00hi%2521100100", "hi%2521", "00hi%2521100100", "00hi!100100" };
        java.util.ArrayList<java.lang.String> strList7 = new java.util.ArrayList<java.lang.String>();
        boolean boolean8 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList7, strArray6);
        java.util.List<java.lang.String> strList9 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList7);
        java.util.List<java.lang.String> strList10 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList7);
        java.util.List<java.lang.String> strList11 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList7);
        java.util.List<java.lang.String> strList12 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList11);
        java.util.List<java.lang.String> strList13 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList12);
        org.junit.Assert.assertNotNull(strArray6);
        org.junit.Assert.assertArrayEquals(strArray6, new java.lang.String[] { "00hi%252521100100", "0000hi!100100100100", "00hi%2521100100", "hi%2521", "00hi%2521100100", "00hi!100100" });
        org.junit.Assert.assertTrue("'" + boolean8 + "' != '" + true + "'", boolean8 == true);
        org.junit.Assert.assertNotNull(strList9);
        org.junit.Assert.assertNotNull(strList10);
        org.junit.Assert.assertNotNull(strList11);
        org.junit.Assert.assertNotNull(strList12);
        org.junit.Assert.assertNotNull(strList13);
    }

    @Test
    public void test333() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test333");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException3);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException4);
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
        org.junit.Assert.assertNotNull(runtimeException6);
    }

    @Test
    public void test334() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test334");
        // The following exception was thrown during execution in test generation
        try {
            java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 1, (int) (short) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.ArithmeticException; message: / by zero");
        } catch (java.lang.ArithmeticException e) {
            // Expected exception.
        }
    }

    @Test
    public void test335() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test335");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 101L, (java.lang.Long) 11L);
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + 110L + "'", long2 == 110L);
    }

    @Test
    public void test336() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test336");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("hi%2521");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.String cannot be cast to class java.util.Map (java.lang.String and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test337() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test337");
        java.lang.String[] strArray3 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList4 = new java.util.ArrayList<java.lang.String>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList4, strArray3);
        java.util.List<java.lang.String> strList6 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList4);
        java.util.List<java.lang.String> strList7 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList6);
        java.util.List<java.lang.String> strList8 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList6);
        java.util.List<java.lang.String> strList9 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList8);
        org.junit.Assert.assertNotNull(strArray3);
        org.junit.Assert.assertArrayEquals(strArray3, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertNotNull(strList6);
        org.junit.Assert.assertNotNull(strList7);
        org.junit.Assert.assertNotNull(strList8);
        org.junit.Assert.assertNotNull(strList9);
    }

    @Test
    public void test338() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test338");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%21100");
        java.lang.String str13 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "00hi%2525252521100100");
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%21100");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "00hi%21100100" + "'", str11, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "000hi%2525252521100100100" + "'", str13, "000hi%2525252521100100100");
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "00hi%21100100" + "'", str15, "00hi%21100100");
    }

    @Test
    public void test339() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test339");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("00hi%252525252521100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "00hi%25252525252521100100" + "'", str1, "00hi%25252525252521100100");
    }

    @Test
    public void test340() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test340");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("0000hi%21100100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "0000hi%2521100100100100" + "'", str1, "0000hi%2521100100100100");
    }

    @Test
    public void test341() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test341");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("000hi%252521100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi%25252521100100100" + "'", str1, "000hi%25252521100100100");
    }

    @Test
    public void test342() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test342");
        java.lang.Long[] longArray2 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList3 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean4 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList3, longArray2);
        long long5 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList3);
        java.lang.String str7 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "hi!");
        java.lang.String str9 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "0hi%21100");
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList3, "00hi%2525252521100100");
        org.junit.Assert.assertNotNull(longArray2);
        org.junit.Assert.assertArrayEquals(longArray2, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean4 + "' != '" + true + "'", boolean4 == true);
        org.junit.Assert.assertTrue("'" + long5 + "' != '" + 100L + "'", long5 == 100L);
        org.junit.Assert.assertEquals("'" + str7 + "' != '" + "0hi!100" + "'", str7, "0hi!100");
        org.junit.Assert.assertEquals("'" + str9 + "' != '" + "00hi%21100100" + "'", str9, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "000hi%2525252521100100100" + "'", str11, "000hi%2525252521100100100");
    }

    @Test
    public void test343() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test343");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("1");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "1" + "'", str1, "1");
    }

    @Test
    public void test344() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test344");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException3);
        java.lang.Exception exception5 = null;
        java.lang.RuntimeException runtimeException6 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException7 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException8 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException9 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception5);
        java.lang.RuntimeException runtimeException10 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException9);
        java.lang.RuntimeException runtimeException11 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException10);
        java.lang.RuntimeException runtimeException12 = org.apache.storm.utils.refactored.two.Utils.OR(runtimeException4, runtimeException10);
        java.lang.RuntimeException runtimeException13 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException4);
        java.lang.Exception exception14 = null;
        java.lang.RuntimeException runtimeException15 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception14);
        java.lang.Throwable throwable16 = org.apache.storm.utils.refactored.two.Utils.OR((java.lang.Throwable) runtimeException4, (java.lang.Throwable) runtimeException15);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException6);
        org.junit.Assert.assertNotNull(runtimeException7);
        org.junit.Assert.assertNotNull(runtimeException8);
        org.junit.Assert.assertNotNull(runtimeException9);
        org.junit.Assert.assertNotNull(runtimeException10);
        org.junit.Assert.assertNotNull(runtimeException11);
        org.junit.Assert.assertNotNull(runtimeException12);
        org.junit.Assert.assertNotNull(runtimeException13);
        org.junit.Assert.assertNotNull(runtimeException15);
        org.junit.Assert.assertNotNull(throwable16);
        org.junit.Assert.assertNull("throwable16.getLocalizedMessage() == null", throwable16.getLocalizedMessage());
        org.junit.Assert.assertNull("throwable16.getMessage() == null", throwable16.getMessage());
        org.junit.Assert.assertEquals(throwable16.toString(), "java.lang.RuntimeException");
    }

    @Test
    public void test345() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test345");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlDecodeUtf8("000hi%25252521100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi%252521100100100" + "'", str1, "000hi%252521100100100");
    }

    @Test
    public void test346() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test346");
        java.lang.Exception exception0 = null;
        java.lang.RuntimeException runtimeException1 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime(exception0);
        java.lang.RuntimeException runtimeException2 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException1);
        java.lang.RuntimeException runtimeException3 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException2);
        java.lang.RuntimeException runtimeException4 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException2);
        java.lang.RuntimeException runtimeException5 = org.apache.storm.utils.refactored.two.Utils.wrapInRuntime((java.lang.Exception) runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException1);
        org.junit.Assert.assertNotNull(runtimeException2);
        org.junit.Assert.assertNotNull(runtimeException3);
        org.junit.Assert.assertNotNull(runtimeException4);
        org.junit.Assert.assertNotNull(runtimeException5);
    }

    @Test
    public void test347() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test347");
        double double1 = org.apache.storm.utils.refactored.two.Utils.zeroIfNaNOrInf((double) (byte) -1);
        org.junit.Assert.assertTrue("'" + double1 + "' != '" + (-1.0d) + "'", double1 == (-1.0d));
    }

    @Test
    public void test348() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test348");
        // The following exception was thrown during execution in test generation
        try {
            java.util.Map<java.lang.String, java.lang.Object> strMap1 = org.apache.storm.utils.refactored.two.Utils.parseJson("0100");
            org.junit.Assert.fail("Expected exception of type java.lang.ClassCastException; message: class java.lang.Integer cannot be cast to class java.util.Map (java.lang.Integer and java.util.Map are in module java.base of loader 'bootstrap')");
        } catch (java.lang.ClassCastException e) {
            // Expected exception.
        }
    }

    @Test
    public void test349() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test349");
        java.lang.String str1 = org.apache.storm.utils.refactored.two.Utils.urlEncodeUtf8("000hi%2525252521100100100");
        org.junit.Assert.assertEquals("'" + str1 + "' != '" + "000hi%252525252521100100100" + "'", str1, "000hi%252525252521100100100");
    }

    @Test
    public void test350() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test350");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) -1, (int) (byte) 100);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test351() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test351");
        org.apache.storm.utils.refactored.two.Utils.validateTopologyName("hi%252521");
    }

    @Test
    public void test352() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test352");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 1L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        long long7 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList8 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) -1, (java.util.Collection<java.lang.Long>) longList4);
        long long9 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        long long10 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.util.List<java.util.List<java.lang.Long>> longListList11 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (byte) 100, (java.util.Collection<java.lang.Long>) longList4);
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 1L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 1L + "'", long6 == 1L);
        org.junit.Assert.assertTrue("'" + long7 + "' != '" + 1L + "'", long7 == 1L);
        org.junit.Assert.assertNotNull(longListList8);
        org.junit.Assert.assertTrue("'" + long9 + "' != '" + 1L + "'", long9 == 1L);
        org.junit.Assert.assertTrue("'" + long10 + "' != '" + 1L + "'", long10 == 1L);
        org.junit.Assert.assertNotNull(longListList11);
    }

    @Test
    public void test353() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test353");
        boolean boolean1 = org.apache.storm.utils.refactored.two.Utils.isValidKey("0000hi%21100100100100");
        org.junit.Assert.assertTrue("'" + boolean1 + "' != '" + false + "'", boolean1 == false);
    }

    @Test
    public void test354() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test354");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%21100");
        java.lang.String str13 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%2521100");
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%2521100");
        java.lang.String str17 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0000hi%21100100100100");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "00hi%21100100" + "'", str11, "00hi%21100100");
        org.junit.Assert.assertEquals("'" + str13 + "' != '" + "00hi%2521100100" + "'", str13, "00hi%2521100100");
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "00hi%2521100100" + "'", str15, "00hi%2521100100");
        org.junit.Assert.assertEquals("'" + str17 + "' != '" + "00000hi%21100100100100100" + "'", str17, "00000hi%21100100100100100");
    }

    @Test
    public void test355() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test355");
        long long2 = org.apache.storm.utils.refactored.two.Utils.bitXor((java.lang.Long) 111L, (java.lang.Long) (-12L));
        org.junit.Assert.assertTrue("'" + long2 + "' != '" + (-101L) + "'", long2 == (-101L));
    }

    @Test
    public void test356() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test356");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (byte) 100, (int) (short) 100);
        org.junit.Assert.assertNotNull(intMap2);
    }

    @Test
    public void test357() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test357");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive(2147483647);
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 2147483647 + "'", int1 == 2147483647);
    }

    @Test
    public void test358() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test358");
        java.lang.Long[] longArray3 = new java.lang.Long[] { 0L, 100L };
        java.util.ArrayList<java.lang.Long> longList4 = new java.util.ArrayList<java.lang.Long>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.Long>) longList4, longArray3);
        long long6 = org.apache.storm.utils.refactored.two.Utils.bitXorVals((java.util.List<java.lang.Long>) longList4);
        java.lang.String str8 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "hi!");
        java.util.List<java.util.List<java.lang.Long>> longListList9 = org.apache.storm.utils.refactored.two.Utils.partitionFixed((int) (short) 10, (java.util.Collection<java.lang.Long>) longList4);
        java.lang.String str11 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "0hi%21100");
        java.io.Serializable serializable13 = org.apache.storm.utils.refactored.two.Utils.OR((java.io.Serializable) longList4, (java.io.Serializable) "000hi!100100100");
        java.lang.String str15 = org.apache.storm.utils.refactored.two.Utils.join((java.lang.Iterable<java.lang.Long>) longList4, "001100100");
        org.junit.Assert.assertNotNull(longArray3);
        org.junit.Assert.assertArrayEquals(longArray3, new java.lang.Long[] { 0L, 100L });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertTrue("'" + long6 + "' != '" + 100L + "'", long6 == 100L);
        org.junit.Assert.assertEquals("'" + str8 + "' != '" + "0hi!100" + "'", str8, "0hi!100");
        org.junit.Assert.assertNotNull(longListList9);
        org.junit.Assert.assertEquals("'" + str11 + "' != '" + "00hi%21100100" + "'", str11, "00hi%21100100");
        org.junit.Assert.assertNotNull(serializable13);
        org.junit.Assert.assertEquals("'" + str15 + "' != '" + "0001100100100" + "'", str15, "0001100100100");
    }

    @Test
    public void test359() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test359");
        int int1 = org.apache.storm.utils.refactored.two.Utils.toPositive((int) '#');
        org.junit.Assert.assertTrue("'" + int1 + "' != '" + 35 + "'", int1 == 35);
    }

    @Test
    public void test360() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test360");
        java.lang.String[] strArray3 = new java.lang.String[] { "", "hi!", "0hi!100" };
        java.util.ArrayList<java.lang.String> strList4 = new java.util.ArrayList<java.lang.String>();
        boolean boolean5 = java.util.Collections.addAll((java.util.Collection<java.lang.String>) strList4, strArray3);
        java.util.List<java.lang.String> strList6 = org.apache.storm.utils.refactored.two.Utils.getRepeat((java.util.List<java.lang.String>) strList4);
        java.util.List<java.lang.String> strList7 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList6);
        java.util.List<java.lang.String> strList8 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList6);
        java.util.List<java.lang.String> strList9 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList6);
        java.util.List<java.lang.String> strList10 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList6);
        java.util.List<java.lang.String> strList11 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList10);
        java.util.List<java.lang.String> strList12 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList11);
        java.util.List<java.lang.String> strList13 = org.apache.storm.utils.refactored.two.Utils.getRepeat(strList11);
        org.junit.Assert.assertNotNull(strArray3);
        org.junit.Assert.assertArrayEquals(strArray3, new java.lang.String[] { "", "hi!", "0hi!100" });
        org.junit.Assert.assertTrue("'" + boolean5 + "' != '" + true + "'", boolean5 == true);
        org.junit.Assert.assertNotNull(strList6);
        org.junit.Assert.assertNotNull(strList7);
        org.junit.Assert.assertNotNull(strList8);
        org.junit.Assert.assertNotNull(strList9);
        org.junit.Assert.assertNotNull(strList10);
        org.junit.Assert.assertNotNull(strList11);
        org.junit.Assert.assertNotNull(strList12);
        org.junit.Assert.assertNotNull(strList13);
    }

    @Test
    public void test361() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test361");
        java.util.TreeMap<java.lang.Integer, java.lang.Integer> intMap2 = org.apache.storm.utils.refactored.two.Utils.integerDivided((int) (short) 0, 97);
        org.junit.Assert.assertNotNull(intMap2);
    }
}

