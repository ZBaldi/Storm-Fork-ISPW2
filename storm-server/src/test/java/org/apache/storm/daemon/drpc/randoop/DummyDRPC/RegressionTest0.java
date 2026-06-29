package org.apache.storm.daemon.drpc.randoop.DummyDRPC;

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
    public void test01() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test01");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (short) -1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test02() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test02");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (short) 10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test03() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test03");
        java.lang.Object obj0 = new java.lang.Object();
        java.lang.Class<?> wildcardClass1 = obj0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass1);
    }

    @Test
    public void test04() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test04");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, 0L);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test05() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test05");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (short) 1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test06() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test06");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, 100L);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test07() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test07");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) ' ');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test08() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test08");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, 10L);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test09() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test09");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, 0L);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test10() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test10");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) 'a');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test11() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test11");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (-1));
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test12() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test12");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (short) 10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test13() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test13");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) '4');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test14() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test14");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (-1));
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test15() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test15");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, 100L);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test16() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test16");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (short) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test17() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test17");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) ' ');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test18() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test18");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, 1L);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test19() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test19");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (byte) 100);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test20() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test20");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (byte) -1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test21() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test21");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) '4');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test22() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test22");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, 10L);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test23() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test23");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) 100);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test24() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test24");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (short) 100);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test25() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test25");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) 1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test26() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test26");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) 'a');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test27() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test27");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test28() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test28");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (byte) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test29() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test29");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (short) 100);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test30() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test30");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) '#');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test31() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test31");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) 100);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test32() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test32");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (-1L));
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test33() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test33");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) 10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test34() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test34");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (short) 1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test35() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test35");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (byte) 10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test36() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test36");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (byte) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test37() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test37");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (byte) 1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test38() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test38");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (short) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test39() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test39");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) 0);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test40() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test40");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) 1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test41() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test41");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (short) -1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test42() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test42");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (-1L));
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test43() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test43");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) 10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test44() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test44");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, 1L);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test45() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test45");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) '#');
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test46() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test46");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (byte) -1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test47() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test47");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (byte) 100);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test48() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test48");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.OkDummyIAuthorizer okDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, okDummyIAuthorizer1, (long) (byte) 1);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test49() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test49");
        org.apache.storm.daemon.drpc.DummyStormMetricRegistry dummyStormMetricRegistry0 = null;
        org.apache.storm.daemon.drpc.KoDummyIAuthorizer koDummyIAuthorizer1 = null;
        // The following exception was thrown during execution in test generation
        try {
            org.apache.storm.daemon.drpc.DummyDRPC dummyDRPC3 = new org.apache.storm.daemon.drpc.DummyDRPC(dummyStormMetricRegistry0, koDummyIAuthorizer1, (long) (byte) 10);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.daemon.drpc.DummyStormMetricRegistry.registerMeter(String)\" because \"metricsRegistry\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }
}

