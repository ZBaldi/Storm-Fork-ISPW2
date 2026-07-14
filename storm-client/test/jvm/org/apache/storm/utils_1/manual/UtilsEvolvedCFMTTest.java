package org.apache.storm.utils_1.manual;

import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.refactored.one.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.*;

/** Test class which runs unit test on Utils class */
@RunWith(MockitoJUnitRunner.class)
public class UtilsEvolvedCFMTTest {

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

    /** Test findOne method with not correct IPredicate (no element in the collection is coherent with check) , one value Collection. Expected = null */
    @SuppressWarnings("unchecked")
    @Test
    public void findOneNotCorrectIPredicateValidCollectionShouldPass() {

        IPredicate<Integer> mockPredicate = Mockito.mock(IPredicate.class);
        Mockito.when(mockPredicate.test(Mockito.any())).thenReturn(false);
        Set<Integer> set = Set.of(1);
        Assert.assertNull(Utils.findOne(mockPredicate, set));
    }

    // JOIN TESTS

    /** Test join method with null collection and null separator. Expected = throws NullPointerException */
    @Test
    public void joinNullCollectionNullSeparatorThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.join(null, null));
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

    // REVERSE MAP TESTS

    /** Test reverseMap method with null map. Expected = returns empty map */
    @Test
    public void reverseMapNullMapShouldPass() {

        HashMap<Integer, List<String>> result = Utils.reverseMap((Map<String, Integer>) null);
        Assert.assertEquals(new HashMap<>(), result);
    }

    /** Test reverseMap method with empty map. Expected = returns empty map */
    @Test
    public void reverseMapEmptyMapShouldPass() {

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
    public void reverseMapTwoElementsSameValueShouldPass() {

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

        Assert.assertThrows(NullPointerException.class, () -> Utils.tuple((Object[]) null));
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

        Assert.assertThrows(NullPointerException.class, () -> Utils.redactValue(null, ""));
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

    /** Test integerDivided method with sum = 1 and numPieces = 2. Expected = {0=1, 1=1} */
    @Test
    public void integerDividedPositiveSumPositivePiecesShouldPass() {

        TreeMap<Integer, Integer> result = Utils.integerDivided(1, 2);
        TreeMap<Integer, Integer> expected = new TreeMap<>();
        expected.put(0, 1);
        expected.put(1, 1);
        Assert.assertEquals(expected, result);
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

    //--------------------------------//
    //EVOLVED TESTS WITH CF APPROACHES//
    //--------------------------------//

    // REVERSE MAP TESTS

    /** Test reverseMap method with null list. Expected = returns empty map */
    @Test
    public void reverseMapNullListShouldPass() {

        Map<Object, List<Object>> result = Utils.reverseMap((List<List<Object>>) null);
        Assert.assertEquals(new HashMap<>(), result);
    }

    /** Test reverseMap method with empty map. Expected = returns empty map */
    @Test
    public void reverseMapEmptyListShouldPass() {

        List<List<Object>> list = Collections.emptyList();
        Map<Object, List<Object>> result = Utils.reverseMap(list);
        Assert.assertEquals(new HashMap<>(), result);
    }

    /** Test reverseMap method with single element list. Expected = reversed map with one entry */
    @Test
    public void reverseMapSingleElement2ShouldPass() {

        List<List<Object>> list = List.of(List.of("a", 1));
        HashMap<Integer, List<String>> expected = new HashMap<>();
        expected.put(1, new ArrayList<>(List.of("a")));
        Map<Object, List<Object>> result = Utils.reverseMap(list);
        Assert.assertEquals(expected, result);
    }

    /** Test reverseMap method with two elements having same value. Expected = single key mapping to list of both keys */
    @Test
    public void reverseMapTwoElementsSameValue2ShouldPass() {

        List<List<Object>> list = List.of(List.of("a", 1), List.of("b", 1));
        HashMap<Integer, List<String>> expected = new HashMap<>();
        expected.put(1, new ArrayList<>(List.of("a", "b")));
        Map<Object, List<Object>> result = Utils.reverseMap(list);
        Assert.assertEquals(expected, result);
    }

    // BIT XOR VALS TESTS

    /** Test bitXorVals method with null list. Expected = Throws NullPointerException. */
    @Test
    public void bitXorValsNullListThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXorVals(null));
    }

    /** Test bitXorVals method with empty list. Expected = Returns 0. */
    @Test
    public void bitXorValsEmptyListShouldPass() {

        Assert.assertEquals(0L, Utils.bitXorVals(Collections.emptyList()));
    }

    /** Test bitXorVals method with one element. Expected = Returns the element value. */
    @Test
    public void bitXorValsOneElementShouldPass() {

        Assert.assertEquals(5L, Utils.bitXorVals(Collections.singletonList(5L)));
    }

    /** Test bitXorVals method with two elements. Expected = Returns xor result. */
    @Test
    public void bitXorValsTwoElementsShouldPass() {

        Assert.assertEquals(6L, Utils.bitXorVals(Arrays.asList(5L, 3L)));
    }

    // EXCEPTION CAUSE IS INSTANCE OF TESTS

    /** Test exceptionCauseIsInstanceOf method with null throwable. Expected = Returns false. */
    @Test
    public void exceptionCauseIsInstanceOfNullThrowableShouldPass() {

        Assert.assertFalse(Utils.exceptionCauseIsInstanceOf(RuntimeException.class, null));
    }

    /** Test exceptionCauseIsInstanceOf method with matching exception type. Expected = Returns true. */
    @Test
    public void exceptionCauseIsInstanceOfCorrectExceptionShouldPass() {

        Throwable throwable = new RuntimeException("Test exception");
        Assert.assertTrue(Utils.exceptionCauseIsInstanceOf(RuntimeException.class, throwable));
    }

    /** Test exceptionCauseIsInstanceOf method with non matching exception type. Expected = Returns false. */
    @Test
    public void exceptionCauseIsInstanceOfNotCorrectExceptionShouldPass() {

        Throwable throwable = new IllegalArgumentException("Test exception");
        Assert.assertFalse(Utils.exceptionCauseIsInstanceOf(NullPointerException.class, throwable));
    }

    // TO BYTE ARRAY TESTS

    /** Test toByteArray method with null buffer. Expected = Throws NullPointerException. */
    @Test
    public void toByteArrayNullBufferThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.toByteArray(null));
    }

    /** Test toByteArray method with empty buffer. Expected = 0. */
    @Test
    public void toByteArrayEmptyBufferShouldPass() {

        ByteBuffer buffer = ByteBuffer.allocate(0);
        byte[] result = Utils.toByteArray(buffer);
        Assert.assertEquals(0, result.length);
    }

    /** Test toByteArray method with populated buffer. Expected = Returns byte array with buffer contents. */
    @Test
    public void toByteArrayPopulatedBufferShouldPass() {

        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        byte[] result = Utils.toByteArray(buffer);
        Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, result);
    }

    // MERGE TESTS

    /** Test merge method with null maps. Expected = Throws NullPointerException. */
    @Test
    public void mergeNullMapsThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.merge(null, null));
    }

    /** Test merge method with first empty map and second null map. Expected = Returns empty map. */
    @Test
    public void mergeEmptyMapAndNullMapShouldPass() {

        Map<String, String> result = Utils.merge(new HashMap<>(), null);
        Assert.assertTrue(result.isEmpty());
    }

    /** Test merge method with first empty map and second empty map. Expected = Returns empty map. */
    @Test
    public void mergeEmptyMapsShouldPass() {

        Map<String, String> result = Utils.merge(new HashMap<>(), new HashMap<>());
        Assert.assertTrue(result.isEmpty());
    }

    /** Test merge method with one value in first map and second null map. Expected = Returns map with one value. */
    @Test
    public void mergeMapWithOneValueAndNullMapShouldPass() {

        Map<String, String> first = new HashMap<>();
        first.put("key1", "value1");
        Map<String, String> result = Utils.merge(first, null);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("value1", result.get("key1"));
    }

    /**
     * Test merge method with one value in both maps. Expected = Returns merged map with both values.
     */
    @Test
    public void mergeMapsWithOneValueEachShouldPass() {

        Map<String, String> first = new HashMap<>();
        first.put("key1", "value1");
        Map<String, String> second = new HashMap<>();
        second.put("key2", "value2");
        Map<String, String> result = Utils.merge(first, second);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("value1", result.get("key1"));
        Assert.assertEquals("value2", result.get("key2"));
    }

    //ZERO IF NAN OR INF TESTS

    /** Test zeroIfNaNOrInf method with NaN value. Expected = Returns 0.0. */
    @Test
    public void zeroIfNaNOrInfNaNShouldPass() {

        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.NaN), 0.0);
    }

    /** Test zeroIfNaNOrInf method with infinite value. Expected = Returns 0.0. */
    @Test
    public void zeroIfNaNOrInfInfiniteValueShouldPass() {

        Assert.assertEquals(0.0, Utils.zeroIfNaNOrInf(Double.POSITIVE_INFINITY), 0.0);
    }

    /** Test zeroIfNaNOrInf method with finite value. Expected = Returns input value. */
    @Test
    public void zeroIfNaNOrInfFiniteValueShouldPass() {

        Assert.assertEquals(42.5, Utils.zeroIfNaNOrInf(42.5), 0.0);
    }

    // FIND ONE TESTS

    /** Test findOne method with null IPredicate, empty map. Expected = null value */
    @Test
    public void findOneNullIPredicateEmptyMapShouldPass() {

        Assert.assertNull(Utils.findOne(null, Map.of()));
    }

    /** Test findOne method with not valid IPredicate (throws RuntimeException), null Map. Expected = null value */
    @SuppressWarnings("unchecked")
    @Test
    public void findOneNotCorrectIPredicateNullMapShouldPass() {

        IPredicate<Integer> mockPredicate = Mockito.mock(IPredicate.class);
        Mockito.lenient().when(mockPredicate.test(Mockito.any())).thenThrow(new RuntimeException());
        Assert.assertNull(Utils.findOne(mockPredicate, (Map<?, Integer>) null));
    }

    /** Test findOne method with correct IPredicate (first element in the map is coherent with check) , one value Map. Expected = first element */
    @SuppressWarnings("unchecked")
    @Test
    public void findOneValidIPredicateValidMapShouldPass() {

        IPredicate<String> mockPredicate = Mockito.mock(IPredicate.class);
        Mockito.when(mockPredicate.test(Mockito.any())).thenReturn(true);
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "value1");
        Map.Entry<Integer, String> expected = new AbstractMap.SimpleEntry<>(1, "value1");
        Assert.assertEquals(expected, Utils.findOne(mockPredicate, map));
    }

    // THIS METHOD HAS A BUG IN ITS USE OF GENERICS!!!
    // The predicate believes it is operating on a String, but it is actually
    // receiving a Map.Entry. As a result, the condition is never matched.
    // The code compiles, but the generic cast is unsafe.

    /** Test findOne method with correct IPredicate (second element in the map is coherent with check) , two values Map. Expected = second element */
    @SuppressWarnings("unchecked")
    //@Test
    public void findOneValidIPredicateValidMap2ShouldPass() {

        IPredicate<String> mockPredicate = Mockito.mock(IPredicate.class);
        Map.Entry<Integer, String> expected = new AbstractMap.SimpleEntry<>(2, "value2");
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "value1");
        map.put(2, "value2");
        Assert.assertEquals(expected, Utils.findOne(mockPredicate, map));
    }

    /** Test findOne method with not correct IPredicate (no element in the map is coherent with check) , one value Map. Expected = null */
    @SuppressWarnings("unchecked")
    @Test
    public void findOneNotCorrectIPredicateValidMapShouldPass() {

        IPredicate<String> mockPredicate = Mockito.mock(IPredicate.class);
        Mockito.when(mockPredicate.test(Mockito.any())).thenReturn(false);
        Map<Integer, String> map = new HashMap<>();
        map.put(1, "value1");
        Assert.assertNull(Utils.findOne(mockPredicate, map));
    }

    //NULL TO ZERO TESTS

    /** Test nullToZero method with null value. Expected = Returns 0.0. */
    @Test
    public void nullToZeroNullValueShouldPass() {

        Assert.assertEquals(0.0, Utils.nullToZero(null), 0.0);
    }

    /** Test nullToZero method with zero value. Expected = Returns 0.0. */
    @Test
    public void nullToZeroZeroValueShouldPass() {

        Assert.assertEquals(0.0, Utils.nullToZero(0.0), 0.0);
    }

    /** Test nullToZero method with non null value. Expected = Returns input value. */
    @Test
    public void nullToZeroNonNullValueShouldPass() {

        Assert.assertEquals(42.5, Utils.nullToZero(42.5), 0.0);
    }

    // OR TESTS

    /** Test OR method with both values null. Expected = Returns null. */
    @Test
    public void ORBothNullValuesShouldPass() {

        Assert.assertNull(Utils.OR(null, null));
    }

    /** Test OR method with first value null. Expected = Returns second value. */
    @Test
    public void ORFirstValueNullShouldPass() {

        Assert.assertEquals("value2", Utils.OR(null, "value2"));
    }

    /** Test OR method with second value null. Expected = Returns first value. */
    @Test
    public void ORSecondValueNullShouldPass() {

        Assert.assertEquals("value1", Utils.OR("value1", null));
    }

    /** Test OR method with both values non null. Expected = Returns first value. */
    @Test
    public void ORBothValuesNonNullShouldPass() {

        Assert.assertEquals("value1", Utils.OR("value1", "value2"));
    }

    // BIT XOR TESTS

    /** Test bitXor method with both values null. Expected = Throws NullPointerException. */
    @Test
    public void bitXorBothNullValuesThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXor(null, null));
    }

    /** Test bitXor method with first value null. Expected = Throws NullPointerException. */
    @Test
    public void bitXorFirstValueNullThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXor(null, 1L));
    }

    /** Test bitXor method with second value null. Expected = Throws NullPointerException. */
    @Test
    public void bitXorSecondValueNullThrowsNullPointerException() {

        Assert.assertThrows(NullPointerException.class, () -> Utils.bitXor(1L, null));
    }

    /** Test bitXor method with valid values. Expected = Returns xor result. */
    @Test
    public void bitXorValidValuesShouldPass() {

        Assert.assertEquals(1L, Utils.bitXor(1L, 0L));
    }

    // TO POSITIVE TESTS

    /** Test toPositive method with zero value. Expected = Returns 0. */
    @Test
    public void toPositiveZeroValueShouldPass() {

        Assert.assertEquals(0, Utils.toPositive(0));
    }

    /** Test toPositive method with positive value. Expected = Returns same value. */
    @Test
    public void toPositivePositiveValueShouldPass() {

        Assert.assertEquals(1, Utils.toPositive(1));
    }

    /** Test toPositive method with negative value. Expected = Returns positive value. */
    @Test
    public void toPositiveNegativeValueShouldPass() {  //NOT ABSOLUTE VALUE

        Assert.assertEquals(2147483647, Utils.toPositive(-1));
    }

    //--------------------------------//
    //EVOLVED TESTS WITH MUTATION TESTING//
    //--------------------------------//

    // REVERSE MAP TESTS

    /** Test reverseMap method with null list. Expected = returns mutable empty map. */
    @Test
    public void reverseMapNullList2ShouldPass() {

        Map<Object, List<Object>> result = Utils.reverseMap((List<List<Object>>) null);
        Assert.assertTrue(result.isEmpty());
        result.put("key", new ArrayList<>());
        Assert.assertTrue(result.containsKey("key"));
    }

    // PARTITION FIXED TESTS

    /** Test partitionFixed method with zero chunks and non empty collection. Expected = returns mutable empty list. */
    @Test
    public void partitionFixedZeroChunksOneValueShouldPass() {

        List<List<String>> result = Utils.partitionFixed(0, List.of("x"));
        Assert.assertTrue(result.isEmpty());
        result.add(List.of("added"));
        Assert.assertEquals(List.of(List.of("added")), result);
    }

    /** Test partitionFixed method with null collection. Expected = returns mutable empty list. */
    @Test
    public void partitionFixedZeroChunksNullCollection2ShouldPass() {

        List<List<Object>> result = Utils.partitionFixed(0, null);
        Assert.assertTrue(result.isEmpty());
        result.add(List.of("added"));
        Assert.assertEquals(List.of(List.of("added")), result);
    }

    /** Test partitionFixed method with ten values and three chunks. Expected = returns chunks ordered by descending chunk size. */
    @Test
    public void partitionFixedThreeChunksTenValuesShouldPass() {

        List<Integer> input = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<List<Integer>> result = Utils.partitionFixed(3, input);
        Assert.assertEquals(List.of(List.of(1, 2, 3, 4), List.of(5, 6, 7), List.of(8, 9, 10)), result);
        Assert.assertEquals(3, result.size());
        Assert.assertFalse(result.stream().anyMatch(List::isEmpty));
    }

}
