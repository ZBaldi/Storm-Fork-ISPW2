package org.apache.storm.utils_1.manual;

import org.apache.storm.utils.IPredicate;
import org.apache.storm.utils.refactored.one.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

/** Test class which runs unit test on Utils class */
@RunWith(MockitoJUnitRunner.class)
public class UtilsTest {

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

}
