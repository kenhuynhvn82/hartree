from exercise.bubblesort import sort

def test_sort():
    nums = [5,3,4,2]
    result = sort(nums)
    assert result == [2,3,4,5]
