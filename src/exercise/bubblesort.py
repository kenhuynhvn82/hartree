

def sort(nums):
    n = len(nums)
    if n <= 1:
        return nums
    
    for i in range(n-1):
        for j in range(0, n - i - 1):
            if nums[j] > nums[j+1]:
                swap(nums, j, j+1)
    
    return nums

def swap(nums, i, j):
    tmp = nums[i]
    nums[i] = nums[j]
    nums[j] = tmp
    
    
