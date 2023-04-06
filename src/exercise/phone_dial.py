# On a phone dial (look at the one on your smartphone) we want to known how many phone numbers 
# we can dial with 7 digits assuming the first digit is "2" and assuming we jump from one digit 
# to the next using the chess knight move (L shape).
# For example:
# from "2" we can go to "7" or "9"from "3" we can go to "4" or "8"
# The list of all moves is given to you as this dictionary.
# Js = {0:[4,6], 1: [8, 6], 2: [7, 9], 3: [4, 8], 4:[3, 9, 0], 5:[], .... to complete as part of test ...}
# Using Js and a recursive Python function solve the problem.

# 1 2 3
# 4 5 6
# 7 8 9
# * 0 #

# 7 digits => how many phone numbers 
# the first digit is "2"

js = { 0:[4,6], 1: [8, 6], 2: [7, 9], 3: [4, 8], 4:[3, 9, 0], 5:[] }

def dial(digit, level) -> int:
    if level == 1:
        return 1
    
    count = 0
    for next_digit in js[digit]:
        count += dial(next_digit, level - 1)
    return count



result = dial(2, 7)
print(result)