import random 
import os 

def make_shuffle(count, path):
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
        
    
    numbers = list(range(count))
    random.shuffle(numbers)
    with open(path, "w") as f:
        for n in numbers:
            f.write("{}\n".format(n))
             


def get_incremented_number(filename='number.txt'):
    # Step 1: Read the current number from a file
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            current_number = int(file.read().strip())
    else:
        current_number = 0  # Default to 0 if file doesn't exist

    # Step 2: Increment the number
    incremented_number = current_number + 1

    # Step 3: Write the updated number back to the file
    with open(filename, 'w') as file:
        file.write(str(incremented_number))

    # Step 4: Return the updated number
    return incremented_number
