with open("on_premise/src/loaders/test.txt", "r") as f:
    # read one line at a time
    line1 = f.readline()
    # read whole file
    text = f.read()
    print(f"1: {line1}")
    print(f"2: {text}")