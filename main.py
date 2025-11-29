import classes
import sys

def func():
    if int(sys.argv[1]) < 2:
        print("Invalid number of nodes: Exiting program...")
        return -1
    elif int(sys.argv[1]) > 16:
        print("Invalid number of nodes: Exiting program...")
        return -1

    m = classes.main(int(sys.argv[1]))

func()