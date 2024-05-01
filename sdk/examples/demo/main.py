from beta9 import function


@function(cpu=8, gpu="A100-40")
def square(i: int):
    return i**2


def main():
    numbers = list(range(10))
    squared = []

    # Run a remote container per number that needs to be squared
    for result in square.map(numbers):
        squared.append(result)

    print("numbers:", numbers)
    print("squared:", sorted(squared))


if __name__ == "__main__":
    main()
