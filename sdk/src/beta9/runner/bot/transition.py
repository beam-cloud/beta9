class BotTransition:
    def __init__(self) -> None:
        pass

    def start(self):
        i = 0
        while i < 10:
            print("hi")
            i += 1


if __name__ == "__main__":
    bt = BotTransition()
    bt.start()
