class BotTransition:
    def __init__(self) -> None:
        pass

    def start(self):
        """
        So when we start, we load the transition function from config
        Then we pop the required markers from redis for this transition, and run it.
        Once we run out of markers that satisfy this specific transition, we spin down.

        Questions:
            - Do we want to set up a separate container for each transition?
            - Or do we want to have a single container that can a series of transitions of a particular type?



        """
        i = 0
        while i < 10:
            print("hi")
            i += 1


if __name__ == "__main__":
    bt = BotTransition()
    bt.start()
