# encoding: utf-8

from collections import defaultdict


class SequenceGenerator(object):
    def __init__(self):
        self.__d = defaultdict(int)

    def get_next(self, key):
        self.__d[key] += 1
        return str(self.__d[key])


if __name__ == "__main__":
    sg = SequenceGenerator()
    for i in range(1, 999):
        assert sg.get_next('order') == str(i)

    text = 'trade'
    sg.get_next(text)
    sg.get_next(text)
    for i in range(3, 999):
        assert sg.get_next(text) == str(i)
