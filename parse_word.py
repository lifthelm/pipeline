# parse_word.py
from pymorphy2 import MorphAnalyzer
import sys

def parse_word(word):
    morph = MorphAnalyzer()
    parsed_word = morph.parse(word)[0]
    return [ parsed_word.normal_form, str(parsed_word.tag) ]

if __name__ == "__main__":
    word = sys.argv[1]
    result = parse_word(word)
    print(*result, end='')
