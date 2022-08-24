
import re
from constants import (
    ONE_TO_ONE, 
    ALL_TO_ALL, 
    NONE,
    DEPENDENCY_STRATEGIES
)

grammar = re.compile(r"\[ (\[ [0-9]+(,[0-9]+|i|r)*] \])+\]")
def getDependencies(self, strategy, j):
    
    if (strategy not in DEPENDENCY_STRATEGIES):
        #print(grammar.fullmatch(strategy))
        pass
