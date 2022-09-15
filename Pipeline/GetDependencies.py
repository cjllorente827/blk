
import re
from blk.constants import (
    ONE_TO_ONE, 
    ALL_TO_ALL, 
    NONE,
    DEPENDENCY_STRATEGIES
)

ROUND_ROBIN_COUNTER = 0

grammar = re.compile(r"\[\[[0-9]+(,([0-9]+|i|r))?\](,\[[0-9]+(,([0-9]+|i|r))?\])*\]")
def getDependencies(self, strategy, num_tasks):
    global ROUND_ROBIN_COUNTER

    dependencies_list = [[] for x in range(num_tasks)]

    if (strategy in DEPENDENCY_STRATEGIES):

        segment_index = -2

        for j in range(num_tasks):

            if strategy == ONE_TO_ONE:
                dependencies_list[j] += [self.all_tasks[segment_index][j]]
            elif strategy == ALL_TO_ALL:
                dependencies_list[j] += self.all_tasks[segment_index]
            else: # NONE 
                pass
        return dependencies_list

    strategy = strategy.replace(" ", "")
    
    grammar_match = grammar.fullmatch(strategy)
    
    if grammar_match == None:
        print(f"[Error] Invalid syntax for dependency strategy: {strategy}")
        exit()

    tokens = self.guessType(strategy)

    try :
    
        for i, token in enumerate(tokens):
            for j in range(num_tasks):

                segment_index = token[0]-1

                if len(token) == 1:
                    dependencies_list[j] += self.all_tasks[segment_index]
                    continue

                # if len(token) isnt 1, then it should be guaranteed to be 2
                if token[1] == 'i':
                    dependencies_list[j] += [self.all_tasks[segment_index][j]]
                elif token[1] == 'r':

                    if ROUND_ROBIN_COUNTER >= len(self.all_tasks[segment_index]):
                        ROUND_ROBIN_COUNTER = 0
                    dependencies_list[j] += [self.all_tasks[segment_index][ROUND_ROBIN_COUNTER]]
                    ROUND_ROBIN_COUNTER += 1
                else: # assume token[1] is an integer
                    dependencies_list[j] += [self.all_tasks[segment_index][token[1]]]
    except IndexError as e:
        print("[Error] Out of bounds error occurred. Please check your config file for errors.")
        print(f""" 
        Current segment : {len(self.all_tasks)}
        Number of tasks : {num_tasks}
        Previous segment : {segment_index+1}
        Number of tasks in previous segment : {len(self.all_tasks[segment_index])}
        Attempted dependency strategy : {strategy}
        Problem occurred during: {token}
        """)
        exit()

  

    return dependencies_list
        
