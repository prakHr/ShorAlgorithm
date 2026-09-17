import math
from sympy import factorint
import sys 
import mpire
import mpire
import os
os.environ["OMP_NUM_THREADS"] = "1"
import time
import multiprocessing 
from mpire import WorkerPool
from pprint import pprint
import itertools
num_cores =max(multiprocessing.cpu_count()//2,1)
import shutil
import random
from multiprocessing import Manager
from sympy import factorint, isprime
import shorAlgo
sys.set_int_max_str_digits(10**9)

def transform(arr):
    rv = []
    for i in range(len(arr)-1):
        if arr[i] != "":
            rv.append(arr[i]+arr[i+1][0])
            arr[i+1] = arr[i+1][1:]
    rv.append(arr[-1])
    rv = [r for r in rv if r!='']
    n = len(rv)
    if n==1:
        x = rv[n-1]
        rv = split_chunks_again(x)
        rv = [r for r in rv if r!='']
        return rv
    return rv

def split_chunks_again(string):
    
    chunks = []
    rv_str = ""
    for i in range(len(string)):
        if int(i)%2 == 0 and int(string[i]) != 0:
            chunks.append(rv_str)
            rv_str = string[i] 
            
        else:
            rv_str += string[i]
    chunks.append(rv_str)
    chunks = [int(c) for c in chunks if c != ""]
    return chunks

def split_chunks(string):
    chunks = []
    rv_str = ""
    for i in range(len(string)):
        if int(string[i])%2 == 0 and int(string[i]) != 0:
            chunks.append(rv_str)
            rv_str = string[i] 
            
        else:
            rv_str += string[i]
    chunks.append(rv_str)
    chunks = transform(chunks)
    chunks = [int(c) for c in chunks if c != ""]
    return chunks


def smart_factor(n):
    chunks = split_chunks(str(n))
    for c in chunks:
        g = math.gcd(n, c)
        if g>1 and g<n:
            my_dict = {g:1,n//g:1}
            return my_dict
    return {n:1}

def smart_factor_for_parallel_chunks(n,chunks):
    for c in chunks:
        g = math.gcd(n, c)
        if g>1 and g<n:
            my_dict = {g:1,n//g:1}
            return my_dict
    return {n:1}


def smart_parallel_factor(n,c):
    chunks = split_chunks(str(n))
    for c in chunks:
        g = math.gcd(n, c)
        if g>1 and g<n:
            my_dict = {g:1,n//g:1}
            return my_dict
    return {n:1}



def parallel_factor(n):
    c = split_chunks(str(n))
    results = [{"n" : int(x), "c" : c} for x in split_chunks(str(n))]
    with WorkerPool(n_jobs=num_cores,daemon=False) as pool:
        results = pool.map(smart_parallel_factor, results, progress_bar=False)
    return results


    
def parallel_temp_dict_factor(item, shared_dict):
    val = int(item["item"])
    # print(item["item"])
    if isprime(val):
        shared_dict[val] = shared_dict.get(val, 0) + 1
        return
    if len(str(val))>10:
        x = str(val)
        # print(f"item['item'] = {x}")
        factors =  parallel_for_loop_factor(val)
        # print(f"factors for {item['item']} = {factors}")        
    else:
        factors = factorint(val)
    for f, exp in factors.items():
        if f in shared_dict:    
            shared_dict[f] += exp
        else:
            shared_dict[f] = exp
    # print(factors)

def parallel_temp_dict_factor_original(item, shared_dict):
    val = int(item["item"])
    # print(item["item"])
    if isprime(val):
        shared_dict[val] = shared_dict.get(val, 0) + 1
        return
    if len(str(val))>10:
        x = str(val)
        # print(f"item['item'] = {x}")
        factors =  parallel_for_loop_factor_original(val)
        # print(f"factors for {item['item']} = {factors}")        
    else:
        factors = factorint(val)
    for f, exp in factors.items():
        if f in shared_dict:    
            shared_dict[f] += exp
        else:
            shared_dict[f] = exp
    # print(factors)

def parallel_temp_dict_factor(item, shared_dict):
    val = int(item["item"])

    # if isprime(val):
    #     shared_dict[val] = shared_dict.get(val, 0) + 1
    #     return

    temp_dict = smart_factor(val)

    if list(temp_dict.keys()) == [val]:
        if len(str(val)) > 10:
            key = int(val)
            # print(f"[fallback-factorint] item = {val}")
            # print(f"[recurse] item = {key}")
            # print(f"attempting to subfactor {key} using parallel for loop")
            subfactors = shorEvenAlgo.parallel_for_loop_factor(key)
            # print(f"subfactors for {key} = {subfactors}")
            factors = {}
            for f, exp in subfactors.items():
                factors[f] = factors.get(f, 0) + exp
            
            for f, exp in factors.items():
                shared_dict[f] = shared_dict.get(f, 0) + exp
            # print("returning from fallback")
            return
        else:
            factors = factorint(val)
    else:
        factors = {}
        for k in temp_dict:
            if k == val:

                factors[val] = 1
            elif isprime(k):
                factors[k] = factors.get(k, 0) + 1
            else:
                subfactors = parallel_for_loop_factor(k)
                print(f"subfactors for {k} = {subfactors}")
                for f, exp in subfactors.items():
                    factors[f] = factors.get(f, 0) + exp

    for f, exp in factors.items():
        shared_dict[f] = shared_dict.get(f, 0) + exp

def parallel_for_loop_factor_original(n):
    # print(f"parallel_for_loop_factor_original called with n = {n}")
    results = parallel_factor(n)
    # print(f"parallel_factor results for {n} = {results}")
    chunks = []
    for my_dict in results:
        chunks.extend(my_dict.keys())
    # print(chunks)
    temp_dict = smart_factor_for_parallel_chunks(n,chunks)
    # print(temp_dict)
    items = [{"item": k} for k in list(temp_dict.keys())]
    # print(f"items = {items}")

    with Manager() as manager:
        shared_dict = manager.dict()  

        with WorkerPool(n_jobs=num_cores, daemon=False) as pool:
            pool.map(
                parallel_temp_dict_factor,
                [(item, shared_dict) for item in items],
                progress_bar=False
            )

        # Convert back to normal dict
        final = dict(shared_dict)
    return final
  

def parallel_for_loop_factor(n):
    results = parallel_factor(n)
    chunks = []
    for my_dict in results:
        chunks.extend(my_dict.keys())
    # print(chunks)
    temp_dict = smart_factor_for_parallel_chunks(n,chunks)
    # print(temp_dict)
    items = [{"item": k} for k in list(temp_dict.keys())]
    # print(f"items = {items}")

    with Manager() as manager:
        shared_dict = manager.dict()  

        with WorkerPool(n_jobs=num_cores, daemon=False) as pool:
            pool.map(
                parallel_temp_dict_factor_original,
                [(item, shared_dict) for item in items],
                progress_bar=False
            )

        # Convert back to normal dict
        final = dict(shared_dict)
    return final
  

def for_loop_factor(n):
    temp_dict = smart_factor(n)
    rv = {}
    for k,v in temp_dict.items():
        factors = factorint(k)
        for f, exp in factors.items():
            if f in rv:
                rv[f] += exp
            else:
                rv[f] = exp
    return rv
    

if __name__ == "__main__":
    NEWLINE_CHAR = "\n"
    # for string in ["4","6","10","19","9992","999999992","1234","1234567898"]:
    #     # print(string)
    #     out = parallel_for_loop_factor(int(string))
    #     print(f"Factors of {string} using for loop: {out}")
    #     print(NEWLINE_CHAR)

    for string in ["2"*10,"2"*20,"2"*50,"2"*100,"2"*200]:
        # print(len(string))
        fast_out = parallel_for_loop_factor(int(string))
        print(f"Factors of {string} using parallel for loop: {fast_out}")
        print(NEWLINE_CHAR)
    # length = 100
    # string = ""
    # for i in range(1, length+1):
    #     string += str(random.randint(1,9))
    # n=len(string)
    # if int(string[n-1])%2==1:
    #     string = string[:-1] + "2"
    # print(string)
    # print(len(string))
    # fast_out = parallel_for_loop_factor(int(string))
    # print(f"Factors of {string} using parallel for loop: {fast_out}")

    # for length in [10, 11, 12, 13, 14, 15, 16]:
    #     string = ""
    #     for i in range(1, length+1):
    #         string += str(9)
    #     n=len(string)
    #     if int(string[n-1])%2==1:
    #         string = string[:-1] + "2"
    #     print(string)
    #     print(len(string))
    #     fast_out = parallel_for_loop_factor(int(string))
    #     print(f"Factors of {string} using parallel for loop: {fast_out}")

    #     print("\n\n")

    # for length in [20]:
    #     string = ""
    #     for i in range(1, length+1):
    #         string += str(9)
    #     n=len(string)
    #     if int(string[n-1])%2==1:
    #         string = string[:-1] + "2"
    #     print(string)
    #     print(len(string))
    #     fast_out = parallel_for_loop_factor(int(string))
    #     print(f"Factors of {string} using parallel for loop: {fast_out}")

    #     print("\n\n")
    
    # string = "11"
    # print(string)
    # out = parallel_for_loop_factor(int(string))
    # print(f"Factors of {string} using for loop: {out}")

    # length = 100
    # string = ""
    # for i in range(1, length+1):
    #     string += str(random.randint(1,9))
    # n=len(string)
    # if int(string[n-1])%2==1:
    #     string = string[:-1] + "1"
    # print(string)
    # print(len(string))
    # fast_out = parallel_for_loop_factor(int(string))
    # print(f"Factors of {string} using parallel for loop: {fast_out}")
