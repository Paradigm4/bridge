#!/usr/bin/env python3

import os
import sys
import scidbbridge

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(f"Need path to array: {sys.argv}")
        sys.exit(1)

    ar = scidbbridge.Array(sys.argv[1])    
    idx = ar.build_index()
    print(f"Array has {len(idx)} chunks")
    ar.write_index(idx)
