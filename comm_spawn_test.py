
import sys
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

comm = MPI.COMM_SELF.Spawn(sys.executable, 
        args="sub.py", 
        maxprocs=4)
