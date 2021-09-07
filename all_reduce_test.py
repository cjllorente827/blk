import sys
from mpi4py import MPI

comm = MPI.COMM_SELF.Spawn(sys.executable, 
        args=["sub_all_reduce_test.py"], 
        maxprocs=4)


print(comm.Get_name())
size = comm.Get_size()

all_mins = []
all_mins = comm.gather(all_mins, root=MPI.ROOT)

print(all_mins)
