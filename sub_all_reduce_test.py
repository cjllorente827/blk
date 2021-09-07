from mpi4py import MPI

comm = MPI.COMM_WORLD
parent_comm = MPI.Comm.Get_parent()
size = comm.Get_size()
rank = comm.Get_rank()

print(f"Rank {rank} using comm {comm.Get_name()}")

my_min = (rank+1)*100

print(f"Rank {rank} starts with {my_min}")

actual_min = comm.allreduce(my_min, op=MPI.MIN)

print(f"Rank {rank} says the minimum is {actual_min}")

parent_comm.gather(actual_min, root=0)

parent_comm.Disconnect()