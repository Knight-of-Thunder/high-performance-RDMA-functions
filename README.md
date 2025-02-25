# high-performance RDMA functions

This project is intended to implement some functions 
which can improve the performance of RDMA

Each of the functions is implemented in a branch,
use checkout to go to the branch which contains the function 
and test environment

NOW implemented functions is listed here:
- PreSend







# local test enviroment:

ubuntu 22.04 (virtual machine)

gcc 12.1

# compile and use
`make` or `make debug`

server terminal:  `./rdma-tutorial port`
client terminal:  `./rdma-tutorial server_ip  port`

do not forget `sudo rdma link add rxe_name type rxe netdev xxx` in ubuntu
