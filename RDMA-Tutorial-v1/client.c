#define _GNU_SOURCE
#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>

#include "debug.h"
#include "config.h"
#include "setup_ib.h"
#include "ib.h"
#include "client.h"

#include <stdatomic.h>


pthread_mutex_t traindata_mux;
struct TrainData{
    int thread;
    long global_ops_count;
    int msg_size;
    int msgs;
}traindata = {0, 0, 0, 0};

void *client_thread_func (void *arg)
{
    //加锁更新全局 ops_count
    pthread_mutex_lock(&traindata_mux);
    traindata.msg_size = config_info.num_concurr_msgs;
    traindata.msgs = config_info.msg_size;
    pthread_mutex_unlock(&traindata_mux);


    int         ret             = 0, i = 0, n = 0;
    long	thread_id	= (long) arg;
    int         num_concurr_msgs= config_info.num_concurr_msgs;
    int         msg_size        = config_info.msg_size;
    int		num_wc		= 20;
    bool	start_sending   = false;
    bool        stop            = false;

    pthread_t   self;
    cpu_set_t   cpuset;

    struct ibv_qp	*qp	    = ib_res.qp;
    struct ibv_cq	*cq	    = ib_res.cq;
    struct ibv_wc	*wc	    = NULL;
    uint32_t             lkey       = ib_res.mr->lkey;
    char		*buf_ptr    = ib_res.ib_buf;
    int			 buf_offset = 0;
    size_t               buf_size   = ib_res.ib_buf_size;

    struct timeval      start, end;
    long                ops_count  = 0;
    double              duration   = 0.0;
    double              throughput = 0.0;

    /* set thread affinity */
    CPU_ZERO (&cpuset);
    CPU_SET  ((int)thread_id, &cpuset);
    self = pthread_self ();
    ret  = pthread_setaffinity_np (self, sizeof(cpu_set_t), &cpuset);
    check (ret == 0, "thread[%ld]: failed to set thread affinity", thread_id);

    /* pre-post recvs */
    wc = (struct ibv_wc *) calloc (num_wc, sizeof(struct ibv_wc));
    check (wc != NULL, "thread[%ld]: failed to allocate wc", thread_id);

    for (i = 0; i < num_concurr_msgs; i++) {
	ret = post_recv (msg_size, lkey, (uint64_t)buf_ptr, qp, buf_ptr);
	check (ret == 0, "thread[%ld]: failed to post recv", thread_id);
	buf_offset = (buf_offset + msg_size) % buf_size;
	buf_ptr += buf_offset;
    }

    /* wait for start signal */
    while (start_sending != true) {
        do {
            n = ibv_poll_cq (cq, num_wc, wc);
        } while (n < 1);
        check (n > 0, "thread[%ld]: failed to poll cq", thread_id);

        for (i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                check (0, "thread[%ld]: wc failed status: %s.",
                       thread_id, ibv_wc_status_str(wc[i].status));
            }
	    if (wc[i].opcode == IBV_WC_RECV) {
		/* post a receive */
		post_recv (msg_size, lkey, (uint64_t)buf_ptr, qp, buf_ptr);
		buf_offset = (buf_offset + msg_size) % buf_size;
		buf_ptr += buf_offset;

                if (ntohl(wc[i].imm_data) == MSG_CTL_START) {
		    start_sending = true;
		    break;
                }
            }
        }
    }
    log ("thread[%ld]: ready to send", thread_id);
    
    /* pre-post sends */
    buf_ptr = ib_res.ib_buf;
    for (i = 0; i < num_concurr_msgs; i++) {
	ret = post_send (msg_size, lkey, 0, MSG_REGULAR, qp, buf_ptr);
	check (ret == 0, "thread[%ld]: failed to post send", thread_id);
	buf_offset = (buf_offset + msg_size) % buf_size;
	buf_ptr += buf_offset;
    }
    

    while (stop != true) {    
	/* poll cq */
	n = ibv_poll_cq (cq, num_wc, wc);
        if (n < 0) {
            check (0, "thread[%ld]: Failed to poll cq", thread_id);
        }

        for (i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].opcode == IBV_WC_SEND) {
                    check (0, "thread[%ld]: send failed status: %s",
                           thread_id, ibv_wc_status_str(wc[i].status));
                } else {
                    check (0, "thread[%ld]: recv failed status: %s",
                           thread_id, ibv_wc_status_str(wc[i].status));
                }
            }

            if (wc[i].opcode == IBV_WC_RECV) {
		ops_count += 1;
        
        //加锁更新全局 ops_count
        pthread_mutex_lock(&traindata_mux);
        traindata.global_ops_count += 1;
        pthread_mutex_unlock(&traindata_mux);
        
        debug ("ops_count = %ld", ops_count);


        // if (ops_count % 1000 == 0) {  // 每 1000 次操作加锁一次，减少开销
        //     pthread_mutex_lock(&traindata_mux);
        //     traindata.global_ops_count += 1000; 
        //     traindata.thread = thread_id;
        //     debug ("ops_count = %ld", traindata.global_ops_count);
        //     pthread_mutex_unlock(&traindata_mux);
        // }

        // atomic_fetch_add(&global_ops_countx, 1);
        // debug ("ops_count = %ld", global_ops_countx);

        // if (ops_count % 100 == 0)
                
 

		if (ops_count == NUM_WARMING_UP_OPS) {
		    gettimeofday (&start, NULL);
		}

		if (ntohl(wc[i].imm_data) == MSG_CTL_STOP) {
		    gettimeofday (&end, NULL);
		    stop = true;
		    break;
		}
		
		/* echo the message back */
		char *msg_ptr = (char *)wc[i].wr_id;
		post_send (msg_size, lkey, 0, MSG_REGULAR, qp, msg_ptr);

        /* post a new receive */
        post_recv (msg_size, lkey, (uint64_t)buf_ptr, qp, buf_ptr);
		buf_offset = (buf_offset + msg_size) % buf_size;
		buf_ptr += buf_offset;
	    }
	} /* loop through all wc */
    }

    /* dump statistics */
    duration   = (double)((end.tv_sec - start.tv_sec) * 1000000 + 
			  (end.tv_usec - start.tv_usec));
    throughput = (double)(ops_count) / duration;
    log ("thread[%ld]: throughput = %f (Mops/s)",  thread_id, throughput);
    

    free (wc);
    pthread_exit ((void *)0);

 error:
    if (wc != NULL) {
        free (wc);
    }
    pthread_exit ((void *)-1);
}


// 数据导出线程函数
void *export_thread_func(void *arg) {
    long precount = 0;
    // 打开 CSV 文件以写入数据
    FILE *fp = fopen("exportdata.csv", "a");
    if (fp == NULL) {
        perror("Failed to open file");
        return NULL; //有问题可能
    }
    while (1) {
  
        sleep(1); // 每隔 1 秒导出一次数据

        // 加锁读取全局 ops_count
        pthread_mutex_lock(&traindata_mux);
        long current_ops_count = traindata.global_ops_count;
        int thread = traindata.thread;
        int msg_size = traindata.msg_size;
        int msgs = traindata.msgs;
        pthread_mutex_unlock(&traindata_mux);
        long this_count = current_ops_count - precount;
        precount = current_ops_count;

        fprintf(fp, "duration:%d    thread:%d   ops:%ld throughput:%f   concurrent msgs:%d   msg size:%d", (int)1, thread, this_count, (double)this_count, msgs, msg_size);
        // 每行数据结束后添加换行符
        fprintf(fp, "\n");
        fflush(fp);
            // 关闭文件
   
}
    fclose(fp);
    return NULL;

    }


int run_client ()
{
    int		ret	    = 0;
    long	num_threads = 1;
    long	i	    = 0;
    
    pthread_t	   *client_threads = NULL;
    pthread_attr_t  attr;
    void	   *status;

    log (LOG_SUB_HEADER, "Run Client");
    
    /* initialize threads */
    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_JOINABLE);

    client_threads = (pthread_t *) calloc (num_threads, sizeof(pthread_t));
    check (client_threads != NULL, "Failed to allocate client_threads.");

    for (i = 0; i < num_threads; i++) {
	ret = pthread_create (&client_threads[i], &attr, 
			      client_thread_func, (void *)i);
	check (ret == 0, "Failed to create client_thread[%ld]", i);
    }
    
    pthread_t export_thread;
    // 创建数据导出线程
    if (pthread_create(&export_thread, NULL, export_thread_func, NULL) != 0) {
        perror("Failed to create export thread");
        return 1;
    }

    bool thread_ret_normally = true;
    for (i = 0; i < num_threads; i++) {
	ret = pthread_join (client_threads[i], &status);
	check (ret == 0, "Failed to join client_thread[%ld].", i);
	if ((long)status != 0) {
            thread_ret_normally = false;
            log ("thread[%ld]: failed to execute", i);
        }
    }

    if (thread_ret_normally == false) {
        goto error;
    }

    pthread_attr_destroy (&attr);
    free (client_threads);
    return 0;

 error:
    if (client_threads != NULL) {
        free (client_threads);
    }
    
    pthread_attr_destroy (&attr);
    return -1;
}
