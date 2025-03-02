#define _GNU_SOURCE
#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>

#include "debug.h"
#include "config.h"
#include "setup_ib.h"
#include "ib.h"
#include "client.h"
//new lib
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
//new function
// 定义发送记录的数据结构
typedef struct {
    uint32_t req_size;
    char *buf;
    struct timeval last_send_time;  // 上一次发送的时间
    int send_count;  // 发送次数
    uint32_t avg_interval;  // 平均发送间隔（毫秒）
    int valid;  // 记录是否有效
} SendRecord;

#define MAX_RECORDS 10  // 固定大小的记录数组

// 获取当前时间
struct timeval get_current_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv;
}

// 计算两个时间的差值（毫秒）
long time_diff_ms(struct timeval start, struct timeval end) {
    return (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
}

// 查找记录中是否存在相同的数据
int find_record(SendRecord *records, int record_count, uint32_t req_size, char *buf) {
    for (int i = 0; i < record_count; i++) {
        if (records[i].valid && records[i].req_size == req_size) {
            // 检查指针是否有效
            if (records[i].buf != NULL && buf != NULL && req_size > 0) {
                if (memcmp(records[i].buf, buf, req_size) == 0) {
                    return i;
                }
            }
        }
    }
    return -1;
}

// 记录发送信息
void record_send(uint32_t req_size, char *buf, uint32_t lkey, uint64_t wr_id,
                 uint32_t imm_data, struct ibv_qp *qp, SendRecord *records, int *record_count) {
    int index = find_record(records, *record_count, req_size, buf);
    if (index != -1) {
        // 存在相同数据的记录
        struct timeval current_time = get_current_time();
        long diff = time_diff_ms(records[index].last_send_time, current_time);
        records[index].send_count++;
        if (records[index].send_count == 2) {
            records[index].avg_interval = diff;
        } else if (records[index].send_count > 2) {
            records[index].avg_interval = (records[index].avg_interval * (records[index].send_count - 1) + diff) / records[index].send_count;
        }
        records[index].last_send_time = current_time;
    } else {
        // 不存在相同数据的记录，添加新记录
        if (*record_count < MAX_RECORDS) {
            records[*record_count].req_size = req_size;
            records[*record_count].buf = buf;
            records[*record_count].last_send_time = get_current_time();
            records[*record_count].send_count = 1;
            records[*record_count].avg_interval = 0;
            records[*record_count].valid = 1;
            (*record_count)++;
        }
    }
    int ret = post_send(req_size, lkey, wr_id, imm_data, qp, buf);
    if (ret != 0) {
        // 处理发送失败的情况
        printf("Failed to post send\n");
    }
}

//
static SendRecord records[MAX_RECORDS];
static int record_count = 0;


// 检查并执行预发送
void check_and_send(uint32_t lkey, uint64_t wr_id, uint32_t imm_data, struct ibv_qp *qp) {
    // static SendRecord records[MAX_RECORDS];
    // static int record_count = 0;
    struct timeval current_time = get_current_time();
    // record_send(req_size, buf, lkey, wr_id, imm_data, qp, records, &record_count);
    printf("get time succeed\n");
    for (int i = 0; i < record_count; i++) {
        if (records[i].valid && records[i].send_count >= 2 &&
            time_diff_ms(current_time, records[i].last_send_time) >= records[i].avg_interval) {
                printf("timediff succeed\n");
            post_send(records[i].req_size,lkey,wr_id,imm_data,qp,records[i].buf);
        }
    }
    return;
}



void *client_thread_func (void *arg)
{
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
    // for (i = 0; i < num_concurr_msgs; i++) {
	// ret = post_send (msg_size, lkey, 0, MSG_REGULAR, qp, buf_ptr);
	// check (ret == 0, "thread[%ld]: failed to post send", thread_id);
	// buf_offset = (buf_offset + msg_size) % buf_size;
	// buf_ptr += buf_offset;
    // }
    

    for (i = 0; i < 20; i++) {
    // check_and_send (lkey, 0, MSG_REGULAR, qp, msg_size, buf_ptr);
    record_send(msg_size, buf_ptr, lkey, 0, MSG_REGULAR, qp, records, &record_count);
	buf_offset = (buf_offset + msg_size) % buf_size;
	buf_ptr += buf_offset;
    }
    //check the records and pre_send
    // 模拟循环检查
    for (int i = 0; i < 30; i++) {
        check_and_send(lkey, 0, MSG_REGULAR, qp);
        sleep(1);  // 每秒检查一次
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
		debug ("ops_count = %ld", ops_count);

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
