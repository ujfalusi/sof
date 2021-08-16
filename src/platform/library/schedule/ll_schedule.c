// SPDX-License-Identifier: BSD-3-Clause
//
// Copyright(c) 2019 Intel Corporation. All rights reserved.
//
// Author: Tomasz Lauda <tomasz.lauda@linux.intel.com>

#include <sof/audio/component.h>
#include <sof/schedule/task.h>
#include <stdint.h>
#include <sof/schedule/schedule.h>
#include <sof/schedule/ll_schedule.h>
#include <sof/schedule/ll_schedule_domain.h>
#include <sof/lib/wait.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <poll.h>

 /* scheduler testbench definition */

/* 77de2074-828c-4044-a40b-420b72749e8b */
DECLARE_SOF_UUID("ll-schedule", ll_sched_uuid, 0x77de2074, 0x828c, 0x4044,
		 0xa4, 0x0b, 0x42, 0x0b, 0x72, 0x74, 0x9e, 0x8b);

DECLARE_TR_CTX(ll_tr, SOF_UUID(ll_sched_uuid), LOG_LEVEL_INFO);

struct ll_vcore {
	struct list_item list; /* list of tasks in priority queue */
	pthread_mutex_t list_mutex;
	pthread_t thread_id;
	int vcore_ready;
};

static int tick_period_us;

static void *ll_thread(void *data)
{
	struct ll_vcore *vc = data;
	struct timespec ts, td0, td1;
	struct list_item *tlist, *tlist_;
	struct task *task;
	int err;
	uint64_t delta;

	/* convert uS to seconds and ns */
	ts.tv_sec = tick_period_us / 1000000;
	ts.tv_nsec = (tick_period_us % 1000000) * 1000;

	while (1) {

		/*
		 * The LL scheduler works with a periodic tick which we emulate
		 * here to provide a similar processing experience on testbench
		 * to actual DSP FW.
		 */
		if (tick_period_us) {
			while (1) {
				/* wait for next tick */
				err = nanosleep(&ts, &ts);
				if (err == 0)
					break; /* sleep fully completed */
				else if (err == EINTR)
					continue; /* interrupted - keep going */
				else {
					/* something bad happened ... */
					fprintf(stderr, "error: sleep failed: %s\n",
						strerror(err));
					goto out;
				}
			}
		}

		/* LL time slice now running at this point */
		pthread_mutex_lock(&vc->list_mutex);

		/* list empty then return */
		if (list_is_empty(&vc->list)) {
			pthread_mutex_unlock(&vc->list_mutex);
			break;
		}

		/* iterate through the task list */
		list_for_item_safe(tlist, tlist_, &vc->list) {
			task = container_of(tlist, struct task, list);

			/* only run queued tasks */
			if (task->state == SOF_TASK_STATE_QUEUED) {
				pthread_mutex_unlock(&vc->list_mutex);

				/* run task and time it */
				clock_gettime(CLOCK_MONOTONIC, &td0);
				task->ops.run(task->data);
				clock_gettime(CLOCK_MONOTONIC, &td1);

				pthread_mutex_lock(&vc->list_mutex);

				/* Calculate average task exec time */
				delta = (td1.tv_sec - td0.tv_sec) * 1000000;
				delta += (td1.tv_nsec - td0.tv_nsec) / 1000;
				task->start += delta;
			}
		}

		pthread_mutex_unlock(&vc->list_mutex);
	}

out:
	/* nothing in list so stop LL thread */
	vc->vcore_ready = 0;
	return NULL;
}

static int schedule_ll_task_complete(void *data, struct task *task)
{
	struct ll_vcore *vc = data;

	/* task is complete so remove it from list */
	pthread_mutex_lock(&vc->list_mutex);
	list_item_del(&task->list);
	task->state = SOF_TASK_STATE_COMPLETED;
	pthread_mutex_unlock(&vc->list_mutex);

	return 0;
}

/* schedule new LL task */
static int schedule_ll_task(void *data, struct task *task, uint64_t start,
			    uint64_t period)
{
	struct ll_vcore *vc = data;
	int err;

	/* add task to list */
	pthread_mutex_lock(&vc->list_mutex);
	list_item_prepend(&task->list, &vc->list);
	task->state = SOF_TASK_STATE_QUEUED;
	task->start = 0;
	pthread_mutex_unlock(&vc->list_mutex);

	/* is vcore thread running ? */
	if (!vc->vcore_ready) {

		/* nope, so start thread for this virtual core */
		err = pthread_create(&vc->thread_id, NULL,
					ll_thread, &vc[task->core]);
		if (err < 0) {
			fprintf(stderr, "error: failed to create LL thread for vcore %d %s\n",
				task->core, strerror(err));
			return err;
		}

		vc->vcore_ready = 1;
	}

	return 0;
}

static void ll_scheduler_free(void *data)
{
	free(data);
}

static int schedule_ll_task_cancel(void *data, struct task *task)
{
	struct ll_vcore *vc = data;

	pthread_mutex_lock(&vc->list_mutex);
	if (task->state == SOF_TASK_STATE_QUEUED) {
		/* delete task */
		task->state = SOF_TASK_STATE_CANCEL;
		list_item_del(&task->list);
	}

	/* list empty then return */
	if (list_is_empty(&vc->list)) {
		pthread_mutex_unlock(&vc->list_mutex);
		pthread_join(vc->thread_id, NULL);
	} else
		pthread_mutex_unlock(&vc->list_mutex);

	return 0;
}

static int schedule_ll_task_free(void *data, struct task *task)
{
	struct ll_vcore *vc = data;

	pthread_mutex_lock(&vc->list_mutex);
	task->state = SOF_TASK_STATE_FREE;
	pthread_mutex_unlock(&vc->list_mutex);

	return 0;
}

static struct scheduler_ops schedule_ll_ops = {
	.schedule_task		= schedule_ll_task,
	.schedule_task_running	= NULL,
	.schedule_task_complete = schedule_ll_task_complete,
	.reschedule_task	= NULL,
	.schedule_task_cancel	= schedule_ll_task_cancel,
	.schedule_task_free	= schedule_ll_task_free,
	.scheduler_free		= ll_scheduler_free,
};

int schedule_task_init_ll(struct task *task,
			  const struct sof_uuid_entry *uid, uint16_t type,
			  uint16_t priority, enum task_state (*run)(void *data),
			  void *data, uint16_t core, uint32_t flags)
{
	return schedule_task_init(task, uid, SOF_SCHEDULE_LL_TIMER, 0, run,
				  data, core, flags);
}

/* initialize scheduler */
int scheduler_init_ll(struct ll_schedule_domain *domain)
{
	struct ll_vcore *vcore;
	int i;

	tr_info(&ll_tr, "ll_scheduler_init()");
	tick_period_us = domain->next_tick;

	vcore = calloc(sizeof(*vcore), CONFIG_CORE_COUNT);
	if (!vcore)
		return -ENOMEM;

	for (i = 0; i < CONFIG_CORE_COUNT; i++)
		list_init(&vcore[i].list);

	scheduler_init(SOF_SCHEDULE_LL_TIMER, &schedule_ll_ops, vcore);

	return 0;
}
