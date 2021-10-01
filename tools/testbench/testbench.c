// SPDX-License-Identifier: BSD-3-Clause
//
// Copyright(c) 2018 Intel Corporation. All rights reserved.
//
// Author: Seppo Ingalsuo <seppo.ingalsuo@linux.intel.com>
//         Ranjani Sridharan <ranjani.sridharan@linux.intel.com>

#include <pthread.h>
#include <sof/ipc/driver.h>
#include <sof/ipc/topology.h>
#include <sof/list.h>
#include <getopt.h>
#include <dlfcn.h>
#include "testbench/common_test.h"
#include <tplg_parser/topology.h>
#include "testbench/trace.h"
#include "testbench/file.h"

#ifdef TESTBENCH_CACHE_CHECK
#include <arch/lib/cache.h>
struct tb_cache_context hc = {0};

/* cache debugger */
struct tb_cache_context *tb_cache = &hc;
int tb_elem_id;
#else

/* host thread context - folded into cachec context when cache debug is enabled */
struct tb_host_context {
	pthread_t thread_id[CACHE_VCORE_COUNT];
};
struct tb_host_context hc = {0};
#endif

#define DECLARE_SOF_TB_UUID(entity_name, uuid_name,			\
			 va, vb, vc,					\
			 vd0, vd1, vd2, vd3, vd4, vd5, vd6, vd7)	\
	struct sof_uuid uuid_name = {					\
		.a = va, .b = vb, .c = vc,				\
		.d = {vd0, vd1, vd2, vd3, vd4, vd5, vd6, vd7}		\
	}

#define SOF_TB_UUID(uuid_name) (&(uuid_name))

DECLARE_SOF_TB_UUID("crossover", crossover_uuid, 0x948c9ad1, 0x806a, 0x4131,
		    0xad, 0x6c, 0xb2, 0xbd, 0xa9, 0xe3, 0x5a, 0x9f);

DECLARE_SOF_TB_UUID("tdfb", tdfb_uuid,  0xdd511749, 0xd9fa, 0x455c,
		    0xb3, 0xa7, 0x13, 0x58, 0x56, 0x93, 0xf1, 0xaf);

DECLARE_SOF_TB_UUID("drc", drc_uuid, 0xb36ee4da, 0x006f, 0x47f9,
		    0xa0, 0x6d, 0xfe, 0xcb, 0xe2, 0xd8, 0xb6, 0xce);

DECLARE_SOF_TB_UUID("multiband_drc", multiband_drc_uuid, 0x0d9f2256, 0x8e4f, 0x47b3,
		    0x84, 0x48, 0x23, 0x9a, 0x33, 0x4f, 0x11, 0x91);

#define TESTBENCH_NCH 2 /* Stereo */

struct pipeline_thread_data {
	struct testbench_prm *tp;
	int count;			/* copy iteration count */
	int core_id;
};

/* shared library look up table */
struct shared_lib_table lib_table[NUM_WIDGETS_SUPPORTED] = {
	{"file", "", SOF_COMP_HOST, NULL, 0, NULL}, /* File must be first */
	{"volume", "libsof_volume.so", SOF_COMP_VOLUME, NULL, 0, NULL},
	{"src", "libsof_src.so", SOF_COMP_SRC, NULL, 0, NULL},
	{"asrc", "libsof_asrc.so", SOF_COMP_ASRC, NULL, 0, NULL},
	{"eq-fir", "libsof_eq-fir.so", SOF_COMP_EQ_FIR, NULL, 0, NULL},
	{"eq-iir", "libsof_eq-iir.so", SOF_COMP_EQ_IIR, NULL, 0, NULL},
	{"dcblock", "libsof_dcblock.so", SOF_COMP_DCBLOCK, NULL, 0, NULL},
	{"crossover", "libsof_crossover.so", SOF_COMP_NONE, SOF_TB_UUID(crossover_uuid), 0, NULL},
	{"tdfb", "libsof_tdfb.so", SOF_COMP_NONE, SOF_TB_UUID(tdfb_uuid), 0, NULL},
	{"drc", "libsof_drc.so", SOF_COMP_NONE, SOF_TB_UUID(drc_uuid), 0, NULL},
	{"multiband_drc", "libsof_multiband_drc.so", SOF_COMP_NONE, SOF_TB_UUID(multiband_drc_uuid), 0, NULL},
};

/* compatible variables, not used */
intptr_t _comp_init_start, _comp_init_end;

/*
 * Parse output filenames from user input
 * This function takes in the output filenames as an input in the format:
 * "output_file1,output_file2,..."
 * The max supported output filename number is 4, min is 1.
 */
static int parse_output_files(char *outputs, struct testbench_prm *tp)
{
	char *output_token = NULL;
	char *token = strtok_r(outputs, ",", &output_token);
	int index;

	for (index = 0; index < MAX_OUTPUT_FILE_NUM && token; index++) {
		/* get output file name with current index */
		tp->output_file[index] = strdup(token);

		/* next output */
		token = strtok_r(NULL, ",", &output_token);
	}

	if (index == MAX_OUTPUT_FILE_NUM && token) {
		fprintf(stderr, "error: max output file number is %d\n",
			MAX_OUTPUT_FILE_NUM);
		for (index = 0; index < MAX_OUTPUT_FILE_NUM; index++)
			free(tp->output_file[index]);
		return -EINVAL;
	}

	/* set total output file number */
	tp->output_file_num = index;
	return 0;
}

static int parse_pipelines(char *pipelines, struct testbench_prm *tp)
{
	char *output_token = NULL;
	char *token = strtok_r(pipelines, ",", &output_token);
	int index;

	for (index = 0; index < MAX_OUTPUT_FILE_NUM && token; index++) {
		/* get output file name with current index */
		tp->pipelines[index] = atoi(token);

		/* next output */
		token = strtok_r(NULL, ",", &output_token);
	}

	if (index == MAX_OUTPUT_FILE_NUM && token) {
		fprintf(stderr, "error: max output file number is %d\n",
			MAX_OUTPUT_FILE_NUM);
		return -EINVAL;
	}

	/* set total output file number */
	tp->pipeline_num = index;
	return 0;
}

/*
 * Parse shared library from user input
 * Currently only handles volume and src comp
 * This function takes in the libraries to be used as an input in the format:
 * "vol=libsof_volume.so,src=libsof_src.so,..."
 * The function parses the above string to identify the following:
 * component type and the library name and sets up the library handle
 * for the component and stores it in the shared library table
 */
static int parse_libraries(char *libs)
{
	char *lib_token = NULL;
	char *comp_token = NULL;
	char *token = strtok_r(libs, ",", &lib_token);
	int index;

	while (token) {

		/* get component type */
		char *token1 = strtok_r(token, "=", &comp_token);

		/* get shared library index from library table */
		index = get_index_by_name(token1, lib_table);
		if (index < 0) {
			fprintf(stderr, "error: unsupported comp type %s\n", token1);
			return -EINVAL;
		}

		/* get shared library name */
		token1 = strtok_r(NULL, "=", &comp_token);
		if (!token1)
			break;

		/* set to new name that may be used while loading */
		strncpy(lib_table[index].library_name, token1,
			MAX_LIB_NAME_LEN - 1);

		/* next library */
		token = strtok_r(NULL, ",", &lib_token);
	}
	return 0;
}

/* print usage for testbench */
static void print_usage(char *executable)
{
	printf("Usage: %s -i <input_file> ", executable);
	printf("-o <output_file1,output_file2,...> ");
	printf("-t <tplg_file> -b <input_format> -c <channels>");
	printf("-a <comp1=comp1_library,comp2=comp2_library>\n");
	printf("   input_format should be S16_LE, S32_LE, S24_LE or FLOAT_LE\n\n");
	printf("Example Usage:\n");
	printf("%s -i in.txt -o out.txt -t test.tplg ", executable);
	printf("-r 48000 -R 96000 -c 2 ");
	printf("-b S16_LE -a volume=libsof_volume.so\n");
	printf("-C number of copy() iterations\n");
	printf("-P number of dynamic pipeline iterations\n");
	printf("-s Use real time priorities for threads (needs sudo)\n");
}

/* free components */
static void pipeline_free_comps(int pipeline_id)
{
	struct list_item *clist;
	struct list_item *temp;
	struct ipc_comp_dev *icd = NULL;

	/* remove the components for this pipeline */
	list_for_item_safe(clist, temp, &sof_get()->ipc->comp_list) {
		icd = container_of(clist, struct ipc_comp_dev, list);

		switch (icd->type) {
		case COMP_TYPE_COMPONENT:
			if (icd->cd->pipeline->pipeline_id != pipeline_id)
				break;
			ipc_comp_free(sof_get()->ipc, icd->id);
			break;
		case COMP_TYPE_BUFFER:
			if (icd->cb->pipeline_id != pipeline_id)
				break;
			ipc_buffer_free(sof_get()->ipc, icd->id);
			break;
		default:
			if (icd->pipeline->pipeline_id != pipeline_id)
				break;
			ipc_pipeline_free(sof_get()->ipc, icd->id);
			break;
		}
	}
}

static void pipeline_set_test_limits(int pipeline_id, int max_copies, int max_samples)
{
	struct list_item *clist;
	struct list_item *temp;
	struct ipc_comp_dev *icd = NULL;
	struct comp_dev *cd;
	struct file_comp_data *fcd;

	/* set the test limits for this pipeline */
	list_for_item_safe(clist, temp, &sof_get()->ipc->comp_list) {
		icd = container_of(clist, struct ipc_comp_dev, list);

		switch (icd->type) {
		case COMP_TYPE_COMPONENT:
			cd = icd->cd;
			if (cd->pipeline->pipeline_id != pipeline_id)
				break;

			switch (cd->drv->type) {
			case SOF_COMP_HOST:
			case SOF_COMP_DAI:
			case SOF_COMP_FILEREAD:
			case SOF_COMP_FILEWRITE:
				/* only file limits supported today. TODO: add others */
				fcd = comp_get_drvdata(cd);
				fcd->max_samples = max_samples;
				fcd->max_copies = max_copies;
				break;
			default:
				break;
			}
			break;
		case COMP_TYPE_BUFFER:
		default:
			break;
		}
	}
}

static void pipeline_get_file_stats(int pipeline_id)
{
	struct list_item *clist;
	struct list_item *temp;
	struct ipc_comp_dev *icd = NULL;
	struct comp_dev *cd;
	struct file_comp_data *fcd;
	unsigned long time;

	/* get the file IO status for each file in pipeline */
	list_for_item_safe(clist, temp, &sof_get()->ipc->comp_list) {
		icd = container_of(clist, struct ipc_comp_dev, list);

		switch (icd->type) {
		case COMP_TYPE_COMPONENT:
			cd = icd->cd;
			if (cd->pipeline->pipeline_id != pipeline_id)
				break;
			switch (cd->drv->type) {
			case SOF_COMP_HOST:
			case SOF_COMP_DAI:
			case SOF_COMP_FILEREAD:
			case SOF_COMP_FILEWRITE:
				fcd = comp_get_drvdata(cd);
				time = cd->pipeline->pipe_task->start;
				if (fcd->fs.copy_count == 0)
					fcd->fs.copy_count = 1;
				printf("file %s: samples %d copies %d total time %zu uS avg time %zu uS\n",
				       fcd->fs.fn, fcd->fs.n, fcd->fs.copy_count,
				       time, time / fcd->fs.copy_count);
				break;
			default:
				break;
			}
			break;
		case COMP_TYPE_BUFFER:
		default:
			break;
		}
	}
}

static int parse_input_args(int argc, char **argv, struct testbench_prm *tp)
{
	int option = 0;
	int ret = 0;

	while ((option = getopt(argc, argv, "hdi:o:t:b:a:r:R:c:C:P:Vp:T:D:s")) != -1) {
		switch (option) {
		/* input sample file */
		case 'i':
			tp->input_file = strdup(optarg);
			break;

		/* output sample files */
		case 'o':
			ret = parse_output_files(optarg, tp);
			break;

		/* topology file */
		case 't':
			tp->tplg_file = strdup(optarg);
			break;

		/* input samples bit format */
		case 'b':
			tp->bits_in = strdup(optarg);
			tp->frame_fmt = find_format(tp->bits_in);
			break;

		/* override default libraries */
		case 'a':
			ret = parse_libraries(optarg);
			break;

		/* input sample rate */
		case 'r':
			tp->fs_in = atoi(optarg);
			break;

		/* output sample rate */
		case 'R':
			tp->fs_out = atoi(optarg);
			break;

		/* input/output channels */
		case 'c':
			tp->channels = atoi(optarg);
			break;

		/* enable debug prints */
		case 'd':
			debug = 1;
			break;

		/* number of pipeline copy() iterations */
		case 'C':
			tp->copy_iterations = atoi(optarg);
			tp->copy_check = true;
			break;

		/* number of dynamic pipeline iterations */
		case 'P':
			tp->dynamic_pipeline_iterations = atoi(optarg);
			break;

		/* number of virtual cores */
		case 'V':
			tp->num_vcores = atoi(optarg);
			break;

		/* output sample files */
		case 'p':
			ret = parse_pipelines(optarg, tp);
			break;

		/* ticks per millisec, 0 = realtime (tickless) */
		case 'T':
			tp->tick_period_us = atoi(optarg);
			break;

		/* pipeline duration in millisec, 0 = realtime (tickless) */
		case 'D':
			tp->pipeline_duration_ms = atoi(optarg);
			break;

		/* use real time priorities for threads */
		case 's':
			tp->real_time = 1;
			break;

		/* print usage */
		default:
			fprintf(stderr, "unknown option %c\n", option);
			ret = -EINVAL;
			__attribute__ ((fallthrough));
		case 'h':
			print_usage(argv[0]);
			return ret;
		}

		if (ret < 0)
			return ret;
	}

	return ret;
}

static int pipline_stop(struct pipeline_thread_data *ptd, struct tplg_context *ctx)
{
	struct testbench_prm *tp = ptd->tp;
	struct ipc_comp_dev *pcm_dev;
	struct pipeline *p;

	pcm_dev = ipc_get_comp_by_id(sof_get()->ipc, tp->sched_id);
	p = pcm_dev->cd->pipeline;

	return tb_pipeline_stop(sof_get()->ipc, p, tp);
}

static int pipline_reset(struct pipeline_thread_data *ptd, struct tplg_context *ctx)
{
	struct testbench_prm *tp = ptd->tp;
	struct ipc_comp_dev *pcm_dev;
	struct pipeline *p;

	pcm_dev = ipc_get_comp_by_id(sof_get()->ipc, tp->sched_id);
	p = pcm_dev->cd->pipeline;

	return tb_pipeline_reset(sof_get()->ipc, p, tp);
}

static int pipline_start(struct pipeline_thread_data *ptd,
		       struct tplg_context *ctx)
{
	struct testbench_prm *tp = ptd->tp;
	struct ipc_comp_dev *pcm_dev;
	struct pipeline *p;

	/* Run pipeline until EOF from fileread */
	pcm_dev = ipc_get_comp_by_id(sof_get()->ipc, tp->sched_id);
	p = pcm_dev->cd->pipeline;

	/* input and output sample rate */
	if (!tp->fs_in)
		tp->fs_in = p->period * p->frames_per_sched;

	if (!tp->fs_out)
		tp->fs_out = p->period * p->frames_per_sched;

	pipeline_set_test_limits(ctx->pipeline_id, tp->copy_iterations, 0);

	/* set pipeline params and trigger start */
	if (tb_pipeline_start(sof_get()->ipc, p, tp) < 0) {
		fprintf(stderr, "error: pipeline params\n");
		return -EINVAL;
	}

	return 0;
}

static int pipline_get_state(struct pipeline_thread_data *ptd,
		       struct tplg_context *ctx)
{
	struct testbench_prm *tp = ptd->tp;
	struct ipc_comp_dev *pcm_dev;
	struct pipeline *p;

	/* Run pipeline until EOF from fileread */
	pcm_dev = ipc_get_comp_by_id(sof_get()->ipc, tp->sched_id);
	p = pcm_dev->cd->pipeline;
	return p->pipe_task->state;
}

static int pipline_load(struct pipeline_thread_data *ptd,
			struct tplg_context *ctx, int pipeline_id)
{
	struct testbench_prm *tp = ptd->tp;
	int ret;

	/* setup the thread virtual core config */
	memset(ctx, 0, sizeof(*ctx));
	ctx->comp_id = 1000 * ptd->core_id;
	ctx->core_id = ptd->core_id;
	ctx->file = tp->file;
	ctx->sof = sof_get();
	ctx->tp = tp;
	ctx->tplg_file = tp->tplg_file;
	ctx->pipeline_id = pipeline_id;

	/* parse topology file and create pipeline */
	ret = parse_topology(ctx);
	if (ret < 0) {
		fprintf(stderr, "error: parsing topology\n");
		exit(EXIT_FAILURE);
	}

	return ret;
}

static void pipline_free(struct pipeline_thread_data *ptd,
			struct tplg_context *ctx, int pipeline_id)
{
	pipeline_free_comps(pipeline_id);
}

static void pipeline_stats(struct pipeline_thread_data *ptd,
			   struct tplg_context *ctx, uint64_t delta)
{
	struct testbench_prm *tp = ptd->tp;
	int count = ptd->count;
	struct ipc_comp_dev *pcm_dev;
	struct pipeline *p;
	struct file_comp_data *frcd, *fwcd;
	int n_in, n_out;
	int i;

	/* Get pointer to filewrite */
	pcm_dev = ipc_get_comp_by_id(sof_get()->ipc, tp->fw_id);
	if (!pcm_dev) {
		fprintf(stderr, "error: failed to get pointers to filewrite\n");
		exit(EXIT_FAILURE);
	}
	fwcd = comp_get_drvdata(pcm_dev->cd);

	/* Get pointer to fileread */
	pcm_dev = ipc_get_comp_by_id(sof_get()->ipc, tp->fr_id);
	if (!pcm_dev) {
		fprintf(stderr, "error: failed to get pointers to fileread\n");
		exit(EXIT_FAILURE);
	}
	frcd = comp_get_drvdata(pcm_dev->cd);

	/* Run pipeline until EOF from fileread */
	pcm_dev = ipc_get_comp_by_id(sof_get()->ipc, tp->sched_id);
	p = pcm_dev->cd->pipeline;

	/* input and output sample rate */
	if (!tp->fs_in)
		tp->fs_in = p->period * p->frames_per_sched;

	if (!tp->fs_out)
		tp->fs_out = p->period * p->frames_per_sched;


	n_in = frcd->fs.n;
	n_out = fwcd->fs.n;

	/* print test summary */
	printf("==========================================================\n");
	printf("		           Test Summary %d\n", count);
	printf("==========================================================\n");
	printf("Test Pipeline:\n");
	printf("%s\n", tp->pipeline_string);
	pipeline_get_file_stats(ctx->pipeline_id);

	printf("Input bit format: %s\n", tp->bits_in);
	printf("Input sample rate: %d\n", tp->fs_in);
	printf("Output sample rate: %d\n", tp->fs_out);
	for (i = 0; i < tp->output_file_num; i++) {
		printf("Output[%d] written to file: \"%s\"\n",
		       i, tp->output_file[i]);
	}
	printf("Input sample (frame) count: %d (%d)\n", n_in, n_in / tp->channels);
	printf("Output sample (frame) count: %d (%d)\n", n_out, n_out / tp->channels);
	printf("Total execution time: %zu us, %.2f x realtime\n\n",
	       delta, (double) ((double)n_out / tp->channels / tp->fs_out) * 1000000 / delta);
}

/*
 * Tester thread, one for each virtual core. This is NOT the thread that will
 * execute the virtual core.
 */
static void *pipline_test(void *data)
{
	struct pipeline_thread_data *ptd = data;
	struct testbench_prm *tp = ptd->tp;
	int dp_count = 0;
	struct tplg_context ctx;
	struct timespec ts;
	struct timespec td0, td1;
	int err;
	int nsleep_time;
	uint64_t delta;

	/* build, run and teardown pipelines */
	while (dp_count < tp->dynamic_pipeline_iterations) {
		fprintf(stdout, "pipeline run %d/%d\n", dp_count,
			tp->dynamic_pipeline_iterations);

		/* print test summary */
		printf("==========================================================\n");
		printf("		           Test Start %d\n", dp_count);
		printf("==========================================================\n");

		err = pipline_load(ptd, &ctx, tp->pipelines[0]);
		if (err < 0) {
			fprintf(stderr, "error: pipeline load %d failed %d\n",
				dp_count, err);
			break;
		}

		err = pipline_start(ptd, &ctx);
		if (err < 0) {
			fprintf(stderr, "error: pipeline run %d failed %d\n",
				dp_count, err);
			break;
		}
		clock_gettime(CLOCK_MONOTONIC, &td0);

		/* sleep to let the pipeline work - we exit at timeout OR
		 * if copy iterations OR max_samples is reached (whatever first)
		 */
		nsleep_time = 0;
		ts.tv_sec = tp->tick_period_us / 1000000;
		ts.tv_nsec = (tp->tick_period_us % 1000000) * 1000;
		while (nsleep_time < tp->pipeline_duration_ms * tp->copy_iterations) {
			/* wait for next tick */
			err = nanosleep(&ts, &ts);
			if (err == 0) {
				nsleep_time += tp->tick_period_us; /* sleep fully completed */
				if (pipline_get_state(ptd, &ctx) != SOF_TASK_STATE_QUEUED)
					goto stop;
			} else if (err == EINTR)
				continue; /* interrupted - keep going */
			else {
				printf("error: sleep failed: %s\n", strerror(err));
				goto stop;
			}
		}

stop:
		clock_gettime(CLOCK_MONOTONIC, &td1);
		err = pipline_stop(ptd, &ctx);
		if (err < 0) {
			fprintf(stderr, "error: pipeline stop %d failed %d\n",
				dp_count, err);
			break;
		}

		delta = (td1.tv_sec - td0.tv_sec) * 1000000;
		delta += (td1.tv_nsec - td0.tv_nsec) / 1000;

		pipeline_stats(ptd, &ctx, delta);

		err = pipline_reset(ptd, &ctx);
		if (err < 0) {
			fprintf(stderr, "error: pipeline stop %d failed %d\n",
				dp_count, err);
			break;
		}

		pipline_free(ptd, &ctx, tp->pipelines[0]);

		ptd->count++;
		dp_count++;
	}

	return NULL;
}

int main(int argc, char **argv)
{
	struct testbench_prm tp;
	struct pipeline_thread_data ptd[CACHE_VCORE_COUNT];
	int i, err;
	pthread_attr_t attr;
	struct sched_param param;

	/* initialize input and output sample rates, files, etc. */
	tp.fs_in = 0;
	tp.fs_out = 0;
	tp.bits_in = 0;
	tp.input_file = NULL;
	tp.tplg_file = NULL;
	tp.real_time = 0;
	for (i = 0; i < MAX_OUTPUT_FILE_NUM; i++)
		tp.output_file[i] = NULL;
	tp.output_file_num = 0;
	tp.channels = TESTBENCH_NCH;
	tp.max_pipeline_id = 0;
	tp.copy_check = false;
	tp.dynamic_pipeline_iterations = 1;
	tp.num_vcores = 0;
	tp.pipeline_string = calloc(1, DEBUG_MSG_LEN);
	tp.pipelines[0] = 1;
	tp.pipeline_num = 1;
	tp.tick_period_us = 1000;
	tp.pipeline_duration_ms = 5000;
	tp.copy_iterations = 1;

	/* command line arguments*/
	err = parse_input_args(argc, argv, &tp);
	if (err < 0)
		goto out;

	/* check mandatory args */
	if (!tp.tplg_file) {
		fprintf(stderr, "topology file not specified, use -t file.tplg\n");
		print_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	if (!tp.input_file) {
		fprintf(stderr, "input audio file not specified, use -i file\n");
		print_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	if (!tp.output_file_num) {
		fprintf(stderr, "output files not specified, use -o file1,file2\n");
		print_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	if (!tp.bits_in) {
		fprintf(stderr, "input format not specified, use -b format\n");
		print_usage(argv[0]);
		exit(EXIT_FAILURE);
	}

	if (tp.num_vcores > CACHE_VCORE_COUNT) {
		fprintf(stderr, "virtual core count %d is greater than max %d\n",
				tp.num_vcores, CACHE_VCORE_COUNT);
		print_usage(argv[0]);
		exit(EXIT_FAILURE);
	} else if (!tp.num_vcores)
		tp.num_vcores = 1;

	tb_enable_trace(true);

	/* initialize ipc and scheduler */
	if (tb_setup(sof_get(), &tp) < 0) {
		fprintf(stderr, "error: pipeline init\n");
		exit(EXIT_FAILURE);
	}

	/* build, run and teardown pipelines */
	for (i = 0; i < tp.num_vcores; i++) {
		ptd[i].core_id = i;
		ptd[i].tp = &tp;
		ptd[i].count = 0;

		if (tp.real_time) {
			err = pthread_attr_init(&attr);
			if (err) {
				printf("error: can't create thread attr %d %s\n",
				       err, strerror(err));
				goto join;
			}

			err = pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
			if (err) {
				printf("error: can't set thread policy %d %s\n",
					err, strerror(err));
				goto join;
			}
			param.sched_priority = 80;
			err = pthread_attr_setschedparam(&attr, &param);
			if (err) {
				printf("error: can't set thread sched param %d %s\n",
					err, strerror(err));
				goto join;
			}
			err = pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
			if (err) {
				printf("error: can't set thread inherit %d %s\n",
					err, strerror(err));
				goto join;
			}
		}
		err = pthread_create(&hc.thread_id[i],
				     tp.real_time ? &attr : NULL,
				     pipline_test, &ptd[i]);
		if (err) {
			printf("error: can't create thread %d %s\n",
				err, strerror(err));
			goto join;
		}
	}

join:
	for (i = 0; i < tp.num_vcores; i++)
		err = pthread_join(hc.thread_id[i], NULL);

	/* free other core FW services */
	tb_free(sof_get());

out:
	/* free all other data */
	free(tp.bits_in);
	free(tp.input_file);
	free(tp.tplg_file);
	for (i = 0; i < tp.output_file_num; i++)
		free(tp.output_file[i]);
	free(tp.pipeline_string);

#ifdef TESTBENCH_CACHE_CHECK
	_cache_free_all();
#endif

	/* close shared library objects */
	for (i = 0; i < NUM_WIDGETS_SUPPORTED; i++) {
		if (lib_table[i].handle)
			dlclose(lib_table[i].handle);
	}

	return EXIT_SUCCESS;
}
