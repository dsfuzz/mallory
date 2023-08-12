#include "../include/types.h"
#include "../include/config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/shm.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <pthread.h>

#define CONST_PRIO 0

const u64 BLOCK_EVENT_TYPE = 1;
const u64 FUNC_EVENT_TYPE = 2;

#pragma pack(8) // 8-byte memory alignment
/** Event entry **/
struct Event
{
  union
  {
    struct
    {
      u16 evtCounter;
      u64 batchNumber;
    };
    struct
    {
      u64 bevtType; // 1: basicblock
      s64 btimestamp;
      u64 bevtID;
    };
    struct
    {
      u64 fevtType; // 2: function
      s64 ftimestamp;
      u64 fevtID;
    };
    struct
    {
      u64 evtType; // 3 = packet send; 4 = packet receive
      s64 timestamp;
      u64 evtID; // a mostly-unique identifier for the packet
    };
  };
};

struct Event evtVec[EVT_SIZE];
struct Event *evtVec_ptr = evtVec; // evtID in idx_0: to record evt counter

// For saving AFL feedback
u8 __afl_area_initial[MAP_SIZE];
u8 *__afl_area_ptr = __afl_area_initial;

__thread u32 __afl_prev_loc;

/***
 * instrument bb starting point
 ***/
void track_blocks(u16 evtID)
{
  /* find location to record this event */
  u16 loc = __atomic_add_fetch(&evtVec_ptr[0].evtCounter, 1, __ATOMIC_RELAXED);

  /* collect tid and timestamp */
  struct timespec st;
  clock_gettime(CLOCK_MONOTONIC, &st);
  s64 time = st.tv_sec * 1000000000 + st.tv_nsec;

  /* record this event */
  evtVec_ptr[loc].bevtType = BLOCK_EVENT_TYPE;
  evtVec_ptr[loc].bevtID = evtID;
  evtVec_ptr[loc].btimestamp = time;
}

/***
 * instrument functions starting point
 ***/
void track_functions(u16 evtID)
{
  /* find location to record this event */
  u16 loc = __atomic_add_fetch(&evtVec_ptr[0].evtCounter, 1, __ATOMIC_RELAXED);

  /* collect tid and timestamp */
  struct timespec st;
  clock_gettime(CLOCK_MONOTONIC, &st);
  s64 time = st.tv_sec * 1000000000 + st.tv_nsec;

  /* record this event */
  evtVec_ptr[loc].fevtType = FUNC_EVENT_TYPE;
  evtVec_ptr[loc].fevtID = evtID;
  evtVec_ptr[loc].ftimestamp = time;
}

void init_shm_dsfuzz()
{
  FILE *fp = NULL;
  fp = fopen("/opt/shm/dsfuzz_shm_id", "r");

  if (fp)
  {
    u8 shm_id_str[4]; // key type: int for C/C++
    if (fscanf(fp, "%s", shm_id_str) != EOF)
    {
      u32 shm_id = atoi(shm_id_str);
      evtVec_ptr = (struct Event *)shmat(shm_id, NULL, 0);
      if (evtVec_ptr == (void *)-1)
      {
        perror("[FAILED] shmat");
        fprintf(stderr, "[!!!] DS shared memory error: subject fails to access shared memory\n");
        _exit(1);
      }
    }
    fclose(fp);
  }
  else
  {
    fprintf(stderr, "[!!!] DS shared memory warning: subject is not running under the coverage server\n");
  }
}

void init_shm_afl()
{
  FILE *fp = NULL;
  fp = fopen("/opt/shm/afl_shm_id", "r");

  if (fp)
  {
    u8 shm_id_str[4]; // key type: int for C/C++
    if (fscanf(fp, "%s", shm_id_str) != EOF)
    {
      u32 shm_id = atoi(shm_id_str);
      __afl_area_ptr = shmat(shm_id, NULL, 0);
      if (__afl_area_ptr == (void *)-1)
      {
        perror("[FAILED] shmat");
        fprintf(stderr, "[!!!] AFL shared memory error: Subject fails to access shared memory\n");
        _exit(1);
      }
    }
    fclose(fp);
  }
  else
  {
    fprintf(stderr, "[!!!] AFL shared memory warning: Subject is not running under the coverage server\n");
  }
}

__attribute__((constructor(CONST_PRIO))) void __afl_auto_init(void)
{
  init_shm_afl();
  init_shm_dsfuzz();
}

/* The following stuff deals with supporting -fsanitize-coverage=trace-pc-guard.
   It remains non-operational in the traditional, plugin-backed LLVM mode.

   The first function (__sanitizer_cov_trace_pc_guard) is called back on every
   edge (as opposed to every basic block). */

void __sanitizer_cov_trace_pc_guard(uint32_t *guard)
{
  __afl_area_ptr[*guard]++;
}

/* Init callback. Populates instrumentation IDs. Note that we're using
   ID of 0 as a special value to indicate non-instrumented bits. That may
   still touch the bitmap, but in a fairly harmless way. */

void __sanitizer_cov_trace_pc_guard_init(uint32_t *start, uint32_t *stop)
{
  u32 inst_ratio = 100;
  u8 *x;

  if (start == stop || *start)
    return;

  x = getenv("AFL_INST_RATIO");
  if (x)
    inst_ratio = atoi(x);

  if (!inst_ratio || inst_ratio > 100)
  {
    fprintf(stderr, "[-] ERROR: Invalid AFL_INST_RATIO (must be 1-100).\n");
    abort();
  }

  /* Make sure that the first element in the range is always set - we use that
     to avoid duplicate calls (which can happen as an artifact of the underlying
     implementation in LLVM). */

  *(start++) = AFL_RR(MAP_SIZE - 1) + 1;

  while (start < stop)
  {

    if (AFL_RR(100) < inst_ratio)
      *start = AFL_RR(MAP_SIZE - 1) + 1;
    else
      *start = 0;

    start++;
  }
}