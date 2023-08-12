/*
  Copyright 2015 Google LLC All rights reserved.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at:

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

/*
   american fuzzy lop - LLVM-mode instrumentation pass
   ---------------------------------------------------

   Written by Laszlo Szekeres <lszekeres@google.com> and
              Michal Zalewski <lcamtuf@google.com>

   LLVM integration design comes from Laszlo Szekeres. C bits copied-and-pasted
   from afl-as.c are Michal's fault.

   This library is plugged into LLVM when invoking clang through afl-clang-fast.
   It tells the compiler to add code roughly equivalent to the bits discussed
   in ../afl-as.h.
*/

#define AFL_LLVM_PASS

#include "../include/config.h"
#include "../include/debug.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <set>
#include <sys/shm.h>
#include <cxxabi.h>

#include "llvm/ADT/Statistic.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/Debug.h"
#include "llvm/IR/DebugInfoMetadata.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"

#include "../rustc-demangle/crates/capi/include/rustc_demangle.h"

using namespace llvm;

#define TARGETS_TYPE std::unordered_map<std::string, std::set<int>>

namespace
{

  class AFLCoverage : public ModulePass
  {

  public:
    static char ID;
    AFLCoverage() : ModulePass(ID) {}

    bool runOnModule(Module &M) override;

    // Global variables
    GlobalVariable *AFLMapPtr;
    GlobalVariable *AFLPrevLoc;
    unsigned int inst_ratio;

    // Types
    Type *VoidTy;
    PointerType *Int8PtrTy;
    IntegerType *Int8Ty;
    IntegerType *Int16Ty;
    IntegerType *Int32Ty;
    IntegerType *Int64Ty;
    PointerType *Int64PtrTy;

    // Store mapping data from basicblock location to ID
    std::ofstream bbToID;

    u16 *get_ID_ptr();
    static void get_debug_loc(const Instruction *I, std::string &Filename, unsigned &Line);
    static void load_instr_targets(TARGETS_TYPE &bb_targets, TARGETS_TYPE &func_targets);

    // -1: not checking, 0: not targets, 1: target BBs, 2: target functions
    static u8 is_target_loc(std::string codefile, unsigned line, TARGETS_TYPE &bb_targets, TARGETS_TYPE &func_targets);

    u8 check_code_language(std::string codefile);
    void printFuncLog(std::string filename, unsigned line, u16 evtID, std::string func_name);
    void printBBLog(std::string filename, unsigned line, u16 evtID);
  };

}

/***
 * Load identified interesting basicblocks(targets) to instrument
 ***/
void AFLCoverage::load_instr_targets(TARGETS_TYPE &bb_targets, TARGETS_TYPE &func_targets)
{
  char *target_file = getenv("TARGETS_FILE");
  if (!target_file)
  {
    target_file = (char *)"/opt/instrumentor/instr-targets.txt";
  }

  std::ifstream targetsfile(target_file);
  if (!targetsfile.is_open())
  {
    outs() << "[!!] Fail to open targets file"
           << "\n";
    return;
  }

  std::string line;
  std::string codefile;
  int num = 0;
  while (std::getline(targetsfile, line))
  {
    if (num % 3 == 0)
    {
      codefile = line;
    }
    else if (num % 3 == 1)
    {
      std::stringstream ss(line);
      std::string item;
      while (std::getline(ss, item, ' '))
      {
        bb_targets[codefile].insert(std::stoi(item));
      }
    }
    else
    {
      std::stringstream ss(line);
      std::string item;
      while (std::getline(ss, item, ' '))
      {
        func_targets[codefile].insert(std::stoi(item));
      }
    }
    ++num;
  }
  targetsfile.close();
}

/***
 * Check if current location is target: 1 for BB, 2 for function, 0 for not targets, -1 for not checking
 ***/
u8 AFLCoverage::is_target_loc(std::string codefile, unsigned line, TARGETS_TYPE &bb_targets, TARGETS_TYPE &func_targets)
{
  if (bb_targets.count(codefile))
  {
    std::set<int> locs = bb_targets[codefile];
    for (auto ep = locs.begin(); ep != locs.end(); ep++)
    {
      if (*ep == line)
      {
        bb_targets[codefile].erase(line);
        return 1;
      }
    }
  }

  if (func_targets.count(codefile))
  {
    std::set<int> locs = func_targets[codefile];
    for (auto ep = locs.begin(); ep != locs.end(); ep++)
    {
      if (*ep == line)
      {
        func_targets[codefile].erase(line);
        return 2;
      }
    }
  }

  return 0;
}

/***
 * Get filename and location given one instruction
 ***/
void AFLCoverage::get_debug_loc(const Instruction *I, std::string &Filename, unsigned &Line)
{
  if (DILocation *Loc = I->getDebugLoc())
  {
    Line = Loc->getLine();
    Filename = Loc->getFilename().str();

    char *path = realpath(Filename.c_str(), NULL);
    if (path)
    {
      Filename = std::string(path);
    }
    else
    {
      std::string dir = Loc->getDirectory().str();
      if (dir.size() > 0)
      {
        Filename = dir + "/" + Filename;
      }
    }

    if (Filename.empty())
    {
      DILocation *oDILoc = Loc->getInlinedAt();
      if (oDILoc)
      {
        Line = oDILoc->getLine();
        Filename = oDILoc->getFilename().str();
        char *path = realpath(Filename.c_str(), NULL);
        if (path)
        {
          Filename = std::string(path);
        }
        else
        {
          std::string dir = Loc->getDirectory().str();
          if (dir.size() > 0)
          {
            Filename = dir + "/" + Filename;
          }
        }
      }
    }
  }
}

/***
 * Assign event ID in increasing order, instead of random assignment in AFL
 ***/
u16 *AFLCoverage::get_ID_ptr()
{
  // Create the shared memory if it does not exist, otherwise get the existing one
  int shmid = shmget((key_t)SHM_ID_KEY, sizeof(u16), IPC_CREAT | IPC_EXCL | 0666);
  if (shmid != 0)
  {
    shmid = shmget((key_t)SHM_ID_KEY, sizeof(u16), 0666);
  }

  // FIXME: If compilation is done in Docker (in subsequent RUN layers), the
  // shared memory is not carried over between layers, so IDs will conflict

  u16 *_id_ptr;
  if (shmid >= 0)
  {
    _id_ptr = (u16 *)shmat(shmid, NULL, 0);
    if (_id_ptr == (u16 *)-1)
    {
      ABORT("!!! shared memory error: fail to connect");
      _exit(1);
    }
    return _id_ptr;
  }
  else
  {
    ABORT("!!! shared memory error: fail to create");
    _exit(1);
  }
}

u8 AFLCoverage::check_code_language(std::string codefile)
{
  // Check if the code is written in Rust (return 1) or C/C++ (return 2)
  if (codefile.find(".rs") != std::string::npos)
  {
    return 1;
  }
  else
  {
    return 2;
  }
}

/***
 * Print compilation log
 ***/
void AFLCoverage::printFuncLog(std::string filename, unsigned line, u16 evtID, std::string func_name)
{
  OKF("Instrument %u at %s: at line %u for function %s", evtID, filename.c_str(), line, func_name.c_str());
  bbToID << evtID << ": at " << filename << " ; at line " << line << " for function " << func_name << std::endl;
}

void AFLCoverage::printBBLog(std::string filename, unsigned line, u16 evtID)
{
  bbToID << evtID << ": at " << filename << " ; at line " << line << " for block" << std::endl;
  OKF("Instrument %u at %s: at line %u for block", evtID, filename.c_str(), line);
}

char AFLCoverage::ID = 0;

bool AFLCoverage::runOnModule(Module &M)
{

  LLVMContext &C = M.getContext();

  VoidTy = Type::getVoidTy(C);
  Int8PtrTy = Type::getInt8PtrTy(C);
  Int8Ty = IntegerType::getInt8Ty(C);
  Int16Ty = IntegerType::getInt16Ty(C);
  Int32Ty = IntegerType::getInt32Ty(C);
  Int64Ty = IntegerType::getInt64Ty(C);
  Int64PtrTy = Type::getInt64PtrTy(C);

  bbToID.open("/opt/instrumentor/BB2ID.txt", std::ofstream::out | std::ofstream::app);
  if (!bbToID.is_open())
  {
    bbToID.open("./BB2ID.txt", std::ofstream::out | std::ofstream::app);
  }

  /* Show a banner */

  // char be_quiet = 0;

  if (isatty(2) && !getenv("AFL_QUIET"))
  {
    SAYF(cCYA "afl-llvm-pass " cBRI VERSION cRST " by <lszekeres@google.com>\n");
  }

  /* Decide the size of instrumented functions */
  char *instr_func_size_str = getenv("INST_FUNC_SIZE");

  // Set one large number to disable it if the environment variable is not set
  unsigned int instr_func_size = 65536;
  if (instr_func_size_str)
  {
    if (sscanf(instr_func_size_str, "%u", &instr_func_size) != 1)
      FATAL("Bad value of INST_FUNC_SIZE");
  }

  if (getenv("USE_TRADITIONAL_BRANCH")){
    /* Decide instrumentation ratio */
    char *inst_ratio_str = getenv("AFL_INST_RATIO");
    inst_ratio = 100;
    if (inst_ratio_str)
    {
      if (sscanf(inst_ratio_str, "%u", &inst_ratio) != 1 || !inst_ratio ||
          inst_ratio > 100)
        FATAL("Bad value of AFL_INST_RATIO (must be between 1 and 100)");
    }

    /* Get globals for the SHM region and the previous location. Note that
      __afl_prev_loc is thread-local. */
    AFLMapPtr =
        new GlobalVariable(M, PointerType::get(Int8Ty, 0), false,
                          GlobalValue::ExternalLinkage, 0, "__afl_area_ptr");

    AFLPrevLoc = new GlobalVariable(
        M, Int32Ty, false, GlobalValue::ExternalLinkage, 0, "__afl_prev_loc",
        0, GlobalVariable::GeneralDynamicTLSModel, 0, false);
  }

  int inst_blocks = 0;
  TARGETS_TYPE bb_targets, func_targets;
  load_instr_targets(bb_targets, func_targets);
  u8 codeLang = 0;

  static const std::string Xlibs("/usr/");
  // static const std::string Clibs("/rustc/");

  for (auto &F : M)
  {
    // Label if this function is instrumented
    bool isTargetFunc = false;

    std::string filename;
    unsigned line = 0;

    for (auto &BB : F)
    {

      BasicBlock::iterator IP = BB.getFirstInsertionPt();

      // in each basic block, check if it is a target
      bool isTargetBB = false;

      for (auto &I : BB)
      {

        get_debug_loc(&I, filename, line);

        if (filename.empty() || line == 0 || !filename.compare(0, Xlibs.size(), Xlibs))
        {
          continue;
        }
        // printf("filename: %s, line: %u\n", filename.c_str(), line);

        /* check if target locations */
        u8 isTarget = is_target_loc(filename, line, bb_targets, func_targets);
        if (isTarget == 1)
        {
          isTargetBB = true;
        }
        else if (isTarget == 2)
        {
          isTargetFunc = true;
        }
      }

      /* skip if no target found or instrumented, and also not selected */
      if (!isTargetBB && AFL_R(100) >= inst_ratio)
      {
        continue;
      }

      /* instrument starting block point */
      IRBuilder<> IRB(&(*IP));
      if (isTargetBB)
      {
        u16 *evtIDPtr = get_ID_ptr();
        u16 evtID = *evtIDPtr;
        Value *evtValue = ConstantInt::get(Int16Ty, evtID);

        auto *helperTy_stack = FunctionType::get(VoidTy, Int16Ty);
        auto helper_stack_start = M.getOrInsertFunction("track_blocks", helperTy_stack);

        IRB.CreateCall(helper_stack_start, {evtValue});

        /* store BB ID info */
        printBBLog(filename, line, evtID);

        /* increase counter */
        *evtIDPtr = ++evtID;
      }

      if (getenv("USE_TRADITIONAL_BRANCH")){
        // Instrument all basicblocks to compute AFL feedback
        unsigned int cur_loc = AFL_R(MAP_SIZE);

        ConstantInt *CurLoc = ConstantInt::get(Int32Ty, cur_loc);

        /* Load prev_loc */

        LoadInst *PrevLoc = IRB.CreateLoad(AFLPrevLoc);
        PrevLoc->setMetadata(M.getMDKindID("nosanitize"), MDNode::get(C, None));
        Value *PrevLocCasted = IRB.CreateZExt(PrevLoc, IRB.getInt32Ty());

        /* Load SHM pointer */

        LoadInst *MapPtr = IRB.CreateLoad(AFLMapPtr);
        MapPtr->setMetadata(M.getMDKindID("nosanitize"), MDNode::get(C, None));
        Value *MapPtrIdx =
            IRB.CreateGEP(MapPtr, IRB.CreateXor(PrevLocCasted, CurLoc));

        /* Update bitmap */

        LoadInst *Counter = IRB.CreateLoad(MapPtrIdx);
        Counter->setMetadata(M.getMDKindID("nosanitize"), MDNode::get(C, None));
        Value *Incr = IRB.CreateAdd(Counter, ConstantInt::get(Int8Ty, 1));
        IRB.CreateStore(Incr, MapPtrIdx)
            ->setMetadata(M.getMDKindID("nosanitize"), MDNode::get(C, None));

        /* Set prev_loc to cur_loc >> 1 */

        StoreInst *Store =
            IRB.CreateStore(ConstantInt::get(Int32Ty, cur_loc >> 1), AFLPrevLoc);
        Store->setMetadata(M.getMDKindID("nosanitize"), MDNode::get(C, None));
      }

      inst_blocks++;
    }

    /* Instrument function if it is one target or the size is above threshold */
    // if (isTargetFunc || F.getInstructionCount() > instr_func_size)
    if (isTargetFunc)
    {
      /* get inserting point: first inserting point of the entry block */
      BasicBlock *BB = &F.getEntryBlock();
      Instruction *InsertPoint = &(*(BB->getFirstInsertionPt()));
      IRBuilder<> IRB(InsertPoint);

      /* get evt ID */
      u16 *evtIDPtr = get_ID_ptr();
      u16 evtID = *evtIDPtr;
      Value *evtValue = ConstantInt::get(Int16Ty, evtID);

      auto *helperTy_func = FunctionType::get(VoidTy, Int16Ty);
      auto helper_func = M.getOrInsertFunction("track_functions", helperTy_func);
      IRB.CreateCall(helper_func, {evtValue});

      /* store event ID info */
      get_debug_loc(&(*InsertPoint), filename, line);
      std::string func_name = F.getName().str();
      if (codeLang == 0)
      {
        codeLang = check_code_language(filename);
      }

      if (codeLang == 2)
      {
        int demangled_status = -1;
        char *demangled_char = abi::__cxa_demangle(F.getName().data(), nullptr,
                                                   nullptr, &demangled_status);
        if (demangled_status == 0)
        {
          func_name = demangled_char;
        }
      }
      else
      {
        int demangled_status = -1;
        char *demangled_char = rustc_demangle(F.getName().data(), &demangled_status);
        if (demangled_status == 0)
        {
          func_name = demangled_char;
        }
      }
      printFuncLog(filename, line, evtID, func_name);

      /* increase counter */
      *evtIDPtr = ++evtID;
      inst_blocks++;
    }
  }

  return true;
}

static void registerAFLPass(const PassManagerBuilder &,
                            legacy::PassManagerBase &PM)
{

  PM.add(new AFLCoverage());
}

static RegisterStandardPasses RegisterAFLPass(
    PassManagerBuilder::EP_ModuleOptimizerEarly, registerAFLPass);

static RegisterStandardPasses RegisterAFLPass0(
    PassManagerBuilder::EP_EnabledOnOptLevel0, registerAFLPass);
