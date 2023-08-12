#include <stdlib.h>
#include <stdio.h>
#include <sys/shm.h>
#include "../include/types.h"
#include "../include/config.h"

int main(){
    int shmid = shmget((key_t)SHM_ID_KEY, sizeof(u16), IPC_CREAT | 0600);
    u16* _id_ptr;
    if(shmid >= 0){
        _id_ptr = (u16*)shmat(shmid, NULL, 0);
        if(_id_ptr == (u16*)-1){
            printf(">>> fail to connect\n");
        }
        *_id_ptr = 0;
    }
    else{
        printf(">>> fail to create\n");
    }
}