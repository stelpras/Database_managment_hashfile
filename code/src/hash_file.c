#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20


extern int *open_files;

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

HT_ErrorCode HT_Init() {
  //insert code here
  open_files = malloc(MAX_OPEN_FILES * sizeof(int));
  for (int i = 0; i < MAX_OPEN_FILES; i++)
    open_files[i] = -1;
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
  //insert code here
  int file_desc, num, records_per_block, block_num = 0;
  BF_Block *first_block, *block;
  char *first_block_data, *block_data;

  CALL_BF(BF_CreateFile(filename));
  CALL_BF(BF_OpenFile(filename, &file_desc));

  // first block
  BF_Block_Init(&first_block); // at the first block we save information about the file
  CALL_BF(BF_AllocateBlock(file_desc, first_block));
  CALL_BF(BF_GetBlock(file_desc, block_num, first_block));
  first_block_data = BF_Block_GetData(first_block);

  memcpy(first_block_data, "hash", 5 * sizeof(char));                                         // the first 5 bytes have the word "hash" so we know it's a hashfile
  memcpy(first_block_data + 5 * sizeof(char), &depth, sizeof(int));                           // then we save the number of buckets we are using in the hashfile
  records_per_block = (BF_BLOCK_SIZE - 2 * sizeof(int)) / sizeof(Record);                     // At each block we will save 2 (int) numbers, one for the number of records at the block and one in case we want to use overflow chains
  memcpy(first_block_data + 5 * sizeof(char) + sizeof(int), &records_per_block, sizeof(int)); // in the end of the block we save the number of records we can save in a block

  // Update the number of records in the first block
  memcpy(first_block_data + BF_BLOCK_SIZE - 2 * sizeof(int), &records_per_block, sizeof(int));
  
  BF_Block_SetDirty(first_block);
  CALL_BF(BF_UnpinBlock(first_block));

  // hash index blocks
  num = -1;
  int buckets_per_block = BF_BLOCK_SIZE / sizeof(int);
  for (int i = 0; i < depth; i++)
  { // We want to initialize every bucket with the number -1
    if (i % buckets_per_block == 0)
    { // In this case we want to use the next hash_block
      block_num++;
      BF_Block_Init(&block);
      CALL_BF(BF_AllocateBlock(file_desc, block));
      CALL_BF(BF_GetBlock(file_desc, block_num, block));
      block_data = BF_Block_GetData(block);
    }

    memcpy(block_data + (i % buckets_per_block) * sizeof(int), &num, sizeof(int)); // (i%ints_per_block) is the number of buckets before the bucket we want to initialize
    BF_Block_SetDirty(block);

    if ((i % buckets_per_block == buckets_per_block - 1) && i != depth - 1) // Unpin the block if the bucket is the last one that fits in the block, except it's the last one we want to initialize
      CALL_BF(BF_UnpinBlock(block));
  }

  CALL_BF(BF_UnpinBlock(block));
  CALL_BF(BF_CloseFile(file_desc));

  return HT_OK;
}


HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  //insert code here
  int file_desc;
  BF_Block *first_block;
  char *first_block_data;

  CALL_BF(BF_OpenFile(fileName, &file_desc));

  // We take the information from the first block
  BF_Block_Init(&first_block);
  CALL_BF(BF_GetBlock(file_desc, 0, first_block));
  first_block_data = BF_Block_GetData(first_block);
  CALL_BF(BF_UnpinBlock(first_block));

  if (strcmp(first_block_data, "hash") != 0)
  { // check if the file is a hashfile
    printf("This is not a hashfile\n");
    return HT_ERROR;
  }

  for (int i = 0; i < MAX_OPEN_FILES; i++)
  {
    if (open_files[i] == -1)
    {                            // find the first empty index in the open_files table
      open_files[i] = file_desc; // save the file_desc in the table
      *indexDesc = i;            // return the indexDesc
      return HT_OK;
    }
  }
  printf("You have reached maximum open files capacity\n");

  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  //insert code here
  int file_desc = open_files[indexDesc];
  if (file_desc == -1)
  {
    printf("There is no open file in this index\n");
    return HT_ERROR;
  }

  CALL_BF(BF_CloseFile(file_desc));
  open_files[indexDesc] = -1; // free this position of the table
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
    int file_desc, buckets, records_per_block, hashcode, bucket, buckets_per_block, number_of_records, next_block;
    BF_Block *first_block, *hash_block, *block;
    char *first_block_data, *hash_block_data, *block_data;

    file_desc = open_files[indexDesc];
    if (file_desc == -1) {
        printf("There is no open file in this index\n");
        return HT_ERROR;
    }

    BF_Block_Init(&first_block);
    CALL_BF(BF_GetBlock(file_desc, 0, first_block));
    first_block_data = BF_Block_GetData(first_block);
    CALL_BF(BF_UnpinBlock(first_block));

    memcpy(&buckets, first_block_data + 5 * sizeof(char), sizeof(int));
    memcpy(&records_per_block, first_block_data + 9 * sizeof(char), sizeof(int)); // sizeof(int)=4*sizeof(char)

    hashcode = record.id % buckets;
    buckets_per_block = BF_BLOCK_SIZE / sizeof(int);

    BF_Block_Init(&hash_block);
    CALL_BF(BF_GetBlock(file_desc, (hashcode / buckets_per_block) + 1, hash_block));
    hash_block_data = BF_Block_GetData(hash_block);

    memcpy(&bucket, hash_block_data + (hashcode % buckets_per_block) * sizeof(int), sizeof(int));

    BF_Block_Init(&block);
    if (bucket == -1) { // In this case, the bucket is empty, so we insert a new block
        BF_Block_Init(&block);
        CALL_BF(BF_AllocateBlock(file_desc, block));
        CALL_BF(BF_GetBlockCounter(file_desc, &bucket));
        bucket--; // bucket is now the number of the block we allocated
        memcpy(hash_block_data + (hashcode % buckets_per_block) * sizeof(int), &bucket, sizeof(int)); // save the number of the block in the hashblock
        BF_Block_SetDirty(hash_block);

        CALL_BF(BF_GetBlock(file_desc, bucket, block)); // we initialize the information in the block
        block_data = BF_Block_GetData(block);
        next_block = -1;
        memcpy(block_data, &next_block, sizeof(int)); // initialize next_block as -1 because we haven't created an overflow chain yet
        number_of_records = 0;
        memcpy(block_data + sizeof(int), &number_of_records, sizeof(int));
        BF_Block_SetDirty(block);
    } else { // if the bucket isn't empty, take the first block of the chain
        CALL_BF(BF_GetBlock(file_desc, bucket, block));
        block_data = BF_Block_GetData(block);
    }
    BF_UnpinBlock(hash_block);

    memcpy(&next_block, block_data, sizeof(int));
    while (next_block != -1) { // now we want to find the last block of the chain
        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Init(&block);
        CALL_BF(BF_GetBlock(file_desc, next_block, block));
        block_data = BF_Block_GetData(block);
        memcpy(&next_block, block_data, sizeof(int));
    }

    memcpy(&number_of_records, block_data + sizeof(int), sizeof(int));
    if (number_of_records == records_per_block) { // If the last block is full, add a new one in the chain
        BF_Block *new_block;
        char *new_block_data;

        BF_Block_Init(&new_block);
        CALL_BF(BF_AllocateBlock(file_desc, new_block));

        CALL_BF(BF_GetBlockCounter(file_desc, &next_block));
        next_block--;
        memcpy(block_data, &next_block, sizeof(int));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));

        CALL_BF(BF_GetBlock(file_desc, next_block, new_block));
        new_block_data = BF_Block_GetData(new_block);

        next_block = -1;
        memcpy(new_block_data, &next_block, sizeof(int));
        number_of_records = 0;
        memcpy(new_block_data + sizeof(int), &number_of_records, sizeof(int));

        BF_Block_SetDirty(new_block);
        CALL_BF(BF_UnpinBlock(new_block));

        BF_Block_Init(&block); // block is now the last block
        int last_block;
        CALL_BF(BF_GetBlockCounter(file_desc, &last_block));
        last_block--;
        CALL_BF(BF_GetBlock(file_desc, last_block, block));
        block_data = BF_Block_GetData(block);
    }

    memcpy(block_data + (number_of_records * sizeof(Record)) + 2 * sizeof(int), &record, sizeof(Record)); // save the record in the last empty position of the block
    number_of_records++;
    memcpy(block_data + sizeof(int), &number_of_records, sizeof(int)); // also, we increase the number_of_records in the block by one
    // for debbuging
    // printf("Inserted record with id: %d\n", record.id);
    // printf("Number of records in the block: %d\n", number_of_records);
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    return HT_OK;
}


HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
    // Insert code here
    int file_desc, buckets, records_per_block, hashcode, record_id, num_of_hashblocks, bucket, buckets_per_block, number_of_records, next_block;
    BF_Block *first_block, *hash_block, *block;
    char *first_block_data, *hash_block_data, *block_data, *current_record;
    Record record;

    // check if there is an open file with given file_desc
    file_desc = open_files[indexDesc];
    if (file_desc == -1)
    {
      printf("There is no open file in this index\n");
      return HT_ERROR;
    }

    BF_Block_Init(&first_block);
    CALL_BF(BF_GetBlock(file_desc, 0, first_block)); // first, we take the information from the first block to get records_per_block and buckets information
    first_block_data = BF_Block_GetData(first_block);
    CALL_BF(BF_UnpinBlock(first_block));

    memcpy(&buckets, first_block_data + 5 * sizeof(char), sizeof(int));
    memcpy(&records_per_block, first_block_data + 9 * sizeof(char), sizeof(int)); // sizeof(int) = 4 * sizeof(char)

    buckets_per_block = BF_BLOCK_SIZE / sizeof(int); // calculate buckets_per_block

    if (id == NULL)
    { // id == NULL case

      num_of_hashblocks = buckets / buckets_per_block + 1; // calculate hash blocks to work in

      for (int k = 1; k <= num_of_hashblocks; k++)
      { // all the hash blocks

        BF_Block_Init(&hash_block);
        CALL_BF(BF_GetBlock(file_desc, k, hash_block));
        hash_block_data = BF_Block_GetData(hash_block);

        for (int i = 0; i < buckets_per_block; i++)
        { // for every bucket in the current hash block
          if (i + buckets_per_block * (k - 1) == buckets)
            break;

          memcpy(&next_block, hash_block_data + (i % buckets_per_block) * sizeof(int), sizeof(int));

          BF_Block_Init(&block);
          while (next_block != -1)
          { // for every block

            CALL_BF(BF_GetBlock(file_desc, next_block, block));
            block_data = BF_Block_GetData(block);

            memcpy(&next_block, block_data, sizeof(int));
            block_data += sizeof(int); // increase block data every next block
            memcpy(&number_of_records, block_data, sizeof(int));
            block_data += sizeof(int);

            current_record = block_data;
            // print every entry for every record
            for (int j = 0; j < number_of_records; j++, current_record += sizeof(Record))
            {
              memcpy(&record, current_record, sizeof(Record));
              printf("%d, %s, %s, %s\n", record.id, record.name, record.surname, record.city);
            }
            CALL_BF(BF_UnpinBlock(block));
          }
        }
        CALL_BF(BF_UnpinBlock(hash_block));
      }
      return HT_OK;
    }
    else
    {
      hashcode = *id % buckets;
      BF_Block_Init(&hash_block);
      CALL_BF(BF_GetBlock(file_desc, (hashcode / buckets_per_block) + 1, hash_block)); // (hashcode / buckets_per_block) + 1 the number of the block that has the bucket with this hashcode
      hash_block_data = BF_Block_GetData(hash_block);

      memcpy(&bucket, hash_block_data + (hashcode % buckets_per_block) * sizeof(int), sizeof(int));

      // checks if there is a record with the given id
      if (bucket == -1)
      {
        printf("There is no record with id = %d\n", *id);
        return HT_ERROR;
      }

      BF_Block_Init(&block);
      next_block = bucket;

      do
      {
        CALL_BF(BF_GetBlock(file_desc, next_block, block));
        block_data = BF_Block_GetData(block);
        memcpy(&next_block, block_data, sizeof(int));
        block_data += sizeof(int);
        memcpy(&number_of_records, block_data, sizeof(int));
        block_data += sizeof(int);

        current_record = block_data;
        for (int i = 0; i < number_of_records; i++, current_record += sizeof(Record))
        {
          memcpy(&record, current_record, sizeof(Record));
          // print every entry for the given id
          if (record.id == *id)
          {
            printf("%d, %s, %s, %s\n", record.id, record.name, record.surname, record.city);
            return HT_OK;
          }
        }
        BF_UnpinBlock(block);
      } while (next_block != -1);
      // if HT_Ok isn't returned there is no record with the wanted id
      printf("There is no record with id = %d\n", *id);
    }
    return HT_OK;
  }

// HT_ErrorCode HashStatistics(char *filename) {
//     int file_desc, block_num, records_per_block, max_records = 0, min_records = 900000, total_records = 0;
//     BF_Block *block = NULL;
//     char *block_data;

//     CALL_BF(BF_OpenFile(filename, &file_desc));

//     int blocks_num;
//     CALL_BF(BF_GetBlockCounter(file_desc, &blocks_num));

//     for (block_num = 1; block_num < blocks_num; ++block_num) {
//         BF_Block_Init(&block);
//         CALL_BF(BF_GetBlock(file_desc, block_num, block));
//         block_data = BF_Block_GetData(block);

//         memcpy(&records_per_block, block_data + BF_BLOCK_SIZE - 2 * sizeof(int), sizeof(int));
//         // for debugging
//         //printf("Block %d: %d records\n", block_num, records_per_block);

//         total_records += records_per_block;
//         if (records_per_block > max_records) {
//             max_records = records_per_block;
//         }
//         if (records_per_block < min_records) {
//             min_records = records_per_block;
//         }

//         CALL_BF(BF_UnpinBlock(block));
//         block = NULL;
//     }

//     CALL_BF(BF_CloseFile(file_desc));

//     // printf("Number of blocks: %d\n", blocks_num - 1);
//     // printf("Minimum records per block: %d\n", min_records);
//     // printf("Maximum records per block: %d\n", max_records);
//     // printf("Average records per block: %.2f\n", (float)total_records / (blocks_num - 1));

//     return HT_OK;
// }

