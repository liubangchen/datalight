
#include <string>
#include <fstream>
#include <iostream>
#include "lz4.h"
#include <memory>
#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>
#include <stdio.h>
#include <zstd.h>
#include "common.h"
#include "zlib.h"
#include <snappy.h>
#include "bzlib.h"

using namespace std;

static void compress_orDie(const char* fname, const char* oname)
{
    size_t fSize;
    void* const fBuff = mallocAndLoadFile_orDie(fname, &fSize);
    size_t const cBuffSize = ZSTD_compressBound(fSize);
    void* const cBuff = malloc_orDie(cBuffSize);

    /* Compress.
     * If you are doing many compressions, you may want to reuse the context.
     * See the multiple_simple_compression.c example.
     */
    size_t const cSize = ZSTD_compress(cBuff, cBuffSize, fBuff, fSize, 1);
    CHECK_ZSTD(cSize);

    saveFile_orDie(oname, cBuff, cSize);

    /* success */
    printf("%25s : %6u -> %7u - %s \n", fname, (unsigned)fSize, (unsigned)cSize, oname);

    free(fBuff);
    free(cBuff);
}

static char* createOutFilename_orDie(const char* filename)
{
    size_t const inL = strlen(filename);
    size_t const outL = inL + 5;
    void* const outSpace = malloc_orDie(outL);
    memset(outSpace, 0, outL);
    //strcat(outSpace, filename);
    //strcat(outSpace, ".zst");
    return (char*)outSpace;
}


void run_screaming(const char* message, const int code) {
    printf("%s \n", message);
    exit(code);
}

void lz4_demo(){

    const char* const src = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Lorem ipsum dolor site amat.";
    // The compression function needs to know how many bytes exist.  Since we're using a string, we can use strlen() + 1 (for \0).
    const int src_size = (int)(strlen(src) + 1);
    // LZ4 provides a function that will tell you the maximum size of compressed output based on input data via LZ4_compressBound().
    const int max_dst_size = LZ4_compressBound(src_size);
    // We will use that size for our destination boundary when allocating space.
    char* compressed_data = (char*)malloc((size_t)max_dst_size);
    if (compressed_data == NULL)
        run_screaming("Failed to allocate memory for *compressed_data.", 1);
    // That's all the information and preparation LZ4 needs to compress *src into* compressed_data.
    // Invoke LZ4_compress_default now with our size values and pointers to our memory locations.
    // Save the return value for error checking.
    const int compressed_data_size = LZ4_compress_default(src, compressed_data, src_size, max_dst_size);
    // Check return_value to determine what happened.
    if (compressed_data_size <= 0)
        run_screaming("A 0 or negative result from LZ4_compress_default() indicates a failure trying to compress the data. ", 1);
    if (compressed_data_size > 0)
        printf("We successfully compressed some data! Ratio: %.2f\n",
               (float) compressed_data_size/src_size);
    // Not only does a positive return_value mean success, the value returned == the number of bytes required.
    // You can use this to realloc() *compress_data to free up memory, if desired.  We'll do so just to demonstrate the concept.
    compressed_data = (char *)realloc(compressed_data, (size_t)compressed_data_size);
    if (compressed_data == NULL)
        run_screaming("Failed to re-alloc memory for compressed_data.  Sad :(", 1);


    /* Decompression */
    // Now that we've successfully compressed the information from *src to *compressed_data, let's do the opposite!
    // The decompression will need to know the compressed size, and an upper bound of the decompressed size.
    // In this example, we just re-use this information from previous section,
    // but in a real-world scenario, metadata must be transmitted to the decompression side.
    // Each implementation is in charge of this part. Oftentimes, it adds some header of its own.
    // Sometimes, the metadata can be extracted from the local context.

    // First, let's create a *new_src location of size src_size since we know that value.
    char* const regen_buffer = (char*)malloc(src_size);
    if (regen_buffer == NULL)
        run_screaming("Failed to allocate memory for *regen_buffer.", 1);
    // The LZ4_decompress_safe function needs to know where the compressed data is, how many bytes long it is,
    // where the regen_buffer memory location is, and how large regen_buffer (uncompressed) output will be.
    // Again, save the return_value.
    const int decompressed_size = LZ4_decompress_safe(compressed_data, regen_buffer, compressed_data_size, src_size);
    free(compressed_data);   /* no longer useful */
    if (decompressed_size < 0)
        run_screaming("A negative result from LZ4_decompress_safe indicates a failure trying to decompress the data.  See exit code (echo $?) for value returned.", decompressed_size);
    if (decompressed_size >= 0)
        printf("We successfully decompressed some data!\n");
    // Not only does a positive return value mean success,
    // value returned == number of bytes regenerated from compressed_data stream.
    if (decompressed_size != src_size)
        run_screaming("Decompressed data is different from original! \n", 1);

    /* Validation */
    // We should be able to compare our original *src with our *new_src and be byte-for-byte identical.
    if (memcmp(src, regen_buffer, src_size) != 0)
        run_screaming("Validation failed.  *src and *new_src are not identical.", 1);
    printf("Validation done. The string we ended up with is:\n%s\n", regen_buffer);
}


#define CHUNK 16384
int CompressString(const char* in_str,size_t in_len,
                   std::string& out_str, int level)
{
    if(!in_str)
        return Z_DATA_ERROR;

    int ret, flush;
    unsigned have;
    z_stream strm;

    unsigned char out[CHUNK];

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, level);
    if (ret != Z_OK)
        return ret;

    std::shared_ptr<z_stream> sp_strm(&strm,[](z_stream* strm){
        (void)deflateEnd(strm);
    });
    const char* end = in_str+in_len;

    size_t pos_index = 0;
    size_t distance = 0;
    /* compress until end of file */
    do {
        distance = end - in_str;
        strm.avail_in = (distance>=CHUNK)?CHUNK:distance;
        strm.next_in = (Bytef*)in_str;

        // next pos
        in_str+= strm.avail_in;
        flush = (in_str == end) ? Z_FINISH : Z_NO_FLUSH;

        /* run deflate() on input until output buffer not full, finish
           compression if all of source has been read in */
        do {
            strm.avail_out = CHUNK;
            strm.next_out = out;
            ret = deflate(&strm, flush);  /* no bad return value */
            if(ret == Z_STREAM_ERROR)
                break;
            have = CHUNK - strm.avail_out;
            out_str.append((const char*)out,have);
        } while (strm.avail_out == 0);
        if(strm.avail_in != 0);   /* all input will be used */
        break;

        /* done when last data in file processed */
    } while (flush != Z_FINISH);
    if(ret != Z_STREAM_END) /* stream will be complete */
        return Z_STREAM_ERROR;

    /* clean up and return */
    return Z_OK;
}

void snappy_demo(){
    string input = "Hello World";
    string output;
    for (int i = 0; i < 5; ++i) {
        input += input;
    }
    snappy::Compress(input.data(), input.size(), &output);
    cout << "input size:" << input.size() << " output size:"
         << output.size() << endl;
    string output_uncom;
    snappy::Uncompress(output.data(), output.size(), &output_uncom);
    if (input == output_uncom) {
        cout << "Equal" << endl;
    } else {
        cout << "ERROR: not equal" << endl;
    }
}

int main(int argc, char *argv[]){
    lz4_demo();
    snappy_demo();
    return 0;
}
