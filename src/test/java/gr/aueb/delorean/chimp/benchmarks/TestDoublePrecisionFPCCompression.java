package gr.aueb.delorean.chimp.benchmarks;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.compress.brotli.BrotliCodec;
import org.apache.hadoop.hbase.io.compress.lz4.Lz4Codec;
import org.apache.hadoop.hbase.io.compress.xerial.SnappyCodec;
import org.apache.hadoop.hbase.io.compress.xz.LzmaCodec;
import org.apache.hadoop.hbase.io.compress.zstd.ZstdCodec;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.jupiter.api.Test;

import com.github.kutschkem.fpc.FpcCompressor;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.Compressor;
import fi.iki.yak.ts.compression.gorilla.Decompressor;
import gr.aueb.delorean.chimp.Chimp;
import gr.aueb.delorean.chimp.ChimpDecompressor;
import gr.aueb.delorean.chimp.ChimpN;
import gr.aueb.delorean.chimp.ChimpNDecompressor;
import gr.aueb.delorean.chimp.ChimpNNoIndex;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * the value compression.
 *
 */
public class TestDoublePrecisionFPCCompression {

	private static final int MINIMUM_TOTAL_BLOCKS = 50_000;
	private static String[] FILENAMES = {
//            "/repeat.csv.gz"
//            "/repeat-data-cssc0.csv.gz"
//            "/repeat-data-0.8-random.csv.gz"
	        "/city_temperature.csv.gz"
//	        "/Stocks-Germany-sample.txt.gz",
//	        "/SSD_HDD_benchmarks.csv.gz"
			};


    @Test
    public void testFPCSnappy() throws IOException {
        for (String filename : FILENAMES) {
            TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
            long totalSize = 0;
            float totalBlocks = 0;
            double[] values;
            long encodingDuration = 0;
            long compressDuration = 0;
            long decodingDuration = 0;
            while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
                if (values == null) {
                    timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
                    values = timeseriesFileReader.nextBlock();
                }
                FpcCompressor fpc = new FpcCompressor();

                ByteBuffer buffer = ByteBuffer.allocate(TimeseriesFileReader.DEFAULT_BLOCK_SIZE * 10);
                // Compress
                long start = System.nanoTime();
                fpc.compress(buffer, values);
                encodingDuration += System.nanoTime() - start;

                byte[] input = buffer.array();
                SnappyCodec codec = new SnappyCodec();


                // Compress
                start = System.nanoTime();
                org.apache.hadoop.io.compress.Compressor compressor_compress = codec.createCompressor();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = codec.createOutputStream(baos, compressor_compress);
                out.write(input);
                out.close();
                compressDuration += System.nanoTime() - start;
                final byte[] compressed = baos.toByteArray();
                totalSize += compressed.length * 8;
                totalBlocks++;

//                ChimpNDecompressor d = new ChimpNDecompressor(compressor.getOut(), 128);
//                start = System.nanoTime();
//                List<Double> uncompressedValues = d.getValues();
//                decodingDuration += System.nanoTime() - start;
//                for(int i=0; i<values.length; i++) {
//                    assertEquals(values[i], uncompressedValues.get(i).doubleValue(), "Value did not match");
//                }


            }
            System.out.println(String.format("FPC+Snappy: %s -, %.2f, %.2f,  %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / (totalBlocks*1000), decodingDuration / (totalBlocks*1000)));
        }
    }

    @Test
    public void testFPCZstd() throws IOException {
        for (String filename : FILENAMES) {
            TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
            long totalSize = 0;
            float totalBlocks = 0;
            double[] values;
            long encodingDuration = 0;
            long compressDuration = 0;
            long decodingDuration = 0;
            while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
                if (values == null) {
                    timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
                    values = timeseriesFileReader.nextBlock();
                }
                FpcCompressor fpc = new FpcCompressor();

                ByteBuffer buffer = ByteBuffer.allocate(TimeseriesFileReader.DEFAULT_BLOCK_SIZE * 10);
                // Compress
                long start = System.nanoTime();
                fpc.compress(buffer, values);
                encodingDuration += System.nanoTime() - start;

                byte[] input = buffer.array();

                ZstdCodec codec = new ZstdCodec();


                // Compress
                start = System.nanoTime();
                org.apache.hadoop.io.compress.Compressor compressor_compress = codec.createCompressor();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = codec.createOutputStream(baos, compressor_compress);
                out.write(input);
                out.close();
                compressDuration += System.nanoTime() - start;
                final byte[] compressed = baos.toByteArray();
                totalSize += compressed.length * 8;
                totalBlocks++;



            }
            System.out.println(String.format("FPC+Zstd: %s -, %.2f, %.2f,  %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / (totalBlocks*1000), decodingDuration / (totalBlocks*1000)));
        }
    }


    @Test
    public void testFPCLZ4() throws IOException {
        for (String filename : FILENAMES) {
            TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
            long totalSize = 0;
            float totalBlocks = 0;
            double[] values;
            long encodingDuration = 0;
            long compressDuration = 0;
            long decodingDuration = 0;
            while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
                if (values == null) {
                    timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
                    values = timeseriesFileReader.nextBlock();
                }
                FpcCompressor fpc = new FpcCompressor();

                ByteBuffer buffer = ByteBuffer.allocate(TimeseriesFileReader.DEFAULT_BLOCK_SIZE * 10);
                // Compress
                long start = System.nanoTime();
                fpc.compress(buffer, values);
                encodingDuration += System.nanoTime() - start;

                byte[] input = buffer.array();

               Lz4Codec codec = new Lz4Codec();

                // Compress
                start = System.nanoTime();
                org.apache.hadoop.io.compress.Compressor compressor_compress = codec.createCompressor();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = codec.createOutputStream(baos, compressor_compress);
                out.write(input);
                out.close();
                compressDuration += System.nanoTime() - start;
                final byte[] compressed = baos.toByteArray();
                totalSize += compressed.length * 8;
                totalBlocks++;


            }
            System.out.println(String.format("FPC+LZ4: %s -, %.2f, %.2f,  %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / (totalBlocks*1000), decodingDuration / (totalBlocks*1000)));
        }
    }

    @Test
    public void testFPCBrotli() throws IOException {
        for (String filename : FILENAMES) {
            TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
            long totalSize = 0;
            float totalBlocks = 0;
            double[] values;
            long encodingDuration = 0;
            long compressDuration = 0;
            long decodingDuration = 0;
            while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
                if (values == null) {
                    timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
                    values = timeseriesFileReader.nextBlock();
                }
                FpcCompressor fpc = new FpcCompressor();

                ByteBuffer buffer = ByteBuffer.allocate(TimeseriesFileReader.DEFAULT_BLOCK_SIZE * 10);
                // Compress
                long start = System.nanoTime();
                fpc.compress(buffer, values);
                encodingDuration += System.nanoTime() - start;

                byte[] input = buffer.array();
                BrotliCodec codec = new BrotliCodec();


                // Compress
                start = System.nanoTime();
                org.apache.hadoop.io.compress.Compressor compressor_compress = codec.createCompressor();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = codec.createOutputStream(baos, compressor_compress);
                out.write(input);
                out.close();
                compressDuration += System.nanoTime() - start;
                final byte[] compressed = baos.toByteArray();
                totalSize += compressed.length * 8;
                totalBlocks++;


            }
            System.out.println(String.format("FPC+Brotli: %s -, %.2f, %.2f,  %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / (totalBlocks*1000), decodingDuration / (totalBlocks*1000)));
        }
    }

    @Test
    public void testFPCXz() throws IOException {
        for (String filename : FILENAMES) {
            TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
            long totalSize = 0;
            float totalBlocks = 0;
            double[] values;
            long encodingDuration = 0;
            long compressDuration = 0;
            long decodingDuration = 0;
            while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
                if (values == null) {
                    timeseriesFileReader = new TimeseriesFileReader(getClass().getResourceAsStream(filename));
                    values = timeseriesFileReader.nextBlock();
                }
                FpcCompressor fpc = new FpcCompressor();

                ByteBuffer buffer = ByteBuffer.allocate(TimeseriesFileReader.DEFAULT_BLOCK_SIZE * 10);
                // Compress
                long start = System.nanoTime();
                fpc.compress(buffer, values);
                encodingDuration += System.nanoTime() - start;

                byte[] input = buffer.array();




                Configuration conf = new Configuration();
                // LZMA levels range from 1 to 9.
                // Level 9 might take several minutes to complete. 3 is our default. 1 will be fast.
                conf.setInt(LzmaCodec.LZMA_LEVEL_KEY, 3);
                LzmaCodec codec = new LzmaCodec();
                codec.setConf(conf);


                // Compress
                start = System.nanoTime();
                org.apache.hadoop.io.compress.Compressor compressor_compress = codec.createCompressor();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CompressionOutputStream out = codec.createOutputStream(baos, compressor_compress);
                out.write(input);
                out.close();
                compressDuration += System.nanoTime() - start;
                final byte[] compressed = baos.toByteArray();
                totalSize += compressed.length * 8;
                totalBlocks++;


            }
            System.out.println(String.format("FPC+Xz: %s -, %.2f, %.2f,  %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / (totalBlocks*1000), decodingDuration / (totalBlocks*1000)));
        }
    }




    public static double[] toDoubleArray(byte[] byteArray){
        int times = Double.SIZE / Byte.SIZE;
        double[] doubles = new double[byteArray.length / times];
        for(int i=0;i<doubles.length;i++){
            doubles[i] = ByteBuffer.wrap(byteArray, i*times, times).getDouble();
        }
        return doubles;
    }

}
