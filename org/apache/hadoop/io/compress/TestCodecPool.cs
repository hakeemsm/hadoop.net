using Sharpen;

namespace org.apache.hadoop.io.compress
{
	public class TestCodecPool
	{
		private readonly string LEASE_COUNT_ERR = "Incorrect number of leased (de)compressors";

		internal org.apache.hadoop.io.compress.DefaultCodec codec;

		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			this.codec = new org.apache.hadoop.io.compress.DefaultCodec();
			this.codec.setConf(new org.apache.hadoop.conf.Configuration());
		}

		public virtual void testCompressorPoolCounts()
		{
			// Get two compressors and return them
			org.apache.hadoop.io.compress.Compressor comp1 = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(codec);
			org.apache.hadoop.io.compress.Compressor comp2 = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(codec);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 2, org.apache.hadoop.io.compress.CodecPool
				.getLeasedCompressorsCount(codec));
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(comp2);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 1, org.apache.hadoop.io.compress.CodecPool
				.getLeasedCompressorsCount(codec));
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(comp1);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 0, org.apache.hadoop.io.compress.CodecPool
				.getLeasedCompressorsCount(codec));
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(comp1);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 0, org.apache.hadoop.io.compress.CodecPool
				.getLeasedCompressorsCount(codec));
		}

		public virtual void testCompressorNotReturnSameInstance()
		{
			org.apache.hadoop.io.compress.Compressor comp = org.apache.hadoop.io.compress.CodecPool
				.getCompressor(codec);
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(comp);
			org.apache.hadoop.io.compress.CodecPool.returnCompressor(comp);
			System.Collections.Generic.ICollection<org.apache.hadoop.io.compress.Compressor> 
				compressors = new java.util.HashSet<org.apache.hadoop.io.compress.Compressor>();
			for (int i = 0; i < 10; ++i)
			{
				compressors.add(org.apache.hadoop.io.compress.CodecPool.getCompressor(codec));
			}
			NUnit.Framework.Assert.AreEqual(10, compressors.Count);
			foreach (org.apache.hadoop.io.compress.Compressor compressor in compressors)
			{
				org.apache.hadoop.io.compress.CodecPool.returnCompressor(compressor);
			}
		}

		public virtual void testDecompressorPoolCounts()
		{
			// Get two decompressors and return them
			org.apache.hadoop.io.compress.Decompressor decomp1 = org.apache.hadoop.io.compress.CodecPool
				.getDecompressor(codec);
			org.apache.hadoop.io.compress.Decompressor decomp2 = org.apache.hadoop.io.compress.CodecPool
				.getDecompressor(codec);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 2, org.apache.hadoop.io.compress.CodecPool
				.getLeasedDecompressorsCount(codec));
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(decomp2);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 1, org.apache.hadoop.io.compress.CodecPool
				.getLeasedDecompressorsCount(codec));
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(decomp1);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 0, org.apache.hadoop.io.compress.CodecPool
				.getLeasedDecompressorsCount(codec));
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(decomp1);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 0, org.apache.hadoop.io.compress.CodecPool
				.getLeasedCompressorsCount(codec));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMultiThreadedCompressorPool()
		{
			int iterations = 4;
			java.util.concurrent.ExecutorService threadpool = java.util.concurrent.Executors.
				newFixedThreadPool(3);
			java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Compressor
				> queue = new java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Compressor
				>(2 * iterations);
			java.util.concurrent.Callable<bool> consumer = new _Callable_110(queue);
			java.util.concurrent.Callable<bool> producer = new _Callable_119(this, queue);
			for (int i = 0; i < iterations; i++)
			{
				threadpool.submit(consumer);
				threadpool.submit(producer);
			}
			// wait for completion
			threadpool.shutdown();
			threadpool.awaitTermination(1000, java.util.concurrent.TimeUnit.SECONDS);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 0, org.apache.hadoop.io.compress.CodecPool
				.getLeasedCompressorsCount(codec));
		}

		private sealed class _Callable_110 : java.util.concurrent.Callable<bool>
		{
			public _Callable_110(java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Compressor
				> queue)
			{
				this.queue = queue;
			}

			/// <exception cref="System.Exception"/>
			public bool call()
			{
				org.apache.hadoop.io.compress.Compressor c = queue.take();
				org.apache.hadoop.io.compress.CodecPool.returnCompressor(c);
				return c != null;
			}

			private readonly java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Compressor
				> queue;
		}

		private sealed class _Callable_119 : java.util.concurrent.Callable<bool>
		{
			public _Callable_119(TestCodecPool _enclosing, java.util.concurrent.LinkedBlockingDeque
				<org.apache.hadoop.io.compress.Compressor> queue)
			{
				this._enclosing = _enclosing;
				this.queue = queue;
			}

			/// <exception cref="System.Exception"/>
			public bool call()
			{
				org.apache.hadoop.io.compress.Compressor c = org.apache.hadoop.io.compress.CodecPool
					.getCompressor(this._enclosing.codec);
				queue.put(c);
				return c != null;
			}

			private readonly TestCodecPool _enclosing;

			private readonly java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Compressor
				> queue;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMultiThreadedDecompressorPool()
		{
			int iterations = 4;
			java.util.concurrent.ExecutorService threadpool = java.util.concurrent.Executors.
				newFixedThreadPool(3);
			java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Decompressor
				> queue = new java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Decompressor
				>(2 * iterations);
			java.util.concurrent.Callable<bool> consumer = new _Callable_147(queue);
			java.util.concurrent.Callable<bool> producer = new _Callable_156(this, queue);
			for (int i = 0; i < iterations; i++)
			{
				threadpool.submit(consumer);
				threadpool.submit(producer);
			}
			// wait for completion
			threadpool.shutdown();
			threadpool.awaitTermination(1000, java.util.concurrent.TimeUnit.SECONDS);
			NUnit.Framework.Assert.AreEqual(LEASE_COUNT_ERR, 0, org.apache.hadoop.io.compress.CodecPool
				.getLeasedDecompressorsCount(codec));
		}

		private sealed class _Callable_147 : java.util.concurrent.Callable<bool>
		{
			public _Callable_147(java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Decompressor
				> queue)
			{
				this.queue = queue;
			}

			/// <exception cref="System.Exception"/>
			public bool call()
			{
				org.apache.hadoop.io.compress.Decompressor dc = queue.take();
				org.apache.hadoop.io.compress.CodecPool.returnDecompressor(dc);
				return dc != null;
			}

			private readonly java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Decompressor
				> queue;
		}

		private sealed class _Callable_156 : java.util.concurrent.Callable<bool>
		{
			public _Callable_156(TestCodecPool _enclosing, java.util.concurrent.LinkedBlockingDeque
				<org.apache.hadoop.io.compress.Decompressor> queue)
			{
				this._enclosing = _enclosing;
				this.queue = queue;
			}

			/// <exception cref="System.Exception"/>
			public bool call()
			{
				org.apache.hadoop.io.compress.Decompressor c = org.apache.hadoop.io.compress.CodecPool
					.getDecompressor(this._enclosing.codec);
				queue.put(c);
				return c != null;
			}

			private readonly TestCodecPool _enclosing;

			private readonly java.util.concurrent.LinkedBlockingDeque<org.apache.hadoop.io.compress.Decompressor
				> queue;
		}

		public virtual void testDecompressorNotReturnSameInstance()
		{
			org.apache.hadoop.io.compress.Decompressor decomp = org.apache.hadoop.io.compress.CodecPool
				.getDecompressor(codec);
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(decomp);
			org.apache.hadoop.io.compress.CodecPool.returnDecompressor(decomp);
			System.Collections.Generic.ICollection<org.apache.hadoop.io.compress.Decompressor
				> decompressors = new java.util.HashSet<org.apache.hadoop.io.compress.Decompressor
				>();
			for (int i = 0; i < 10; ++i)
			{
				decompressors.add(org.apache.hadoop.io.compress.CodecPool.getDecompressor(codec));
			}
			NUnit.Framework.Assert.AreEqual(10, decompressors.Count);
			foreach (org.apache.hadoop.io.compress.Decompressor decompressor in decompressors)
			{
				org.apache.hadoop.io.compress.CodecPool.returnDecompressor(decompressor);
			}
		}
	}
}
