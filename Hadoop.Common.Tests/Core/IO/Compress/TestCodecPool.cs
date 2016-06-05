using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	public class TestCodecPool
	{
		private readonly string LeaseCountErr = "Incorrect number of leased (de)compressors";

		internal DefaultCodec codec;

		[SetUp]
		public virtual void Setup()
		{
			this.codec = new DefaultCodec();
			this.codec.SetConf(new Configuration());
		}

		public virtual void TestCompressorPoolCounts()
		{
			// Get two compressors and return them
			Compressor comp1 = CodecPool.GetCompressor(codec);
			Compressor comp2 = CodecPool.GetCompressor(codec);
			Assert.Equal(LeaseCountErr, 2, CodecPool.GetLeasedCompressorsCount
				(codec));
			CodecPool.ReturnCompressor(comp2);
			Assert.Equal(LeaseCountErr, 1, CodecPool.GetLeasedCompressorsCount
				(codec));
			CodecPool.ReturnCompressor(comp1);
			Assert.Equal(LeaseCountErr, 0, CodecPool.GetLeasedCompressorsCount
				(codec));
			CodecPool.ReturnCompressor(comp1);
			Assert.Equal(LeaseCountErr, 0, CodecPool.GetLeasedCompressorsCount
				(codec));
		}

		public virtual void TestCompressorNotReturnSameInstance()
		{
			Compressor comp = CodecPool.GetCompressor(codec);
			CodecPool.ReturnCompressor(comp);
			CodecPool.ReturnCompressor(comp);
			ICollection<Compressor> compressors = new HashSet<Compressor>();
			for (int i = 0; i < 10; ++i)
			{
				compressors.AddItem(CodecPool.GetCompressor(codec));
			}
			Assert.Equal(10, compressors.Count);
			foreach (Compressor compressor in compressors)
			{
				CodecPool.ReturnCompressor(compressor);
			}
		}

		public virtual void TestDecompressorPoolCounts()
		{
			// Get two decompressors and return them
			Decompressor decomp1 = CodecPool.GetDecompressor(codec);
			Decompressor decomp2 = CodecPool.GetDecompressor(codec);
			Assert.Equal(LeaseCountErr, 2, CodecPool.GetLeasedDecompressorsCount
				(codec));
			CodecPool.ReturnDecompressor(decomp2);
			Assert.Equal(LeaseCountErr, 1, CodecPool.GetLeasedDecompressorsCount
				(codec));
			CodecPool.ReturnDecompressor(decomp1);
			Assert.Equal(LeaseCountErr, 0, CodecPool.GetLeasedDecompressorsCount
				(codec));
			CodecPool.ReturnDecompressor(decomp1);
			Assert.Equal(LeaseCountErr, 0, CodecPool.GetLeasedCompressorsCount
				(codec));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMultiThreadedCompressorPool()
		{
			int iterations = 4;
			ExecutorService threadpool = Executors.NewFixedThreadPool(3);
			LinkedBlockingDeque<Compressor> queue = new LinkedBlockingDeque<Compressor>(2 * iterations
				);
			Callable<bool> consumer = new _Callable_110(queue);
			Callable<bool> producer = new _Callable_119(this, queue);
			for (int i = 0; i < iterations; i++)
			{
				threadpool.Submit(consumer);
				threadpool.Submit(producer);
			}
			// wait for completion
			threadpool.Shutdown();
			threadpool.AwaitTermination(1000, TimeUnit.Seconds);
			Assert.Equal(LeaseCountErr, 0, CodecPool.GetLeasedCompressorsCount
				(codec));
		}

		private sealed class _Callable_110 : Callable<bool>
		{
			public _Callable_110(LinkedBlockingDeque<Compressor> queue)
			{
				this.queue = queue;
			}

			/// <exception cref="System.Exception"/>
			public bool Call()
			{
				Compressor c = queue.Take();
				CodecPool.ReturnCompressor(c);
				return c != null;
			}

			private readonly LinkedBlockingDeque<Compressor> queue;
		}

		private sealed class _Callable_119 : Callable<bool>
		{
			public _Callable_119(TestCodecPool _enclosing, LinkedBlockingDeque<Compressor> queue
				)
			{
				this._enclosing = _enclosing;
				this.queue = queue;
			}

			/// <exception cref="System.Exception"/>
			public bool Call()
			{
				Compressor c = CodecPool.GetCompressor(this._enclosing.codec);
				queue.Put(c);
				return c != null;
			}

			private readonly TestCodecPool _enclosing;

			private readonly LinkedBlockingDeque<Compressor> queue;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMultiThreadedDecompressorPool()
		{
			int iterations = 4;
			ExecutorService threadpool = Executors.NewFixedThreadPool(3);
			LinkedBlockingDeque<Decompressor> queue = new LinkedBlockingDeque<Decompressor>(2
				 * iterations);
			Callable<bool> consumer = new _Callable_147(queue);
			Callable<bool> producer = new _Callable_156(this, queue);
			for (int i = 0; i < iterations; i++)
			{
				threadpool.Submit(consumer);
				threadpool.Submit(producer);
			}
			// wait for completion
			threadpool.Shutdown();
			threadpool.AwaitTermination(1000, TimeUnit.Seconds);
			Assert.Equal(LeaseCountErr, 0, CodecPool.GetLeasedDecompressorsCount
				(codec));
		}

		private sealed class _Callable_147 : Callable<bool>
		{
			public _Callable_147(LinkedBlockingDeque<Decompressor> queue)
			{
				this.queue = queue;
			}

			/// <exception cref="System.Exception"/>
			public bool Call()
			{
				Decompressor dc = queue.Take();
				CodecPool.ReturnDecompressor(dc);
				return dc != null;
			}

			private readonly LinkedBlockingDeque<Decompressor> queue;
		}

		private sealed class _Callable_156 : Callable<bool>
		{
			public _Callable_156(TestCodecPool _enclosing, LinkedBlockingDeque<Decompressor> 
				queue)
			{
				this._enclosing = _enclosing;
				this.queue = queue;
			}

			/// <exception cref="System.Exception"/>
			public bool Call()
			{
				Decompressor c = CodecPool.GetDecompressor(this._enclosing.codec);
				queue.Put(c);
				return c != null;
			}

			private readonly TestCodecPool _enclosing;

			private readonly LinkedBlockingDeque<Decompressor> queue;
		}

		public virtual void TestDecompressorNotReturnSameInstance()
		{
			Decompressor decomp = CodecPool.GetDecompressor(codec);
			CodecPool.ReturnDecompressor(decomp);
			CodecPool.ReturnDecompressor(decomp);
			ICollection<Decompressor> decompressors = new HashSet<Decompressor>();
			for (int i = 0; i < 10; ++i)
			{
				decompressors.AddItem(CodecPool.GetDecompressor(codec));
			}
			Assert.Equal(10, decompressors.Count);
			foreach (Decompressor decompressor in decompressors)
			{
				CodecPool.ReturnDecompressor(decompressor);
			}
		}
	}
}
