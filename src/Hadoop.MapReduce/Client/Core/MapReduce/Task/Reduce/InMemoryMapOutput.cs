using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	internal class InMemoryMapOutput<K, V> : MapOutput<K, V>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.InMemoryMapOutput
			));

		private Configuration conf;

		private readonly MergeManagerImpl<K, V> merger;

		private readonly byte[] memory;

		private BoundedByteArrayOutputStream byteStream;

		private readonly CompressionCodec codec;

		private readonly Decompressor decompressor;

		public InMemoryMapOutput(Configuration conf, TaskAttemptID mapId, MergeManagerImpl
			<K, V> merger, int size, CompressionCodec codec, bool primaryMapOutput)
			: base(mapId, (long)size, primaryMapOutput)
		{
			// Decompression of map-outputs
			this.conf = conf;
			this.merger = merger;
			this.codec = codec;
			byteStream = new BoundedByteArrayOutputStream(size);
			memory = byteStream.GetBuffer();
			if (codec != null)
			{
				decompressor = CodecPool.GetDecompressor(codec);
			}
			else
			{
				decompressor = null;
			}
		}

		public virtual byte[] GetMemory()
		{
			return memory;
		}

		public virtual BoundedByteArrayOutputStream GetArrayStream()
		{
			return byteStream;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Shuffle(MapHost host, InputStream input, long compressedLength
			, long decompressedLength, ShuffleClientMetrics metrics, Reporter reporter)
		{
			IFileInputStream checksumIn = new IFileInputStream(input, compressedLength, conf);
			input = checksumIn;
			// Are map-outputs compressed?
			if (codec != null)
			{
				decompressor.Reset();
				input = codec.CreateInputStream(input, decompressor);
			}
			try
			{
				IOUtils.ReadFully(input, memory, 0, memory.Length);
				metrics.InputBytes(memory.Length);
				reporter.Progress();
				Log.Info("Read " + memory.Length + " bytes from map-output for " + GetMapId());
				if (input.Read() >= 0)
				{
					throw new IOException("Unexpected extra bytes from input stream for " + GetMapId(
						));
				}
			}
			catch (IOException ioe)
			{
				// Close the streams
				IOUtils.Cleanup(Log, input);
				// Re-throw
				throw;
			}
			finally
			{
				CodecPool.ReturnDecompressor(decompressor);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Commit()
		{
			merger.CloseInMemoryFile(this);
		}

		public override void Abort()
		{
			merger.Unreserve(memory.Length);
		}

		public override string GetDescription()
		{
			return "MEMORY";
		}
	}
}
