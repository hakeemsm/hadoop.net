using System;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	internal class OnDiskMapOutput<K, V> : MapOutput<K, V>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.OnDiskMapOutput
			));

		private readonly FileSystem fs;

		private readonly Path tmpOutputPath;

		private readonly Path outputPath;

		private readonly MergeManagerImpl<K, V> merger;

		private readonly OutputStream disk;

		private long compressedSize;

		private readonly Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		public OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId, MergeManagerImpl
			<K, V> merger, long size, JobConf conf, MapOutputFile mapOutputFile, int fetcher
			, bool primaryMapOutput)
			: this(mapId, reduceId, merger, size, conf, mapOutputFile, fetcher, primaryMapOutput
				, FileSystem.GetLocal(conf).GetRaw(), mapOutputFile.GetInputFileForWrite(mapId.GetTaskID
				(), size))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId, MergeManagerImpl
			<K, V> merger, long size, JobConf conf, MapOutputFile mapOutputFile, int fetcher
			, bool primaryMapOutput, FileSystem fs, Path outputPath)
			: base(mapId, size, primaryMapOutput)
		{
			this.fs = fs;
			this.merger = merger;
			this.outputPath = outputPath;
			tmpOutputPath = GetTempPath(outputPath, fetcher);
			disk = CryptoUtils.WrapIfNecessary(conf, fs.Create(tmpOutputPath));
			this.conf = conf;
		}

		[VisibleForTesting]
		internal static Path GetTempPath(Path outPath, int fetcher)
		{
			return outPath.Suffix(fetcher.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Shuffle(MapHost host, InputStream input, long compressedLength
			, long decompressedLength, ShuffleClientMetrics metrics, Reporter reporter)
		{
			input = new IFileInputStream(input, compressedLength, conf);
			// Copy data to local-disk
			long bytesLeft = compressedLength;
			try
			{
				int BytesToRead = 64 * 1024;
				byte[] buf = new byte[BytesToRead];
				while (bytesLeft > 0)
				{
					int n = ((IFileInputStream)input).ReadWithChecksum(buf, 0, (int)Math.Min(bytesLeft
						, BytesToRead));
					if (n < 0)
					{
						throw new IOException("read past end of stream reading " + GetMapId());
					}
					disk.Write(buf, 0, n);
					bytesLeft -= n;
					metrics.InputBytes(n);
					reporter.Progress();
				}
				Log.Info("Read " + (compressedLength - bytesLeft) + " bytes from map-output for "
					 + GetMapId());
				disk.Close();
			}
			catch (IOException ioe)
			{
				// Close the streams
				IOUtils.Cleanup(Log, input, disk);
				// Re-throw
				throw;
			}
			// Sanity check
			if (bytesLeft != 0)
			{
				throw new IOException("Incomplete map output received for " + GetMapId() + " from "
					 + host.GetHostName() + " (" + bytesLeft + " bytes missing of " + compressedLength
					 + ")");
			}
			this.compressedSize = compressedLength;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Commit()
		{
			fs.Rename(tmpOutputPath, outputPath);
			MergeManagerImpl.CompressAwarePath compressAwarePath = new MergeManagerImpl.CompressAwarePath
				(outputPath, GetSize(), this.compressedSize);
			merger.CloseOnDiskFile(compressAwarePath);
		}

		public override void Abort()
		{
			try
			{
				fs.Delete(tmpOutputPath, false);
			}
			catch (IOException ie)
			{
				Log.Info("failure to clean up " + tmpOutputPath, ie);
			}
		}

		public override string GetDescription()
		{
			return "DISK";
		}
	}
}
