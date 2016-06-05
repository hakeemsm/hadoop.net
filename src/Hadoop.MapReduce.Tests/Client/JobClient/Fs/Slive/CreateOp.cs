using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which selects a random file and a random number of bytes to create
	/// that file with (from the write size option) and selects a random block size
	/// (from the block size option) and a random replication amount (from the
	/// replication option) and attempts to create a file with those options.
	/// </summary>
	/// <remarks>
	/// Operation which selects a random file and a random number of bytes to create
	/// that file with (from the write size option) and selects a random block size
	/// (from the block size option) and a random replication amount (from the
	/// replication option) and attempts to create a file with those options.
	/// This operation will capture statistics on success for bytes written, time
	/// taken (milliseconds), and success count and on failure it will capture the
	/// number of failures and the time taken (milliseconds) to fail.
	/// </remarks>
	internal class CreateOp : Operation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.CreateOp
			));

		private static int DefIoBufferSize = 4096;

		private const string IoBufConfig = ("io.file.buffer.size");

		internal CreateOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(Org.Apache.Hadoop.FS.Slive.CreateOp).Name, cfg, rnd)
		{
		}

		/// <summary>
		/// Returns the block size to use (aligned to nearest BYTES_PER_CHECKSUM if
		/// configuration says a value exists) - this will avoid the warnings caused by
		/// this not occurring and the file will not be created if it is not correct...
		/// </summary>
		/// <returns>long</returns>
		private long DetermineBlockSize()
		{
			Range<long> blockSizeRange = GetConfig().GetBlockSize();
			long blockSize = Range.BetweenPositive(GetRandom(), blockSizeRange);
			long byteChecksum = GetConfig().GetByteCheckSum();
			if (byteChecksum == null)
			{
				return blockSize;
			}
			// adjust to nearest multiple
			long full = (blockSize / byteChecksum) * byteChecksum;
			long toFull = blockSize - full;
			if (toFull >= (byteChecksum / 2))
			{
				full += byteChecksum;
			}
			// adjust if over extended
			if (full > blockSizeRange.GetUpper())
			{
				full = blockSizeRange.GetUpper();
			}
			if (full < blockSizeRange.GetLower())
			{
				full = blockSizeRange.GetLower();
			}
			return full;
		}

		/// <summary>Gets the replication amount</summary>
		/// <returns>short</returns>
		private short DetermineReplication()
		{
			Range<short> replicationAmountRange = GetConfig().GetReplication();
			Range<long> repRange = new Range<long>(replicationAmountRange.GetLower(), replicationAmountRange
				.GetUpper());
			short replicationAmount = (short)Range.BetweenPositive(GetRandom(), repRange);
			return replicationAmount;
		}

		/// <summary>Gets the output buffering size to use</summary>
		/// <returns>int</returns>
		private int GetBufferSize()
		{
			return GetConfig().GetConfig().GetInt(IoBufConfig, DefIoBufferSize);
		}

		/// <summary>Gets the file to create</summary>
		/// <returns>Path</returns>
		protected internal virtual Path GetCreateFile()
		{
			Path fn = GetFinder().GetFile();
			return fn;
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			IList<OperationOutput> @out = base.Run(fs);
			FSDataOutputStream os = null;
			try
			{
				Path fn = GetCreateFile();
				Range<long> writeSizeRange = GetConfig().GetWriteSize();
				long writeSize = 0;
				long blockSize = DetermineBlockSize();
				short replicationAmount = DetermineReplication();
				if (GetConfig().ShouldWriteUseBlockSize())
				{
					writeSizeRange = GetConfig().GetBlockSize();
				}
				writeSize = Range.BetweenPositive(GetRandom(), writeSizeRange);
				long bytesWritten = 0;
				long timeTaken = 0;
				int bufSize = GetBufferSize();
				bool overWrite = false;
				DataWriter writer = new DataWriter(GetRandom());
				Log.Info("Attempting to create file at " + fn + " of size " + Helper.ToByteInfo(writeSize
					) + " using blocksize " + Helper.ToByteInfo(blockSize) + " and replication amount "
					 + replicationAmount);
				{
					// open & create
					long startTime = Timer.Now();
					os = fs.Create(fn, overWrite, bufSize, replicationAmount, blockSize);
					timeTaken += Timer.Elapsed(startTime);
					// write the given length
					DataWriter.GenerateOutput stats = writer.WriteSegment(writeSize, os);
					bytesWritten += stats.GetBytesWritten();
					timeTaken += stats.GetTimeTaken();
					// capture close time
					startTime = Timer.Now();
					os.Close();
					os = null;
					timeTaken += Timer.Elapsed(startTime);
				}
				Log.Info("Created file at " + fn + " of size " + Helper.ToByteInfo(bytesWritten) 
					+ " bytes using blocksize " + Helper.ToByteInfo(blockSize) + " and replication amount "
					 + replicationAmount + " in " + timeTaken + " milliseconds");
				// collect all the stats
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.OkTimeTaken, timeTaken));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.BytesWritten, bytesWritten));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Successes, 1L));
			}
			catch (IOException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error with creating", e);
			}
			finally
			{
				if (os != null)
				{
					try
					{
						os.Close();
					}
					catch (IOException e)
					{
						Log.Warn("Error closing create stream", e);
					}
				}
			}
			return @out;
		}
	}
}
