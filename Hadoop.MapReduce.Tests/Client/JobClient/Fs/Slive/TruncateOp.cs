using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which selects a random file and truncates a random amount of bytes
	/// (selected from the configuration for truncate size) from that file,
	/// if it exists.
	/// </summary>
	/// <remarks>
	/// Operation which selects a random file and truncates a random amount of bytes
	/// (selected from the configuration for truncate size) from that file,
	/// if it exists.
	/// This operation will capture statistics on success for bytes written, time
	/// taken (milliseconds), and success count and on failure it will capture the
	/// number of failures and the time taken (milliseconds) to fail.
	/// </remarks>
	internal class TruncateOp : Operation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.TruncateOp
			));

		internal TruncateOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(Org.Apache.Hadoop.FS.Slive.TruncateOp).Name, cfg, rnd)
		{
		}

		/// <summary>Gets the file to truncate from</summary>
		/// <returns>Path</returns>
		protected internal virtual Path GetTruncateFile()
		{
			Path fn = GetFinder().GetFile();
			return fn;
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			IList<OperationOutput> @out = base.Run(fs);
			try
			{
				Path fn = GetTruncateFile();
				bool waitOnTruncate = GetConfig().ShouldWaitOnTruncate();
				long currentSize = fs.GetFileStatus(fn).GetLen();
				// determine file status for file length requirement
				// to know if should fill in partial bytes
				Range<long> truncateSizeRange = GetConfig().GetTruncateSize();
				if (GetConfig().ShouldTruncateUseBlockSize())
				{
					truncateSizeRange = GetConfig().GetBlockSize();
				}
				long truncateSize = Math.Max(0L, currentSize - Range.BetweenPositive(GetRandom(), 
					truncateSizeRange));
				long timeTaken = 0;
				Log.Info("Attempting to truncate file at " + fn + " to size " + Helper.ToByteInfo
					(truncateSize));
				{
					// truncate
					long startTime = Timer.Now();
					bool completed = fs.Truncate(fn, truncateSize);
					if (!completed && waitOnTruncate)
					{
						WaitForRecovery(fs, fn, truncateSize);
					}
					timeTaken += Timer.Elapsed(startTime);
				}
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.BytesWritten, 0));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.OkTimeTaken, timeTaken));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Successes, 1L));
				Log.Info("Truncate file " + fn + " to " + Helper.ToByteInfo(truncateSize) + " in "
					 + timeTaken + " milliseconds");
			}
			catch (FileNotFoundException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.NotFound, 1L));
				Log.Warn("Error with truncating", e);
			}
			catch (IOException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error with truncating", e);
			}
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		private void WaitForRecovery(FileSystem fs, Path fn, long newLength)
		{
			Log.Info("Waiting on truncate file recovery for " + fn);
			for (; ; )
			{
				FileStatus stat = fs.GetFileStatus(fn);
				if (stat.GetLen() == newLength)
				{
					break;
				}
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
			}
		}
	}
}
