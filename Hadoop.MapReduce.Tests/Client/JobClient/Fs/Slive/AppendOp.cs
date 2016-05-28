using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which selects a random file and appends a random amount of bytes
	/// (selected from the configuration for append size) to that file if it exists.
	/// </summary>
	/// <remarks>
	/// Operation which selects a random file and appends a random amount of bytes
	/// (selected from the configuration for append size) to that file if it exists.
	/// This operation will capture statistics on success for bytes written, time
	/// taken (milliseconds), and success count and on failure it will capture the
	/// number of failures and the time taken (milliseconds) to fail.
	/// </remarks>
	internal class AppendOp : Operation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.AppendOp
			));

		internal AppendOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(Org.Apache.Hadoop.FS.Slive.AppendOp).Name, cfg, rnd)
		{
		}

		/// <summary>Gets the file to append to</summary>
		/// <returns>Path</returns>
		protected internal virtual Path GetAppendFile()
		{
			Path fn = GetFinder().GetFile();
			return fn;
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			IList<OperationOutput> @out = base.Run(fs);
			OutputStream os = null;
			try
			{
				Path fn = GetAppendFile();
				// determine file status for file length requirement
				// to know if should fill in partial bytes
				Range<long> appendSizeRange = GetConfig().GetAppendSize();
				if (GetConfig().ShouldAppendUseBlockSize())
				{
					appendSizeRange = GetConfig().GetBlockSize();
				}
				long appendSize = Range.BetweenPositive(GetRandom(), appendSizeRange);
				long timeTaken = 0;
				long bytesAppended = 0;
				DataWriter writer = new DataWriter(GetRandom());
				Log.Info("Attempting to append to file at " + fn + " of size " + Helper.ToByteInfo
					(appendSize));
				{
					// open
					long startTime = Timer.Now();
					os = fs.Append(fn);
					timeTaken += Timer.Elapsed(startTime);
					// append given length
					DataWriter.GenerateOutput stats = writer.WriteSegment(appendSize, os);
					timeTaken += stats.GetTimeTaken();
					bytesAppended += stats.GetBytesWritten();
					// capture close time
					startTime = Timer.Now();
					os.Close();
					os = null;
					timeTaken += Timer.Elapsed(startTime);
				}
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.BytesWritten, bytesAppended));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.OkTimeTaken, timeTaken));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Successes, 1L));
				Log.Info("Appended " + Helper.ToByteInfo(bytesAppended) + " to file " + fn + " in "
					 + timeTaken + " milliseconds");
			}
			catch (FileNotFoundException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.NotFound, 1L));
				Log.Warn("Error with appending", e);
			}
			catch (IOException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error with appending", e);
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
						Log.Warn("Error with closing append stream", e);
					}
				}
			}
			return @out;
		}
	}
}
