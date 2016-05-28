using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which selects a random file and selects a random read size (from
	/// the read size option) and reads from the start of that file to the read size
	/// (or the full file) and verifies the bytes that were written there.
	/// </summary>
	/// <remarks>
	/// Operation which selects a random file and selects a random read size (from
	/// the read size option) and reads from the start of that file to the read size
	/// (or the full file) and verifies the bytes that were written there.
	/// This operation will capture statistics on success the time taken to read that
	/// file and the number of successful readings that occurred as well as the
	/// number of bytes read and the number of chunks verified and the number of
	/// chunks which failed verification and on failure or error it will capture the
	/// number of failures and the amount of time taken to fail
	/// </remarks>
	internal class ReadOp : Operation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.ReadOp
			));

		internal ReadOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(Org.Apache.Hadoop.FS.Slive.ReadOp).Name, cfg, rnd)
		{
		}

		/// <summary>Gets the file name to read</summary>
		/// <returns>Path</returns>
		protected internal virtual Path GetReadFile()
		{
			Path fn = GetFinder().GetFile();
			return fn;
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			IList<OperationOutput> @out = base.Run(fs);
			DataInputStream @is = null;
			try
			{
				Path fn = GetReadFile();
				Range<long> readSizeRange = GetConfig().GetReadSize();
				long readSize = 0;
				string readStrAm = string.Empty;
				if (GetConfig().ShouldReadFullFile())
				{
					readSize = long.MaxValue;
					readStrAm = "full file";
				}
				else
				{
					readSize = Range.BetweenPositive(GetRandom(), readSizeRange);
					readStrAm = Helper.ToByteInfo(readSize);
				}
				long timeTaken = 0;
				long chunkSame = 0;
				long chunkDiff = 0;
				long bytesRead = 0;
				long startTime = 0;
				DataVerifier vf = new DataVerifier();
				Log.Info("Attempting to read file at " + fn + " of size (" + readStrAm + ")");
				{
					// open
					startTime = Timer.Now();
					@is = fs.Open(fn);
					timeTaken += Timer.Elapsed(startTime);
					// read & verify
					DataVerifier.VerifyOutput vo = vf.VerifyFile(readSize, @is);
					timeTaken += vo.GetReadTime();
					chunkSame += vo.GetChunksSame();
					chunkDiff += vo.GetChunksDifferent();
					bytesRead += vo.GetBytesRead();
					// capture close time
					startTime = Timer.Now();
					@is.Close();
					@is = null;
					timeTaken += Timer.Elapsed(startTime);
				}
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.OkTimeTaken, timeTaken));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.BytesRead, bytesRead));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Successes, 1L));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.ChunksVerified, chunkSame));
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.ChunksUnverified, chunkDiff));
				Log.Info("Read " + Helper.ToByteInfo(bytesRead) + " of " + fn + " with " + chunkSame
					 + " chunks being same as expected and " + chunkDiff + " chunks being different than expected in "
					 + timeTaken + " milliseconds");
			}
			catch (FileNotFoundException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.NotFound, 1L));
				Log.Warn("Error with reading", e);
			}
			catch (BadFileException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.BadFiles, 1L));
				Log.Warn("Error reading bad file", e);
			}
			catch (IOException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error reading", e);
			}
			finally
			{
				if (@is != null)
				{
					try
					{
						@is.Close();
					}
					catch (IOException e)
					{
						Log.Warn("Error closing read stream", e);
					}
				}
			}
			return @out;
		}
	}
}
