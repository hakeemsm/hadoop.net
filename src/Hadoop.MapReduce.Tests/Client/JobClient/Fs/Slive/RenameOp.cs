using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which selects a random file and a second random file and attempts
	/// to rename that first file into the second file.
	/// </summary>
	/// <remarks>
	/// Operation which selects a random file and a second random file and attempts
	/// to rename that first file into the second file.
	/// This operation will capture statistics on success the time taken to rename
	/// those files and the number of successful renames that occurred and on failure
	/// or error it will capture the number of failures and the amount of time taken
	/// to fail
	/// </remarks>
	internal class RenameOp : Operation
	{
		/// <summary>Class that holds the src and target for renames</summary>
		protected internal class SrcTarget
		{
			private Path src;

			private Path target;

			internal SrcTarget(Path src, Path target)
			{
				this.src = src;
				this.target = target;
			}

			internal virtual Path GetSrc()
			{
				return src;
			}

			internal virtual Path GetTarget()
			{
				return target;
			}
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(RenameOp));

		internal RenameOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(RenameOp).Name, cfg, rnd)
		{
		}

		/// <summary>Gets the file names to rename</summary>
		/// <returns>SrcTarget</returns>
		protected internal virtual RenameOp.SrcTarget GetRenames()
		{
			Path src = GetFinder().GetFile();
			Path target = GetFinder().GetFile();
			return new RenameOp.SrcTarget(src, target);
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			IList<OperationOutput> @out = base.Run(fs);
			try
			{
				// find the files to modify
				RenameOp.SrcTarget targets = GetRenames();
				Path src = targets.GetSrc();
				Path target = targets.GetTarget();
				// capture results
				bool renamedOk = false;
				long timeTaken = 0;
				{
					// rename it
					long startTime = Timer.Now();
					renamedOk = fs.Rename(src, target);
					timeTaken = Timer.Elapsed(startTime);
				}
				if (renamedOk)
				{
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.OkTimeTaken, timeTaken));
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.Successes, 1L));
					Log.Info("Renamed " + src + " to " + target);
				}
				else
				{
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.Failures, 1L));
					Log.Warn("Could not rename " + src + " to " + target);
				}
			}
			catch (FileNotFoundException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.NotFound, 1L));
				Log.Warn("Error with renaming", e);
			}
			catch (IOException e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error with renaming", e);
			}
			return @out;
		}
	}
}
