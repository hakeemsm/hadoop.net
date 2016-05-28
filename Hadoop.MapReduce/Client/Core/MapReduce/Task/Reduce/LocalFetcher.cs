using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	/// <summary>
	/// LocalFetcher is used by LocalJobRunner to perform a local filesystem
	/// fetch.
	/// </summary>
	internal class LocalFetcher<K, V> : Fetcher<K, V>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.LocalFetcher
			));

		private static readonly MapHost Localhost = new MapHost("local", "local");

		private JobConf job;

		private IDictionary<TaskAttemptID, MapOutputFile> localMapFiles;

		public LocalFetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl<K, 
			V> scheduler, MergeManager<K, V> merger, Reporter reporter, ShuffleClientMetrics
			 metrics, ExceptionReporter exceptionReporter, SecretKey shuffleKey, IDictionary
			<TaskAttemptID, MapOutputFile> localMapFiles)
			: base(job, reduceId, scheduler, merger, reporter, metrics, exceptionReporter, shuffleKey
				)
		{
			this.job = job;
			this.localMapFiles = localMapFiles;
			SetName("localfetcher#" + id);
			SetDaemon(true);
		}

		public override void Run()
		{
			// Create a worklist of task attempts to work over.
			ICollection<TaskAttemptID> maps = new HashSet<TaskAttemptID>();
			foreach (TaskAttemptID map in localMapFiles.Keys)
			{
				maps.AddItem(map);
			}
			while (maps.Count > 0)
			{
				try
				{
					// If merge is on, block
					merger.WaitForResource();
					metrics.ThreadBusy();
					// Copy as much as is possible.
					DoCopy(maps);
					metrics.ThreadFree();
				}
				catch (Exception)
				{
				}
				catch (Exception t)
				{
					exceptionReporter.ReportException(t);
				}
			}
		}

		/// <summary>The crux of the matter...</summary>
		/// <exception cref="System.IO.IOException"/>
		private void DoCopy(ICollection<TaskAttemptID> maps)
		{
			IEnumerator<TaskAttemptID> iter = maps.GetEnumerator();
			while (iter.HasNext())
			{
				TaskAttemptID map = iter.Next();
				Log.Debug("LocalFetcher " + id + " going to fetch: " + map);
				if (CopyMapOutput(map))
				{
					// Successful copy. Remove this from our worklist.
					iter.Remove();
				}
				else
				{
					// We got back a WAIT command; go back to the outer loop
					// and block for InMemoryMerge.
					break;
				}
			}
		}

		/// <summary>
		/// Retrieve the map output of a single map task
		/// and send it to the merger.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private bool CopyMapOutput(TaskAttemptID mapTaskId)
		{
			// Figure out where the map task stored its output.
			Path mapOutputFileName = localMapFiles[mapTaskId].GetOutputFile();
			Path indexFileName = mapOutputFileName.Suffix(".index");
			// Read its index to determine the location of our split
			// and its size.
			SpillRecord sr = new SpillRecord(indexFileName, job);
			IndexRecord ir = sr.GetIndex(reduce);
			long compressedLength = ir.partLength;
			long decompressedLength = ir.rawLength;
			compressedLength -= CryptoUtils.CryptoPadding(job);
			decompressedLength -= CryptoUtils.CryptoPadding(job);
			// Get the location for the map output - either in-memory or on-disk
			MapOutput<K, V> mapOutput = merger.Reserve(mapTaskId, decompressedLength, id);
			// Check if we can shuffle *now* ...
			if (mapOutput == null)
			{
				Log.Info("fetcher#" + id + " - MergeManager returned Status.WAIT ...");
				return false;
			}
			// Go!
			Log.Info("localfetcher#" + id + " about to shuffle output of map " + mapOutput.GetMapId
				() + " decomp: " + decompressedLength + " len: " + compressedLength + " to " + mapOutput
				.GetDescription());
			// now read the file, seek to the appropriate section, and send it.
			FileSystem localFs = FileSystem.GetLocal(job).GetRaw();
			FSDataInputStream inStream = localFs.Open(mapOutputFileName);
			inStream = CryptoUtils.WrapIfNecessary(job, inStream);
			try
			{
				inStream.Seek(ir.startOffset + CryptoUtils.CryptoPadding(job));
				mapOutput.Shuffle(Localhost, inStream, compressedLength, decompressedLength, metrics
					, reporter);
			}
			finally
			{
				try
				{
					inStream.Close();
				}
				catch (IOException ioe)
				{
					Log.Warn("IOException closing inputstream from map output: " + ioe.ToString());
				}
			}
			scheduler.CopySucceeded(mapTaskId, Localhost, compressedLength, 0, 0, mapOutput);
			return true;
		}
		// successful fetch.
	}
}
