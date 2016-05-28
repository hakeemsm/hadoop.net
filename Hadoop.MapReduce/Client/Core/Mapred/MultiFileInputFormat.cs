using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// An abstract
	/// <see cref="InputFormat{K, V}"/>
	/// that returns
	/// <see cref="MultiFileSplit"/>
	/// 's
	/// in
	/// <see cref="MultiFileInputFormat{K, V}.GetSplits(JobConf, int)"/>
	/// method. Splits are constructed from
	/// the files under the input paths. Each split returned contains <i>nearly</i>
	/// equal content length. <br />
	/// Subclasses implement
	/// <see cref="MultiFileInputFormat{K, V}.GetRecordReader(InputSplit, JobConf, Reporter)
	/// 	"/>
	/// to construct <code>RecordReader</code>'s for <code>MultiFileSplit</code>'s.
	/// </summary>
	/// <seealso cref="MultiFileSplit"/>
	public abstract class MultiFileInputFormat<K, V> : FileInputFormat<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		public override InputSplit[] GetSplits(JobConf job, int numSplits)
		{
			Path[] paths = FileUtil.Stat2Paths(ListStatus(job));
			IList<MultiFileSplit> splits = new AList<MultiFileSplit>(Math.Min(numSplits, paths
				.Length));
			if (paths.Length != 0)
			{
				// HADOOP-1818: Manage splits only if there are paths
				long[] lengths = new long[paths.Length];
				long totLength = 0;
				for (int i = 0; i < paths.Length; i++)
				{
					FileSystem fs = paths[i].GetFileSystem(job);
					lengths[i] = fs.GetContentSummary(paths[i]).GetLength();
					totLength += lengths[i];
				}
				double avgLengthPerSplit = ((double)totLength) / numSplits;
				long cumulativeLength = 0;
				int startIndex = 0;
				for (int i_1 = 0; i_1 < numSplits; i_1++)
				{
					int splitSize = FindSize(i_1, avgLengthPerSplit, cumulativeLength, startIndex, lengths
						);
					if (splitSize != 0)
					{
						// HADOOP-1818: Manage split only if split size is not equals to 0
						Path[] splitPaths = new Path[splitSize];
						long[] splitLengths = new long[splitSize];
						System.Array.Copy(paths, startIndex, splitPaths, 0, splitSize);
						System.Array.Copy(lengths, startIndex, splitLengths, 0, splitSize);
						splits.AddItem(new MultiFileSplit(job, splitPaths, splitLengths));
						startIndex += splitSize;
						foreach (long l in splitLengths)
						{
							cumulativeLength += l;
						}
					}
				}
			}
			return Sharpen.Collections.ToArray(splits, new MultiFileSplit[splits.Count]);
		}

		private int FindSize(int splitIndex, double avgLengthPerSplit, long cumulativeLength
			, int startIndex, long[] lengths)
		{
			if (splitIndex == lengths.Length - 1)
			{
				return lengths.Length - startIndex;
			}
			long goalLength = (long)((splitIndex + 1) * avgLengthPerSplit);
			long partialLength = 0;
			// accumulate till just above the goal length;
			for (int i = startIndex; i < lengths.Length; i++)
			{
				partialLength += lengths[i];
				if (partialLength + cumulativeLength >= goalLength)
				{
					return i - startIndex + 1;
				}
			}
			return lengths.Length - startIndex;
		}

		/// <exception cref="System.IO.IOException"/>
		public abstract override RecordReader<K, V> GetRecordReader(InputSplit split, JobConf
			 job, Reporter reporter);
	}
}
