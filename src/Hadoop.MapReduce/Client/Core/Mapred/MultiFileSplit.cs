using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A sub-collection of input files.</summary>
	/// <remarks>
	/// A sub-collection of input files. Unlike
	/// <see cref="FileSplit"/>
	/// , MultiFileSplit
	/// class does not represent a split of a file, but a split of input files
	/// into smaller sets. The atomic unit of split is a file. <br />
	/// MultiFileSplit can be used to implement
	/// <see cref="RecordReader{K, V}"/>
	/// 's, with
	/// reading one record per file.
	/// </remarks>
	/// <seealso cref="FileSplit"/>
	/// <seealso cref="MultiFileInputFormat{K, V}"></seealso>
	public class MultiFileSplit : CombineFileSplit
	{
		internal MultiFileSplit()
		{
		}

		public MultiFileSplit(JobConf job, Path[] files, long[] lengths)
			: base(job, files, lengths)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override string[] GetLocations()
		{
			HashSet<string> hostSet = new HashSet<string>();
			foreach (Path file in GetPaths())
			{
				FileSystem fs = file.GetFileSystem(GetJob());
				FileStatus status = fs.GetFileStatus(file);
				BlockLocation[] blkLocations = fs.GetFileBlockLocations(status, 0, status.GetLen(
					));
				if (blkLocations != null && blkLocations.Length > 0)
				{
					AddToSet(hostSet, blkLocations[0].GetHosts());
				}
			}
			return Sharpen.Collections.ToArray(hostSet, new string[hostSet.Count]);
		}

		private void AddToSet(ICollection<string> set, string[] array)
		{
			foreach (string s in array)
			{
				set.AddItem(s);
			}
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < GetPaths().Length; i++)
			{
				sb.Append(GetPath(i).ToUri().GetPath() + ":0+" + GetLength(i));
				if (i < GetPaths().Length - 1)
				{
					sb.Append("\n");
				}
			}
			return sb.ToString();
		}
	}
}
