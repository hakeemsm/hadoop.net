using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Partition;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// This comparator implementation provides a subset of the features provided
	/// by the Unix/GNU Sort.
	/// </summary>
	/// <remarks>
	/// This comparator implementation provides a subset of the features provided
	/// by the Unix/GNU Sort. In particular, the supported features are:
	/// -n, (Sort numerically)
	/// -r, (Reverse the result of comparison)
	/// -k pos1[,pos2], where pos is of the form f[.c][opts], where f is the number
	/// of the field to use, and c is the number of the first character from the
	/// beginning of the field. Fields and character posns are numbered starting
	/// with 1; a character position of zero in pos2 indicates the field's last
	/// character. If '.c' is omitted from pos1, it defaults to 1 (the beginning
	/// of the field); if omitted from pos2, it defaults to 0 (the end of the
	/// field). opts are ordering options (any of 'nr' as described above).
	/// We assume that the fields in the key are separated by
	/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.MapOutputKeyFieldSeperator"/>
	/// 
	/// </remarks>
	public class KeyFieldBasedComparator<K, V> : KeyFieldBasedComparator<K, V>, JobConfigurable
	{
		public virtual void Configure(JobConf job)
		{
			base.SetConf(job);
		}
	}
}
