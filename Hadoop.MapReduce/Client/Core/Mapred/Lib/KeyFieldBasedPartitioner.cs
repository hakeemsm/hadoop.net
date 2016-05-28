using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Partition;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// Defines a way to partition keys based on certain key fields (also see
	/// <see cref="KeyFieldBasedComparator{K, V}"/>
	/// .
	/// The key specification supported is of the form -k pos1[,pos2], where,
	/// pos is of the form f[.c][opts], where f is the number
	/// of the key field to use, and c is the number of the first character from
	/// the beginning of the field. Fields and character posns are numbered
	/// starting with 1; a character position of zero in pos2 indicates the
	/// field's last character. If '.c' is omitted from pos1, it defaults to 1
	/// (the beginning of the field); if omitted from pos2, it defaults to 0
	/// (the end of the field).
	/// </summary>
	public class KeyFieldBasedPartitioner<K2, V2> : KeyFieldBasedPartitioner<K2, V2>, 
		Partitioner<K2, V2>
	{
		public virtual void Configure(JobConf job)
		{
			base.SetConf(job);
		}
	}
}
