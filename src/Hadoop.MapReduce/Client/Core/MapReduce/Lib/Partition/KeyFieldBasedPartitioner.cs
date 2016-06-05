using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
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
	public class KeyFieldBasedPartitioner<K2, V2> : Partitioner<K2, V2>, Configurable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(KeyFieldBasedPartitioner
			).FullName);

		public static string PartitionerOptions = "mapreduce.partition.keypartitioner.options";

		private int numOfPartitionFields;

		private KeyFieldHelper keyFieldHelper = new KeyFieldHelper();

		private Configuration conf;

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
			keyFieldHelper = new KeyFieldHelper();
			string keyFieldSeparator = conf.Get(MRJobConfig.MapOutputKeyFieldSeperator, "\t");
			keyFieldHelper.SetKeyFieldSeparator(keyFieldSeparator);
			if (conf.Get("num.key.fields.for.partition") != null)
			{
				Log.Warn("Using deprecated num.key.fields.for.partition. " + "Use mapreduce.partition.keypartitioner.options instead"
					);
				this.numOfPartitionFields = conf.GetInt("num.key.fields.for.partition", 0);
				keyFieldHelper.SetKeyFieldSpec(1, numOfPartitionFields);
			}
			else
			{
				string option = conf.Get(PartitionerOptions);
				keyFieldHelper.ParseOption(option);
			}
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		public override int GetPartition(K2 key, V2 value, int numReduceTasks)
		{
			byte[] keyBytes;
			IList<KeyFieldHelper.KeyDescription> allKeySpecs = keyFieldHelper.KeySpecs();
			if (allKeySpecs.Count == 0)
			{
				return GetPartition(key.ToString().GetHashCode(), numReduceTasks);
			}
			try
			{
				keyBytes = Sharpen.Runtime.GetBytesForString(key.ToString(), "UTF-8");
			}
			catch (UnsupportedEncodingException e)
			{
				throw new RuntimeException("The current system does not " + "support UTF-8 encoding!"
					, e);
			}
			// return 0 if the key is empty
			if (keyBytes.Length == 0)
			{
				return 0;
			}
			int[] lengthIndicesFirst = keyFieldHelper.GetWordLengths(keyBytes, 0, keyBytes.Length
				);
			int currentHash = 0;
			foreach (KeyFieldHelper.KeyDescription keySpec in allKeySpecs)
			{
				int startChar = keyFieldHelper.GetStartOffset(keyBytes, 0, keyBytes.Length, lengthIndicesFirst
					, keySpec);
				// no key found! continue
				if (startChar < 0)
				{
					continue;
				}
				int endChar = keyFieldHelper.GetEndOffset(keyBytes, 0, keyBytes.Length, lengthIndicesFirst
					, keySpec);
				currentHash = HashCode(keyBytes, startChar, endChar, currentHash);
			}
			return GetPartition(currentHash, numReduceTasks);
		}

		protected internal virtual int HashCode(byte[] b, int start, int end, int currentHash
			)
		{
			for (int i = start; i <= end; i++)
			{
				currentHash = 31 * currentHash + b[i];
			}
			return currentHash;
		}

		protected internal virtual int GetPartition(int hash, int numReduceTasks)
		{
			return (hash & int.MaxValue) % numReduceTasks;
		}

		/// <summary>
		/// Set the
		/// <see cref="KeyFieldBasedPartitioner{K2, V2}"/>
		/// options used for
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Partitioner{KEY, VALUE}"/>
		/// </summary>
		/// <param name="keySpec">
		/// the key specification of the form -k pos1[,pos2], where,
		/// pos is of the form f[.c][opts], where f is the number
		/// of the key field to use, and c is the number of the first character from
		/// the beginning of the field. Fields and character posns are numbered
		/// starting with 1; a character position of zero in pos2 indicates the
		/// field's last character. If '.c' is omitted from pos1, it defaults to 1
		/// (the beginning of the field); if omitted from pos2, it defaults to 0
		/// (the end of the field).
		/// </param>
		public virtual void SetKeyFieldPartitionerOptions(Job job, string keySpec)
		{
			job.GetConfiguration().Set(PartitionerOptions, keySpec);
		}

		/// <summary>
		/// Get the
		/// <see cref="KeyFieldBasedPartitioner{K2, V2}"/>
		/// options
		/// </summary>
		public virtual string GetKeyFieldPartitionerOption(JobContext job)
		{
			return job.GetConfiguration().Get(PartitionerOptions);
		}
	}
}
