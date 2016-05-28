using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>A class that allows a map/red job to work on a sample of sequence files.
	/// 	</summary>
	/// <remarks>
	/// A class that allows a map/red job to work on a sample of sequence files.
	/// The sample is decided by the filter class set by the job.
	/// </remarks>
	public class SequenceFileInputFilter<K, V> : SequenceFileInputFormat<K, V>
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(FileInputFormat));

		public const string FilterClass = "mapreduce.input.sequencefileinputfilter.class";

		public const string FilterFrequency = "mapreduce.input.sequencefileinputfilter.frequency";

		public const string FilterRegex = "mapreduce.input.sequencefileinputfilter.regex";

		public SequenceFileInputFilter()
		{
		}

		/// <summary>Create a record reader for the given split</summary>
		/// <param name="split">file split</param>
		/// <param name="context">the task-attempt context</param>
		/// <returns>RecordReader</returns>
		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<K, V> CreateRecordReader(InputSplit split, TaskAttemptContext
			 context)
		{
			context.SetStatus(split.ToString());
			return new SequenceFileInputFilter.FilterRecordReader<K, V>(context.GetConfiguration
				());
		}

		/// <summary>set the filter class</summary>
		/// <param name="job">The job</param>
		/// <param name="filterClass">filter class</param>
		public static void SetFilterClass(Job job, Type filterClass)
		{
			job.GetConfiguration().Set(FilterClass, filterClass.FullName);
		}

		/// <summary>filter interface</summary>
		public interface Filter : Configurable
		{
			/// <summary>
			/// filter function
			/// Decide if a record should be filtered or not
			/// </summary>
			/// <param name="key">record key</param>
			/// <returns>true if a record is accepted; return false otherwise</returns>
			bool Accept(object key);
		}

		/// <summary>base class for Filters</summary>
		public abstract class FilterBase : SequenceFileInputFilter.Filter
		{
			internal Configuration conf;

			public virtual Configuration GetConf()
			{
				return conf;
			}

			public abstract void SetConf(Configuration arg1);

			public abstract bool Accept(object arg1);
		}

		/// <summary>Records filter by matching key to regex</summary>
		public class RegexFilter : SequenceFileInputFilter.FilterBase
		{
			private Sharpen.Pattern p;

			/// <summary>Define the filtering regex and stores it in conf</summary>
			/// <param name="conf">where the regex is set</param>
			/// <param name="regex">regex used as a filter</param>
			/// <exception cref="Sharpen.PatternSyntaxException"/>
			public static void SetPattern(Configuration conf, string regex)
			{
				try
				{
					Sharpen.Pattern.Compile(regex);
				}
				catch (PatternSyntaxException)
				{
					throw new ArgumentException("Invalid pattern: " + regex);
				}
				conf.Set(FilterRegex, regex);
			}

			public RegexFilter()
			{
			}

			/// <summary>configure the Filter by checking the configuration</summary>
			public override void SetConf(Configuration conf)
			{
				string regex = conf.Get(FilterRegex);
				if (regex == null)
				{
					throw new RuntimeException(FilterRegex + "not set");
				}
				this.p = Sharpen.Pattern.Compile(regex);
				this.conf = conf;
			}

			/// <summary>
			/// Filtering method
			/// If key matches the regex, return true; otherwise return false
			/// </summary>
			/// <seealso cref="Filter.Accept(object)"/>
			public override bool Accept(object key)
			{
				return p.Matcher(key.ToString()).Matches();
			}
		}

		/// <summary>
		/// This class returns a percentage of records
		/// The percentage is determined by a filtering frequency <i>f</i> using
		/// the criteria record# % f == 0.
		/// </summary>
		/// <remarks>
		/// This class returns a percentage of records
		/// The percentage is determined by a filtering frequency <i>f</i> using
		/// the criteria record# % f == 0.
		/// For example, if the frequency is 10, one out of 10 records is returned.
		/// </remarks>
		public class PercentFilter : SequenceFileInputFilter.FilterBase
		{
			private int frequency;

			private int count;

			/// <summary>set the frequency and stores it in conf</summary>
			/// <param name="conf">configuration</param>
			/// <param name="frequency">filtering frequencey</param>
			public static void SetFrequency(Configuration conf, int frequency)
			{
				if (frequency <= 0)
				{
					throw new ArgumentException("Negative " + FilterFrequency + ": " + frequency);
				}
				conf.SetInt(FilterFrequency, frequency);
			}

			public PercentFilter()
			{
			}

			/// <summary>configure the filter by checking the configuration</summary>
			/// <param name="conf">configuration</param>
			public override void SetConf(Configuration conf)
			{
				this.frequency = conf.GetInt(FilterFrequency, 10);
				if (this.frequency <= 0)
				{
					throw new RuntimeException("Negative " + FilterFrequency + ": " + this.frequency);
				}
				this.conf = conf;
			}

			/// <summary>
			/// Filtering method
			/// If record# % frequency==0, return true; otherwise return false
			/// </summary>
			/// <seealso cref="Filter.Accept(object)"/>
			public override bool Accept(object key)
			{
				bool accepted = false;
				if (count == 0)
				{
					accepted = true;
				}
				if (++count == frequency)
				{
					count = 0;
				}
				return accepted;
			}
		}

		/// <summary>
		/// This class returns a set of records by examing the MD5 digest of its
		/// key against a filtering frequency <i>f</i>.
		/// </summary>
		/// <remarks>
		/// This class returns a set of records by examing the MD5 digest of its
		/// key against a filtering frequency <i>f</i>. The filtering criteria is
		/// MD5(key) % f == 0.
		/// </remarks>
		public class MD5Filter : SequenceFileInputFilter.FilterBase
		{
			private int frequency;

			private static readonly MessageDigest Digester;

			public const int Md5Len = 16;

			private byte[] digest = new byte[Md5Len];

			static MD5Filter()
			{
				try
				{
					Digester = MessageDigest.GetInstance("MD5");
				}
				catch (NoSuchAlgorithmException e)
				{
					throw new RuntimeException(e);
				}
			}

			/// <summary>set the filtering frequency in configuration</summary>
			/// <param name="conf">configuration</param>
			/// <param name="frequency">filtering frequency</param>
			public static void SetFrequency(Configuration conf, int frequency)
			{
				if (frequency <= 0)
				{
					throw new ArgumentException("Negative " + FilterFrequency + ": " + frequency);
				}
				conf.SetInt(FilterFrequency, frequency);
			}

			public MD5Filter()
			{
			}

			/// <summary>configure the filter according to configuration</summary>
			/// <param name="conf">configuration</param>
			public override void SetConf(Configuration conf)
			{
				this.frequency = conf.GetInt(FilterFrequency, 10);
				if (this.frequency <= 0)
				{
					throw new RuntimeException("Negative " + FilterFrequency + ": " + this.frequency);
				}
				this.conf = conf;
			}

			/// <summary>
			/// Filtering method
			/// If MD5(key) % frequency==0, return true; otherwise return false
			/// </summary>
			/// <seealso cref="Filter.Accept(object)"/>
			public override bool Accept(object key)
			{
				try
				{
					long hashcode;
					if (key is Text)
					{
						hashcode = MD5Hashcode((Text)key);
					}
					else
					{
						if (key is BytesWritable)
						{
							hashcode = MD5Hashcode((BytesWritable)key);
						}
						else
						{
							ByteBuffer bb;
							bb = Text.Encode(key.ToString());
							hashcode = MD5Hashcode(((byte[])bb.Array()), 0, bb.Limit());
						}
					}
					if (hashcode / frequency * frequency == hashcode)
					{
						return true;
					}
				}
				catch (Exception e)
				{
					Log.Warn(e);
					throw new RuntimeException(e);
				}
				return false;
			}

			/// <exception cref="Sharpen.DigestException"/>
			private long MD5Hashcode(Text key)
			{
				return MD5Hashcode(key.GetBytes(), 0, key.GetLength());
			}

			/// <exception cref="Sharpen.DigestException"/>
			private long MD5Hashcode(BytesWritable key)
			{
				return MD5Hashcode(key.GetBytes(), 0, key.GetLength());
			}

			/// <exception cref="Sharpen.DigestException"/>
			private long MD5Hashcode(byte[] bytes, int start, int length)
			{
				lock (this)
				{
					Digester.Update(bytes, 0, length);
					Digester.Digest(digest, 0, Md5Len);
					long hashcode = 0;
					for (int i = 0; i < 8; i++)
					{
						hashcode |= ((digest[i] & unchecked((long)(0xffL))) << (8 * (7 - i)));
					}
					return hashcode;
				}
			}
		}

		private class FilterRecordReader<K, V> : SequenceFileRecordReader<K, V>
		{
			private SequenceFileInputFilter.Filter filter;

			private K key;

			private V value;

			/// <exception cref="System.IO.IOException"/>
			public FilterRecordReader(Configuration conf)
				: base()
			{
				// instantiate filter
				filter = (SequenceFileInputFilter.Filter)ReflectionUtils.NewInstance(conf.GetClass
					(FilterClass, typeof(SequenceFileInputFilter.PercentFilter)), conf);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKeyValue()
			{
				lock (this)
				{
					while (base.NextKeyValue())
					{
						key = base.GetCurrentKey();
						if (filter.Accept(key))
						{
							value = base.GetCurrentValue();
							return true;
						}
					}
					return false;
				}
			}

			public override K GetCurrentKey()
			{
				return key;
			}

			public override V GetCurrentValue()
			{
				return value;
			}
		}
	}
}
