using System;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>A file-based set of keys.</summary>
	public class SetFile : MapFile
	{
		protected internal SetFile()
		{
		}

		/// <summary>Write a new set file.</summary>
		public class Writer : MapFile.Writer
		{
			/// <summary>Create the named set for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"pass a Configuration too")]
			public Writer(FileSystem fs, string dirName, Type keyClass)
				: base(new Configuration(), fs, dirName, keyClass, typeof(NullWritable))
			{
			}

			/// <summary>Create a set naming the element class and compression type.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, SequenceFile.CompressionType
				 compress)
				: this(conf, fs, dirName, WritableComparator.Get(keyClass, conf), compress)
			{
			}

			/// <summary>Create a set naming the element comparator and compression type.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, SequenceFile.CompressionType compress)
				: base(conf, new Path(dirName), Comparator(comparator), ValueClass(typeof(NullWritable
					)), Compression(compress))
			{
			}

			// no public ctor
			/// <summary>Append a key to a set.</summary>
			/// <remarks>
			/// Append a key to a set.  The key must be strictly greater than the
			/// previous key added to the set.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(WritableComparable key)
			{
				Append(key, NullWritable.Get());
			}
		}

		/// <summary>Provide access to an existing set file.</summary>
		public class Reader : MapFile.Reader
		{
			/// <summary>Construct a set reader for the named set.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Reader(FileSystem fs, string dirName, Configuration conf)
				: base(fs, dirName, conf)
			{
			}

			/// <summary>Construct a set reader for the named set using the named comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Reader(FileSystem fs, string dirName, WritableComparator comparator, Configuration
				 conf)
				: base(new Path(dirName), conf, Comparator(comparator))
			{
			}

			// javadoc inherited
			/// <exception cref="System.IO.IOException"/>
			public override bool Seek(WritableComparable key)
			{
				return base.Seek(key);
			}

			/// <summary>Read the next key in a set into <code>key</code>.</summary>
			/// <remarks>
			/// Read the next key in a set into <code>key</code>.  Returns
			/// true if such a key exists and false when at the end of the set.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(WritableComparable key)
			{
				return Next(key, NullWritable.Get());
			}

			/// <summary>Read the matching key from a set into <code>key</code>.</summary>
			/// <remarks>
			/// Read the matching key from a set into <code>key</code>.
			/// Returns <code>key</code>, or null if no match exists.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual WritableComparable Get(WritableComparable key)
			{
				if (Seek(key))
				{
					Next(key);
					return key;
				}
				else
				{
					return null;
				}
			}
		}
	}
}
