using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A file-based set of keys.</summary>
	public class SetFile : org.apache.hadoop.io.MapFile
	{
		protected internal SetFile()
		{
		}

		/// <summary>Write a new set file.</summary>
		public class Writer : org.apache.hadoop.io.MapFile.Writer
		{
			/// <summary>Create the named set for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"pass a Configuration too")]
			public Writer(org.apache.hadoop.fs.FileSystem fs, string dirName, java.lang.Class
				 keyClass)
				: base(new org.apache.hadoop.conf.Configuration(), fs, dirName, keyClass, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.NullWritable)))
			{
			}

			/// <summary>Create a set naming the element class and compression type.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress)
				: this(conf, fs, dirName, org.apache.hadoop.io.WritableComparator.get(keyClass, conf
					), compress)
			{
			}

			/// <summary>Create a set naming the element comparator and compression type.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress)
				: base(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.NullWritable))), compression
					(compress))
			{
			}

			// no public ctor
			/// <summary>Append a key to a set.</summary>
			/// <remarks>
			/// Append a key to a set.  The key must be strictly greater than the
			/// previous key added to the set.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void append(org.apache.hadoop.io.WritableComparable key)
			{
				append(key, org.apache.hadoop.io.NullWritable.get());
			}
		}

		/// <summary>Provide access to an existing set file.</summary>
		public class Reader : org.apache.hadoop.io.MapFile.Reader
		{
			/// <summary>Construct a set reader for the named set.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Reader(org.apache.hadoop.fs.FileSystem fs, string dirName, org.apache.hadoop.conf.Configuration
				 conf)
				: base(fs, dirName, conf)
			{
			}

			/// <summary>Construct a set reader for the named set using the named comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Reader(org.apache.hadoop.fs.FileSystem fs, string dirName, org.apache.hadoop.io.WritableComparator
				 comparator, org.apache.hadoop.conf.Configuration conf)
				: base(new org.apache.hadoop.fs.Path(dirName), conf, comparator(comparator))
			{
			}

			// javadoc inherited
			/// <exception cref="System.IO.IOException"/>
			public override bool seek(org.apache.hadoop.io.WritableComparable key)
			{
				return base.seek(key);
			}

			/// <summary>Read the next key in a set into <code>key</code>.</summary>
			/// <remarks>
			/// Read the next key in a set into <code>key</code>.  Returns
			/// true if such a key exists and false when at the end of the set.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool next(org.apache.hadoop.io.WritableComparable key)
			{
				return next(key, org.apache.hadoop.io.NullWritable.get());
			}

			/// <summary>Read the matching key from a set into <code>key</code>.</summary>
			/// <remarks>
			/// Read the matching key from a set into <code>key</code>.
			/// Returns <code>key</code>, or null if no match exists.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.WritableComparable get(org.apache.hadoop.io.WritableComparable
				 key)
			{
				if (seek(key))
				{
					next(key);
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
