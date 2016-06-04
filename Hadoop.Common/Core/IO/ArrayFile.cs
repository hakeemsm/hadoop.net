using System;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;

namespace Hadoop.Common.Core.IO
{
	/// <summary>A dense file-based mapping from integers to values.</summary>
	public class ArrayFile : MapFile
	{
		protected internal ArrayFile()
		{
		}

		/// <summary>Write a new array file.</summary>
		public class Writer : MapFile.Writer
		{
			private LongWritable count = new LongWritable(0);

			/// <summary>Create the named file for values of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Writer(Configuration conf, FileSystem fs, string file, Type valClass)
				: base(conf, new Path(file), KeyClass(typeof(LongWritable)), ValueClass(valClass)
					)
			{
			}

			/// <summary>Create the named file for values of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Writer(Configuration conf, FileSystem fs, string file, Type valClass, SequenceFile.CompressionType
				 compress, Progressable progress)
				: base(conf, new Path(file), KeyClass(typeof(LongWritable)), ValueClass(valClass)
					, Compression(compress), Progressable(progress))
			{
			}

			// no public ctor
			/// <summary>Append a value to the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(IWritable value)
			{
				lock (this)
				{
					base.Append(count, value);
					// add to map
					count.Set(count.Get() + 1);
				}
			}
			// increment count
		}

		/// <summary>Provide access to an existing array file.</summary>
		public class Reader : MapFile.Reader
		{
			private LongWritable key = new LongWritable();

			/// <summary>Construct an array reader for the named file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Reader(FileSystem fs, string file, Configuration conf)
				: base(new Path(file), conf)
			{
			}

			/// <summary>Positions the reader before its <code>n</code>th value.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Seek(long n)
			{
				lock (this)
				{
					key.Set(n);
					Seek(key);
				}
			}

			/// <summary>Read and return the next value in the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual IWritable Next(IWritable value)
			{
				lock (this)
				{
					return Next(key, value) ? value : null;
				}
			}

			/// <summary>
			/// Returns the key associated with the most recent call to
			/// <see cref="Seek(long)"/>
			/// ,
			/// <see cref="Next(IWritable)"/>
			/// , or
			/// <see cref="Get(long, IWritable)"/>
			/// .
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual long Key()
			{
				lock (this)
				{
					return key.Get();
				}
			}

			/// <summary>Return the <code>n</code>th value in the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual IWritable Get(long n, IWritable value)
			{
				lock (this)
				{
					key.Set(n);
					return Get(key, value);
				}
			}
		}
	}
}
