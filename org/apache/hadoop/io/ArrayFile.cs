using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A dense file-based mapping from integers to values.</summary>
	public class ArrayFile : org.apache.hadoop.io.MapFile
	{
		protected internal ArrayFile()
		{
		}

		/// <summary>Write a new array file.</summary>
		public class Writer : org.apache.hadoop.io.MapFile.Writer
		{
			private org.apache.hadoop.io.LongWritable count = new org.apache.hadoop.io.LongWritable
				(0);

			/// <summary>Create the named file for values of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string file, java.lang.Class valClass)
				: base(conf, new org.apache.hadoop.fs.Path(file), keyClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.LongWritable))), valueClass(valClass))
			{
			}

			/// <summary>Create the named file for values of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string file, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress, org.apache.hadoop.util.Progressable progress)
				: base(conf, new org.apache.hadoop.fs.Path(file), keyClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.LongWritable))), valueClass(valClass), compression(
					compress), progressable(progress))
			{
			}

			// no public ctor
			/// <summary>Append a value to the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void append(org.apache.hadoop.io.Writable value)
			{
				lock (this)
				{
					base.append(count, value);
					// add to map
					count.set(count.get() + 1);
				}
			}
			// increment count
		}

		/// <summary>Provide access to an existing array file.</summary>
		public class Reader : org.apache.hadoop.io.MapFile.Reader
		{
			private org.apache.hadoop.io.LongWritable key = new org.apache.hadoop.io.LongWritable
				();

			/// <summary>Construct an array reader for the named file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public Reader(org.apache.hadoop.fs.FileSystem fs, string file, org.apache.hadoop.conf.Configuration
				 conf)
				: base(new org.apache.hadoop.fs.Path(file), conf)
			{
			}

			/// <summary>Positions the reader before its <code>n</code>th value.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void seek(long n)
			{
				lock (this)
				{
					key.set(n);
					seek(key);
				}
			}

			/// <summary>Read and return the next value in the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.Writable next(org.apache.hadoop.io.Writable value
				)
			{
				lock (this)
				{
					return next(key, value) ? value : null;
				}
			}

			/// <summary>
			/// Returns the key associated with the most recent call to
			/// <see cref="seek(long)"/>
			/// ,
			/// <see cref="next(Writable)"/>
			/// , or
			/// <see cref="get(long, Writable)"/>
			/// .
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual long key()
			{
				lock (this)
				{
					return key.get();
				}
			}

			/// <summary>Return the <code>n</code>th value in the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.Writable get(long n, org.apache.hadoop.io.Writable
				 value)
			{
				lock (this)
				{
					key.set(n);
					return get(key, value);
				}
			}
		}
	}
}
