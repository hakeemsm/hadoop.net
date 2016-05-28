using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// InputFormat reading keys, values from SequenceFiles in binary (raw)
	/// format.
	/// </summary>
	public class SequenceFileAsBinaryInputFormat : SequenceFileInputFormat<BytesWritable
		, BytesWritable>
	{
		public SequenceFileAsBinaryInputFormat()
			: base()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<BytesWritable, BytesWritable> CreateRecordReader(InputSplit
			 split, TaskAttemptContext context)
		{
			return new SequenceFileAsBinaryInputFormat.SequenceFileAsBinaryRecordReader();
		}

		/// <summary>Read records from a SequenceFile as binary (raw) bytes.</summary>
		public class SequenceFileAsBinaryRecordReader : RecordReader<BytesWritable, BytesWritable
			>
		{
			private SequenceFile.Reader @in;

			private long start;

			private long end;

			private bool done = false;

			private DataOutputBuffer buffer = new DataOutputBuffer();

			private SequenceFile.ValueBytes vbytes;

			private BytesWritable key = null;

			private BytesWritable value = null;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
				Path path = ((FileSplit)split).GetPath();
				Configuration conf = context.GetConfiguration();
				FileSystem fs = path.GetFileSystem(conf);
				this.@in = new SequenceFile.Reader(fs, path, conf);
				this.end = ((FileSplit)split).GetStart() + split.GetLength();
				if (((FileSplit)split).GetStart() > @in.GetPosition())
				{
					@in.Sync(((FileSplit)split).GetStart());
				}
				// sync to start
				this.start = @in.GetPosition();
				vbytes = @in.CreateValueBytes();
				done = start >= end;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override BytesWritable GetCurrentKey()
			{
				return key;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override BytesWritable GetCurrentValue()
			{
				return value;
			}

			/// <summary>Retrieve the name of the key class for this SequenceFile.</summary>
			/// <seealso cref="Org.Apache.Hadoop.IO.SequenceFile.Reader.GetKeyClassName()"/>
			public virtual string GetKeyClassName()
			{
				return @in.GetKeyClassName();
			}

			/// <summary>Retrieve the name of the value class for this SequenceFile.</summary>
			/// <seealso cref="Org.Apache.Hadoop.IO.SequenceFile.Reader.GetValueClassName()"/>
			public virtual string GetValueClassName()
			{
				return @in.GetValueClassName();
			}

			/// <summary>Read raw bytes from a SequenceFile.</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKeyValue()
			{
				lock (this)
				{
					if (done)
					{
						return false;
					}
					long pos = @in.GetPosition();
					bool eof = -1 == @in.NextRawKey(buffer);
					if (!eof)
					{
						if (key == null)
						{
							key = new BytesWritable();
						}
						if (value == null)
						{
							value = new BytesWritable();
						}
						key.Set(buffer.GetData(), 0, buffer.GetLength());
						buffer.Reset();
						@in.NextRawValue(vbytes);
						vbytes.WriteUncompressedBytes(buffer);
						value.Set(buffer.GetData(), 0, buffer.GetLength());
						buffer.Reset();
					}
					return !(done = (eof || (pos >= end && @in.SyncSeen())));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				@in.Close();
			}

			/// <summary>Return the progress within the input split</summary>
			/// <returns>0.0 to 1.0 of the input byte range</returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override float GetProgress()
			{
				if (end == start)
				{
					return 0.0f;
				}
				else
				{
					return Math.Min(1.0f, (float)((@in.GetPosition() - start) / (double)(end - start)
						));
				}
			}
		}
	}
}
