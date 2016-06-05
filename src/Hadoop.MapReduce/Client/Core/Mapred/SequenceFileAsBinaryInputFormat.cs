using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
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
		public override RecordReader<BytesWritable, BytesWritable> GetRecordReader(InputSplit
			 split, JobConf job, Reporter reporter)
		{
			return new SequenceFileAsBinaryInputFormat.SequenceFileAsBinaryRecordReader(job, 
				(FileSplit)split);
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

			/// <exception cref="System.IO.IOException"/>
			public SequenceFileAsBinaryRecordReader(Configuration conf, FileSplit split)
			{
				Path path = split.GetPath();
				FileSystem fs = path.GetFileSystem(conf);
				this.@in = new SequenceFile.Reader(fs, path, conf);
				this.end = split.GetStart() + split.GetLength();
				if (split.GetStart() > @in.GetPosition())
				{
					@in.Sync(split.GetStart());
				}
				// sync to start
				this.start = @in.GetPosition();
				vbytes = @in.CreateValueBytes();
				done = start >= end;
			}

			public virtual BytesWritable CreateKey()
			{
				return new BytesWritable();
			}

			public virtual BytesWritable CreateValue()
			{
				return new BytesWritable();
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
			public virtual bool Next(BytesWritable key, BytesWritable val)
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
						key.Set(buffer.GetData(), 0, buffer.GetLength());
						buffer.Reset();
						@in.NextRawValue(vbytes);
						vbytes.WriteUncompressedBytes(buffer);
						val.Set(buffer.GetData(), 0, buffer.GetLength());
						buffer.Reset();
					}
					return !(done = (eof || (pos >= end && @in.SyncSeen())));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				return @in.GetPosition();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				@in.Close();
			}

			/// <summary>Return the progress within the input split</summary>
			/// <returns>0.0 to 1.0 of the input byte range</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual float GetProgress()
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
