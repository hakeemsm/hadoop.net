using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A reader to read fixed length records from a split.</summary>
	/// <remarks>
	/// A reader to read fixed length records from a split.  Record offset is
	/// returned as key and the record as bytes is returned in value.
	/// </remarks>
	public class FixedLengthRecordReader : RecordReader<LongWritable, BytesWritable>
	{
		private int recordLength;

		private Org.Apache.Hadoop.Mapreduce.Lib.Input.FixedLengthRecordReader reader;

		/// <exception cref="System.IO.IOException"/>
		public FixedLengthRecordReader(Configuration job, FileSplit split, int recordLength
			)
		{
			// Make use of the new API implementation to avoid code duplication.
			this.recordLength = recordLength;
			reader = new Org.Apache.Hadoop.Mapreduce.Lib.Input.FixedLengthRecordReader(recordLength
				);
			reader.Initialize(job, split.GetStart(), split.GetLength(), split.GetPath());
		}

		public virtual LongWritable CreateKey()
		{
			return new LongWritable();
		}

		public virtual BytesWritable CreateValue()
		{
			return new BytesWritable(new byte[recordLength]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Next(LongWritable key, BytesWritable value)
		{
			lock (this)
			{
				bool dataRead = reader.NextKeyValue();
				if (dataRead)
				{
					LongWritable newKey = reader.GetCurrentKey();
					BytesWritable newValue = reader.GetCurrentValue();
					key.Set(newKey.Get());
					value.Set(newValue);
				}
				return dataRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float GetProgress()
		{
			return reader.GetProgress();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			lock (this)
			{
				return reader.GetPos();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			reader.Close();
		}
	}
}
