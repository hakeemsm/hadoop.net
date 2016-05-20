using Sharpen;

namespace org.apache.hadoop.record
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class BinaryRecordInput : org.apache.hadoop.record.RecordInput
	{
		private java.io.DataInput @in;

		private class BinaryIndex : org.apache.hadoop.record.Index
		{
			private int nelems;

			private BinaryIndex(int nelems)
			{
				this.nelems = nelems;
			}

			public virtual bool done()
			{
				return (nelems <= 0);
			}

			public virtual void incr()
			{
				nelems--;
			}
		}

		private BinaryRecordInput()
		{
		}

		private void setDataInput(java.io.DataInput inp)
		{
			this.@in = inp;
		}

		private sealed class _ThreadLocal_60 : java.lang.ThreadLocal
		{
			public _ThreadLocal_60()
			{
			}

			protected override object initialValue()
			{
				lock (this)
				{
					return new org.apache.hadoop.record.BinaryRecordInput();
				}
			}
		}

		private static java.lang.ThreadLocal bIn = new _ThreadLocal_60();

		/// <summary>Get a thread-local record input for the supplied DataInput.</summary>
		/// <param name="inp">data input stream</param>
		/// <returns>binary record input corresponding to the supplied DataInput.</returns>
		public static org.apache.hadoop.record.BinaryRecordInput get(java.io.DataInput inp
			)
		{
			org.apache.hadoop.record.BinaryRecordInput bin = (org.apache.hadoop.record.BinaryRecordInput
				)bIn.get();
			bin.setDataInput(inp);
			return bin;
		}

		/// <summary>Creates a new instance of BinaryRecordInput</summary>
		public BinaryRecordInput(java.io.InputStream strm)
		{
			this.@in = new java.io.DataInputStream(strm);
		}

		/// <summary>Creates a new instance of BinaryRecordInput</summary>
		public BinaryRecordInput(java.io.DataInput din)
		{
			this.@in = din;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte readByte(string tag)
		{
			return @in.readByte();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool readBool(string tag)
		{
			return @in.readBoolean();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int readInt(string tag)
		{
			return org.apache.hadoop.record.Utils.readVInt(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long readLong(string tag)
		{
			return org.apache.hadoop.record.Utils.readVLong(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float readFloat(string tag)
		{
			return @in.readFloat();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual double readDouble(string tag)
		{
			return @in.readDouble();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string readString(string tag)
		{
			return org.apache.hadoop.record.Utils.fromBinaryString(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Buffer readBuffer(string tag)
		{
			int len = org.apache.hadoop.record.Utils.readVInt(@in);
			byte[] barr = new byte[len];
			@in.readFully(barr);
			return new org.apache.hadoop.record.Buffer(barr);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startRecord(string tag)
		{
		}

		// no-op
		/// <exception cref="System.IO.IOException"/>
		public virtual void endRecord(string tag)
		{
		}

		// no-op
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Index startVector(string tag)
		{
			return new org.apache.hadoop.record.BinaryRecordInput.BinaryIndex(readInt(tag));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endVector(string tag)
		{
		}

		// no-op
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Index startMap(string tag)
		{
			return new org.apache.hadoop.record.BinaryRecordInput.BinaryIndex(readInt(tag));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endMap(string tag)
		{
		}
		// no-op
	}
}
