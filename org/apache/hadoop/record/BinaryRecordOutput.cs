using Sharpen;

namespace org.apache.hadoop.record
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class BinaryRecordOutput : org.apache.hadoop.record.RecordOutput
	{
		private java.io.DataOutput @out;

		private BinaryRecordOutput()
		{
		}

		private void setDataOutput(java.io.DataOutput @out)
		{
			this.@out = @out;
		}

		private sealed class _ThreadLocal_47 : java.lang.ThreadLocal
		{
			public _ThreadLocal_47()
			{
			}

			protected override object initialValue()
			{
				lock (this)
				{
					return new org.apache.hadoop.record.BinaryRecordOutput();
				}
			}
		}

		private static java.lang.ThreadLocal bOut = new _ThreadLocal_47();

		/// <summary>Get a thread-local record output for the supplied DataOutput.</summary>
		/// <param name="out">data output stream</param>
		/// <returns>binary record output corresponding to the supplied DataOutput.</returns>
		public static org.apache.hadoop.record.BinaryRecordOutput get(java.io.DataOutput 
			@out)
		{
			org.apache.hadoop.record.BinaryRecordOutput bout = (org.apache.hadoop.record.BinaryRecordOutput
				)bOut.get();
			bout.setDataOutput(@out);
			return bout;
		}

		/// <summary>Creates a new instance of BinaryRecordOutput</summary>
		public BinaryRecordOutput(java.io.OutputStream @out)
		{
			this.@out = new java.io.DataOutputStream(@out);
		}

		/// <summary>Creates a new instance of BinaryRecordOutput</summary>
		public BinaryRecordOutput(java.io.DataOutput @out)
		{
			this.@out = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeByte(byte b, string tag)
		{
			@out.writeByte(b);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeBool(bool b, string tag)
		{
			@out.writeBoolean(b);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeInt(int i, string tag)
		{
			org.apache.hadoop.record.Utils.writeVInt(@out, i);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeLong(long l, string tag)
		{
			org.apache.hadoop.record.Utils.writeVLong(@out, l);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeFloat(float f, string tag)
		{
			@out.writeFloat(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeDouble(double d, string tag)
		{
			@out.writeDouble(d);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeString(string s, string tag)
		{
			org.apache.hadoop.record.Utils.toBinaryString(@out, s);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeBuffer(org.apache.hadoop.record.Buffer buf, string tag)
		{
			byte[] barr = buf.get();
			int len = buf.getCount();
			org.apache.hadoop.record.Utils.writeVInt(@out, len);
			@out.write(barr, 0, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startRecord(org.apache.hadoop.record.Record r, string tag)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endRecord(org.apache.hadoop.record.Record r, string tag)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startVector(System.Collections.ArrayList v, string tag)
		{
			writeInt(v.Count, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endVector(System.Collections.ArrayList v, string tag)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startMap(System.Collections.SortedList v, string tag)
		{
			writeInt(v.Count, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endMap(System.Collections.SortedList v, string tag)
		{
		}
	}
}
