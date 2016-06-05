using System.Collections;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Record
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class BinaryRecordOutput : RecordOutput
	{
		private BinaryWriter @out;

		private BinaryRecordOutput()
		{
		}

		private void SetDataOutput(BinaryWriter @out)
		{
			this.@out = @out;
		}

		private sealed class _ThreadLocal_47 : ThreadLocal
		{
			public _ThreadLocal_47()
			{
			}

			protected override object InitialValue()
			{
				lock (this)
				{
					return new Org.Apache.Hadoop.Record.BinaryRecordOutput();
				}
			}
		}

		private static ThreadLocal bOut = new _ThreadLocal_47();

		/// <summary>Get a thread-local record output for the supplied BinaryWriter.</summary>
		/// <param name="out">data output stream</param>
		/// <returns>binary record output corresponding to the supplied BinaryWriter.</returns>
		public static Org.Apache.Hadoop.Record.BinaryRecordOutput Get(BinaryWriter @out)
		{
			Org.Apache.Hadoop.Record.BinaryRecordOutput bout = (Org.Apache.Hadoop.Record.BinaryRecordOutput
				)bOut.Get();
			bout.SetDataOutput(@out);
			return bout;
		}

		/// <summary>Creates a new instance of BinaryRecordOutput</summary>
		public BinaryRecordOutput(OutputStream @out)
		{
			this.@out = new DataOutputStream(@out);
		}

		/// <summary>Creates a new instance of BinaryRecordOutput</summary>
		public BinaryRecordOutput(BinaryWriter @out)
		{
			this.@out = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteByte(byte b, string tag)
		{
			@out.WriteByte(b);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteBool(bool b, string tag)
		{
			@out.WriteBoolean(b);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteInt(int i, string tag)
		{
			Utils.WriteVInt(@out, i);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteLong(long l, string tag)
		{
			Utils.WriteVLong(@out, l);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteFloat(float f, string tag)
		{
			@out.WriteFloat(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteDouble(double d, string tag)
		{
			@out.WriteDouble(d);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteString(string s, string tag)
		{
			Utils.ToBinaryString(@out, s);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteBuffer(Buffer buf, string tag)
		{
			byte[] barr = buf.Get();
			int len = buf.GetCount();
			Utils.WriteVInt(@out, len);
			@out.Write(barr, 0, len);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartRecord(Org.Apache.Hadoop.Record.Record r, string tag)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndRecord(Org.Apache.Hadoop.Record.Record r, string tag)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartVector(ArrayList v, string tag)
		{
			WriteInt(v.Count, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndVector(ArrayList v, string tag)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartMap(SortedList v, string tag)
		{
			WriteInt(v.Count, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndMap(SortedList v, string tag)
		{
		}
	}
}
