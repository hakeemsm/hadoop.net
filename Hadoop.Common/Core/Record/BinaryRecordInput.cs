using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Record
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class BinaryRecordInput : RecordInput
	{
		private DataInput @in;

		private class BinaryIndex : Index
		{
			private int nelems;

			private BinaryIndex(int nelems)
			{
				this.nelems = nelems;
			}

			public virtual bool Done()
			{
				return (nelems <= 0);
			}

			public virtual void Incr()
			{
				nelems--;
			}
		}

		private BinaryRecordInput()
		{
		}

		private void SetDataInput(DataInput inp)
		{
			this.@in = inp;
		}

		private sealed class _ThreadLocal_60 : ThreadLocal
		{
			public _ThreadLocal_60()
			{
			}

			protected override object InitialValue()
			{
				lock (this)
				{
					return new BinaryRecordInput();
				}
			}
		}

		private static ThreadLocal bIn = new _ThreadLocal_60();

		/// <summary>Get a thread-local record input for the supplied DataInput.</summary>
		/// <param name="inp">data input stream</param>
		/// <returns>binary record input corresponding to the supplied DataInput.</returns>
		public static BinaryRecordInput Get(DataInput inp)
		{
			BinaryRecordInput bin = (BinaryRecordInput)bIn.Get();
			bin.SetDataInput(inp);
			return bin;
		}

		/// <summary>Creates a new instance of BinaryRecordInput</summary>
		public BinaryRecordInput(InputStream strm)
		{
			this.@in = new DataInputStream(strm);
		}

		/// <summary>Creates a new instance of BinaryRecordInput</summary>
		public BinaryRecordInput(DataInput din)
		{
			this.@in = din;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte ReadByte(string tag)
		{
			return @in.ReadByte();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool ReadBool(string tag)
		{
			return @in.ReadBoolean();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int ReadInt(string tag)
		{
			return Utils.ReadVInt(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long ReadLong(string tag)
		{
			return Utils.ReadVLong(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float ReadFloat(string tag)
		{
			return @in.ReadFloat();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual double ReadDouble(string tag)
		{
			return @in.ReadDouble();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string ReadString(string tag)
		{
			return Utils.FromBinaryString(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Buffer ReadBuffer(string tag)
		{
			int len = Utils.ReadVInt(@in);
			byte[] barr = new byte[len];
			@in.ReadFully(barr);
			return new Buffer(barr);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartRecord(string tag)
		{
		}

		// no-op
		/// <exception cref="System.IO.IOException"/>
		public virtual void EndRecord(string tag)
		{
		}

		// no-op
		/// <exception cref="System.IO.IOException"/>
		public virtual Index StartVector(string tag)
		{
			return new BinaryRecordInput.BinaryIndex(ReadInt(tag));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndVector(string tag)
		{
		}

		// no-op
		/// <exception cref="System.IO.IOException"/>
		public virtual Index StartMap(string tag)
		{
			return new BinaryRecordInput.BinaryIndex(ReadInt(tag));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndMap(string tag)
		{
		}
		// no-op
	}
}
