using System.Collections;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Record
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class CsvRecordOutput : RecordOutput
	{
		private TextWriter stream;

		private bool isFirst = true;

		/// <exception cref="System.IO.IOException"/>
		private void ThrowExceptionOnError(string tag)
		{
			if (stream.CheckError())
			{
				throw new IOException("Error serializing " + tag);
			}
		}

		private void PrintCommaUnlessFirst()
		{
			if (!isFirst)
			{
				stream.Write(",");
			}
			isFirst = false;
		}

		/// <summary>Creates a new instance of CsvRecordOutput</summary>
		public CsvRecordOutput(OutputStream @out)
		{
			try
			{
				stream = new TextWriter(@out, true, "UTF-8");
			}
			catch (UnsupportedEncodingException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteByte(byte b, string tag)
		{
			WriteLong((long)b, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteBool(bool b, string tag)
		{
			PrintCommaUnlessFirst();
			string val = b ? "T" : "F";
			stream.Write(val);
			ThrowExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteInt(int i, string tag)
		{
			WriteLong((long)i, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteLong(long l, string tag)
		{
			PrintCommaUnlessFirst();
			stream.Write(l);
			ThrowExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteFloat(float f, string tag)
		{
			WriteDouble((double)f, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteDouble(double d, string tag)
		{
			PrintCommaUnlessFirst();
			stream.Write(d);
			ThrowExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteString(string s, string tag)
		{
			PrintCommaUnlessFirst();
			stream.Write(Utils.ToCSVString(s));
			ThrowExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteBuffer(Buffer buf, string tag)
		{
			PrintCommaUnlessFirst();
			stream.Write(Utils.ToCSVBuffer(buf));
			ThrowExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartRecord(Org.Apache.Hadoop.Record.Record r, string tag)
		{
			if (tag != null && !tag.IsEmpty())
			{
				PrintCommaUnlessFirst();
				stream.Write("s{");
				isFirst = true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndRecord(Org.Apache.Hadoop.Record.Record r, string tag)
		{
			if (tag == null || tag.IsEmpty())
			{
				stream.Write("\n");
				isFirst = true;
			}
			else
			{
				stream.Write("}");
				isFirst = false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartVector(ArrayList v, string tag)
		{
			PrintCommaUnlessFirst();
			stream.Write("v{");
			isFirst = true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndVector(ArrayList v, string tag)
		{
			stream.Write("}");
			isFirst = false;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartMap(SortedList v, string tag)
		{
			PrintCommaUnlessFirst();
			stream.Write("m{");
			isFirst = true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndMap(SortedList v, string tag)
		{
			stream.Write("}");
			isFirst = false;
		}
	}
}
