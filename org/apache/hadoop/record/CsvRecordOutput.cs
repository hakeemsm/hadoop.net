using Sharpen;

namespace org.apache.hadoop.record
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class CsvRecordOutput : org.apache.hadoop.record.RecordOutput
	{
		private System.IO.TextWriter stream;

		private bool isFirst = true;

		/// <exception cref="System.IO.IOException"/>
		private void throwExceptionOnError(string tag)
		{
			if (stream.checkError())
			{
				throw new System.IO.IOException("Error serializing " + tag);
			}
		}

		private void printCommaUnlessFirst()
		{
			if (!isFirst)
			{
				stream.Write(",");
			}
			isFirst = false;
		}

		/// <summary>Creates a new instance of CsvRecordOutput</summary>
		public CsvRecordOutput(java.io.OutputStream @out)
		{
			try
			{
				stream = new System.IO.TextWriter(@out, true, "UTF-8");
			}
			catch (java.io.UnsupportedEncodingException ex)
			{
				throw new System.Exception(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeByte(byte b, string tag)
		{
			writeLong((long)b, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeBool(bool b, string tag)
		{
			printCommaUnlessFirst();
			string val = b ? "T" : "F";
			stream.Write(val);
			throwExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeInt(int i, string tag)
		{
			writeLong((long)i, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeLong(long l, string tag)
		{
			printCommaUnlessFirst();
			stream.Write(l);
			throwExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeFloat(float f, string tag)
		{
			writeDouble((double)f, tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeDouble(double d, string tag)
		{
			printCommaUnlessFirst();
			stream.Write(d);
			throwExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeString(string s, string tag)
		{
			printCommaUnlessFirst();
			stream.Write(org.apache.hadoop.record.Utils.toCSVString(s));
			throwExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeBuffer(org.apache.hadoop.record.Buffer buf, string tag)
		{
			printCommaUnlessFirst();
			stream.Write(org.apache.hadoop.record.Utils.toCSVBuffer(buf));
			throwExceptionOnError(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startRecord(org.apache.hadoop.record.Record r, string tag)
		{
			if (tag != null && !tag.isEmpty())
			{
				printCommaUnlessFirst();
				stream.Write("s{");
				isFirst = true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endRecord(org.apache.hadoop.record.Record r, string tag)
		{
			if (tag == null || tag.isEmpty())
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
		public virtual void startVector(System.Collections.ArrayList v, string tag)
		{
			printCommaUnlessFirst();
			stream.Write("v{");
			isFirst = true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endVector(System.Collections.ArrayList v, string tag)
		{
			stream.Write("}");
			isFirst = false;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startMap(System.Collections.SortedList v, string tag)
		{
			printCommaUnlessFirst();
			stream.Write("m{");
			isFirst = true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endMap(System.Collections.SortedList v, string tag)
		{
			stream.Write("}");
			isFirst = false;
		}
	}
}
