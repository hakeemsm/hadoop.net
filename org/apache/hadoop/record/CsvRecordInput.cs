using Sharpen;

namespace org.apache.hadoop.record
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class CsvRecordInput : org.apache.hadoop.record.RecordInput
	{
		private java.io.PushbackReader stream;

		private class CsvIndex : org.apache.hadoop.record.Index
		{
			public virtual bool done()
			{
				char c = '\0';
				try
				{
					c = (char)this._enclosing.stream.read();
					this._enclosing.stream.unread(c);
				}
				catch (System.IO.IOException)
				{
				}
				return (c == '}') ? true : false;
			}

			public virtual void incr()
			{
			}

			internal CsvIndex(CsvRecordInput _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CsvRecordInput _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private void throwExceptionOnError(string tag)
		{
			throw new System.IO.IOException("Error deserializing " + tag);
		}

		/// <exception cref="System.IO.IOException"/>
		private string readField(string tag)
		{
			try
			{
				java.lang.StringBuilder buf = new java.lang.StringBuilder();
				while (true)
				{
					char c = (char)stream.read();
					switch (c)
					{
						case ',':
						{
							return buf.ToString();
						}

						case '}':
						case '\n':
						case '\r':
						{
							stream.unread(c);
							return buf.ToString();
						}

						default:
						{
							buf.Append(c);
							break;
						}
					}
				}
			}
			catch (System.IO.IOException)
			{
				throw new System.IO.IOException("Error reading " + tag);
			}
		}

		/// <summary>Creates a new instance of CsvRecordInput</summary>
		public CsvRecordInput(java.io.InputStream @in)
		{
			try
			{
				stream = new java.io.PushbackReader(new java.io.InputStreamReader(@in, "UTF-8"));
			}
			catch (java.io.UnsupportedEncodingException ex)
			{
				throw new System.Exception(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte readByte(string tag)
		{
			return unchecked((byte)readLong(tag));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool readBool(string tag)
		{
			string sval = readField(tag);
			return "T".Equals(sval) ? true : false;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int readInt(string tag)
		{
			return (int)readLong(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long readLong(string tag)
		{
			string sval = readField(tag);
			try
			{
				long lval = long.Parse(sval);
				return lval;
			}
			catch (java.lang.NumberFormatException)
			{
				throw new System.IO.IOException("Error deserializing " + tag);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float readFloat(string tag)
		{
			return (float)readDouble(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual double readDouble(string tag)
		{
			string sval = readField(tag);
			try
			{
				double dval = double.parseDouble(sval);
				return dval;
			}
			catch (java.lang.NumberFormatException)
			{
				throw new System.IO.IOException("Error deserializing " + tag);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string readString(string tag)
		{
			string sval = readField(tag);
			return org.apache.hadoop.record.Utils.fromCSVString(sval);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Buffer readBuffer(string tag)
		{
			string sval = readField(tag);
			return org.apache.hadoop.record.Utils.fromCSVBuffer(sval);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startRecord(string tag)
		{
			if (tag != null && !tag.isEmpty())
			{
				char c1 = (char)stream.read();
				char c2 = (char)stream.read();
				if (c1 != 's' || c2 != '{')
				{
					throw new System.IO.IOException("Error deserializing " + tag);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endRecord(string tag)
		{
			char c = (char)stream.read();
			if (tag == null || tag.isEmpty())
			{
				if (c != '\n' && c != '\r')
				{
					throw new System.IO.IOException("Error deserializing record.");
				}
				else
				{
					return;
				}
			}
			if (c != '}')
			{
				throw new System.IO.IOException("Error deserializing " + tag);
			}
			c = (char)stream.read();
			if (c != ',')
			{
				stream.unread(c);
			}
			return;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Index startVector(string tag)
		{
			char c1 = (char)stream.read();
			char c2 = (char)stream.read();
			if (c1 != 'v' || c2 != '{')
			{
				throw new System.IO.IOException("Error deserializing " + tag);
			}
			return new org.apache.hadoop.record.CsvRecordInput.CsvIndex(this);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endVector(string tag)
		{
			char c = (char)stream.read();
			if (c != '}')
			{
				throw new System.IO.IOException("Error deserializing " + tag);
			}
			c = (char)stream.read();
			if (c != ',')
			{
				stream.unread(c);
			}
			return;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Index startMap(string tag)
		{
			char c1 = (char)stream.read();
			char c2 = (char)stream.read();
			if (c1 != 'm' || c2 != '{')
			{
				throw new System.IO.IOException("Error deserializing " + tag);
			}
			return new org.apache.hadoop.record.CsvRecordInput.CsvIndex(this);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endMap(string tag)
		{
			char c = (char)stream.read();
			if (c != '}')
			{
				throw new System.IO.IOException("Error deserializing " + tag);
			}
			c = (char)stream.read();
			if (c != ',')
			{
				stream.unread(c);
			}
			return;
		}
	}
}
