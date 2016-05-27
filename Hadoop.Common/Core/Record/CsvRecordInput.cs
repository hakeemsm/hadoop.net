using System;
using System.IO;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Record
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class CsvRecordInput : RecordInput
	{
		private PushbackReader stream;

		private class CsvIndex : Index
		{
			public virtual bool Done()
			{
				char c = '\0';
				try
				{
					c = (char)this._enclosing.stream.Read();
					this._enclosing.stream.Unread(c);
				}
				catch (IOException)
				{
				}
				return (c == '}') ? true : false;
			}

			public virtual void Incr()
			{
			}

			internal CsvIndex(CsvRecordInput _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CsvRecordInput _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private void ThrowExceptionOnError(string tag)
		{
			throw new IOException("Error deserializing " + tag);
		}

		/// <exception cref="System.IO.IOException"/>
		private string ReadField(string tag)
		{
			try
			{
				StringBuilder buf = new StringBuilder();
				while (true)
				{
					char c = (char)stream.Read();
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
							stream.Unread(c);
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
			catch (IOException)
			{
				throw new IOException("Error reading " + tag);
			}
		}

		/// <summary>Creates a new instance of CsvRecordInput</summary>
		public CsvRecordInput(InputStream @in)
		{
			try
			{
				stream = new PushbackReader(new InputStreamReader(@in, "UTF-8"));
			}
			catch (UnsupportedEncodingException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte ReadByte(string tag)
		{
			return unchecked((byte)ReadLong(tag));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool ReadBool(string tag)
		{
			string sval = ReadField(tag);
			return "T".Equals(sval) ? true : false;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int ReadInt(string tag)
		{
			return (int)ReadLong(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long ReadLong(string tag)
		{
			string sval = ReadField(tag);
			try
			{
				long lval = long.Parse(sval);
				return lval;
			}
			catch (FormatException)
			{
				throw new IOException("Error deserializing " + tag);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float ReadFloat(string tag)
		{
			return (float)ReadDouble(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual double ReadDouble(string tag)
		{
			string sval = ReadField(tag);
			try
			{
				double dval = double.ParseDouble(sval);
				return dval;
			}
			catch (FormatException)
			{
				throw new IOException("Error deserializing " + tag);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string ReadString(string tag)
		{
			string sval = ReadField(tag);
			return Utils.FromCSVString(sval);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Buffer ReadBuffer(string tag)
		{
			string sval = ReadField(tag);
			return Utils.FromCSVBuffer(sval);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartRecord(string tag)
		{
			if (tag != null && !tag.IsEmpty())
			{
				char c1 = (char)stream.Read();
				char c2 = (char)stream.Read();
				if (c1 != 's' || c2 != '{')
				{
					throw new IOException("Error deserializing " + tag);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndRecord(string tag)
		{
			char c = (char)stream.Read();
			if (tag == null || tag.IsEmpty())
			{
				if (c != '\n' && c != '\r')
				{
					throw new IOException("Error deserializing record.");
				}
				else
				{
					return;
				}
			}
			if (c != '}')
			{
				throw new IOException("Error deserializing " + tag);
			}
			c = (char)stream.Read();
			if (c != ',')
			{
				stream.Unread(c);
			}
			return;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Index StartVector(string tag)
		{
			char c1 = (char)stream.Read();
			char c2 = (char)stream.Read();
			if (c1 != 'v' || c2 != '{')
			{
				throw new IOException("Error deserializing " + tag);
			}
			return new CsvRecordInput.CsvIndex(this);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndVector(string tag)
		{
			char c = (char)stream.Read();
			if (c != '}')
			{
				throw new IOException("Error deserializing " + tag);
			}
			c = (char)stream.Read();
			if (c != ',')
			{
				stream.Unread(c);
			}
			return;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Index StartMap(string tag)
		{
			char c1 = (char)stream.Read();
			char c2 = (char)stream.Read();
			if (c1 != 'm' || c2 != '{')
			{
				throw new IOException("Error deserializing " + tag);
			}
			return new CsvRecordInput.CsvIndex(this);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndMap(string tag)
		{
			char c = (char)stream.Read();
			if (c != '}')
			{
				throw new IOException("Error deserializing " + tag);
			}
			c = (char)stream.Read();
			if (c != ',')
			{
				stream.Unread(c);
			}
			return;
		}
	}
}
