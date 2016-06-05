using System.Collections;
using System.IO;
using System.Text;


namespace Org.Apache.Hadoop.Record
{
	/// <summary>XML Serializer.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class XmlRecordOutput : RecordOutput
	{
		private TextWriter stream;

		private int indent = 0;

		private Stack<string> compoundStack;

		private void PutIndent()
		{
			StringBuilder sb = new StringBuilder(string.Empty);
			for (int idx = 0; idx < indent; idx++)
			{
				sb.Append("  ");
			}
			stream.Write(sb.ToString());
		}

		private void AddIndent()
		{
			indent++;
		}

		private void CloseIndent()
		{
			indent--;
		}

		private void PrintBeginEnvelope(string tag)
		{
			if (!compoundStack.Empty())
			{
				string s = compoundStack.Peek();
				if ("struct".Equals(s))
				{
					PutIndent();
					stream.Write("<member>\n");
					AddIndent();
					PutIndent();
					stream.Write("<name>" + tag + "</name>\n");
					PutIndent();
					stream.Write("<value>");
				}
				else
				{
					if ("vector".Equals(s))
					{
						stream.Write("<value>");
					}
					else
					{
						if ("map".Equals(s))
						{
							stream.Write("<value>");
						}
					}
				}
			}
			else
			{
				stream.Write("<value>");
			}
		}

		private void PrintEndEnvelope(string tag)
		{
			if (!compoundStack.Empty())
			{
				string s = compoundStack.Peek();
				if ("struct".Equals(s))
				{
					stream.Write("</value>\n");
					CloseIndent();
					PutIndent();
					stream.Write("</member>\n");
				}
				else
				{
					if ("vector".Equals(s))
					{
						stream.Write("</value>\n");
					}
					else
					{
						if ("map".Equals(s))
						{
							stream.Write("</value>\n");
						}
					}
				}
			}
			else
			{
				stream.Write("</value>\n");
			}
		}

		private void InsideVector(string tag)
		{
			PrintBeginEnvelope(tag);
			compoundStack.Push("vector");
		}

		/// <exception cref="System.IO.IOException"/>
		private void OutsideVector(string tag)
		{
			string s = compoundStack.Pop();
			if (!"vector".Equals(s))
			{
				throw new IOException("Error serializing vector.");
			}
			PrintEndEnvelope(tag);
		}

		private void InsideMap(string tag)
		{
			PrintBeginEnvelope(tag);
			compoundStack.Push("map");
		}

		/// <exception cref="System.IO.IOException"/>
		private void OutsideMap(string tag)
		{
			string s = compoundStack.Pop();
			if (!"map".Equals(s))
			{
				throw new IOException("Error serializing map.");
			}
			PrintEndEnvelope(tag);
		}

		private void InsideRecord(string tag)
		{
			PrintBeginEnvelope(tag);
			compoundStack.Push("struct");
		}

		/// <exception cref="System.IO.IOException"/>
		private void OutsideRecord(string tag)
		{
			string s = compoundStack.Pop();
			if (!"struct".Equals(s))
			{
				throw new IOException("Error serializing record.");
			}
			PrintEndEnvelope(tag);
		}

		/// <summary>Creates a new instance of XmlRecordOutput</summary>
		public XmlRecordOutput(OutputStream @out)
		{
			try
			{
				stream = new TextWriter(@out, true, "UTF-8");
				compoundStack = new Stack<string>();
			}
			catch (UnsupportedEncodingException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteByte(byte b, string tag)
		{
			PrintBeginEnvelope(tag);
			stream.Write("<ex:i1>");
			stream.Write(byte.ToString(b));
			stream.Write("</ex:i1>");
			PrintEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteBool(bool b, string tag)
		{
			PrintBeginEnvelope(tag);
			stream.Write("<boolean>");
			stream.Write(b ? "1" : "0");
			stream.Write("</boolean>");
			PrintEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteInt(int i, string tag)
		{
			PrintBeginEnvelope(tag);
			stream.Write("<i4>");
			stream.Write(Extensions.ToString(i));
			stream.Write("</i4>");
			PrintEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteLong(long l, string tag)
		{
			PrintBeginEnvelope(tag);
			stream.Write("<ex:i8>");
			stream.Write(System.Convert.ToString(l));
			stream.Write("</ex:i8>");
			PrintEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteFloat(float f, string tag)
		{
			PrintBeginEnvelope(tag);
			stream.Write("<ex:float>");
			stream.Write(float.ToString(f));
			stream.Write("</ex:float>");
			PrintEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteDouble(double d, string tag)
		{
			PrintBeginEnvelope(tag);
			stream.Write("<double>");
			stream.Write(double.ToString(d));
			stream.Write("</double>");
			PrintEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteString(string s, string tag)
		{
			PrintBeginEnvelope(tag);
			stream.Write("<string>");
			stream.Write(Utils.ToXMLString(s));
			stream.Write("</string>");
			PrintEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteBuffer(Buffer buf, string tag)
		{
			PrintBeginEnvelope(tag);
			stream.Write("<string>");
			stream.Write(Utils.ToXMLBuffer(buf));
			stream.Write("</string>");
			PrintEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartRecord(Org.Apache.Hadoop.Record.Record r, string tag)
		{
			InsideRecord(tag);
			stream.Write("<struct>\n");
			AddIndent();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndRecord(Org.Apache.Hadoop.Record.Record r, string tag)
		{
			CloseIndent();
			PutIndent();
			stream.Write("</struct>");
			OutsideRecord(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartVector(ArrayList v, string tag)
		{
			InsideVector(tag);
			stream.Write("<array>\n");
			AddIndent();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndVector(ArrayList v, string tag)
		{
			CloseIndent();
			PutIndent();
			stream.Write("</array>");
			OutsideVector(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartMap(SortedList v, string tag)
		{
			InsideMap(tag);
			stream.Write("<array>\n");
			AddIndent();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndMap(SortedList v, string tag)
		{
			CloseIndent();
			PutIndent();
			stream.Write("</array>");
			OutsideMap(tag);
		}
	}
}
