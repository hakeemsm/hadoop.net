using Sharpen;

namespace org.apache.hadoop.record
{
	/// <summary>XML Serializer.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class XmlRecordOutput : org.apache.hadoop.record.RecordOutput
	{
		private System.IO.TextWriter stream;

		private int indent = 0;

		private java.util.Stack<string> compoundStack;

		private void putIndent()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder(string.Empty);
			for (int idx = 0; idx < indent; idx++)
			{
				sb.Append("  ");
			}
			stream.Write(sb.ToString());
		}

		private void addIndent()
		{
			indent++;
		}

		private void closeIndent()
		{
			indent--;
		}

		private void printBeginEnvelope(string tag)
		{
			if (!compoundStack.empty())
			{
				string s = compoundStack.peek();
				if ("struct".Equals(s))
				{
					putIndent();
					stream.Write("<member>\n");
					addIndent();
					putIndent();
					stream.Write("<name>" + tag + "</name>\n");
					putIndent();
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

		private void printEndEnvelope(string tag)
		{
			if (!compoundStack.empty())
			{
				string s = compoundStack.peek();
				if ("struct".Equals(s))
				{
					stream.Write("</value>\n");
					closeIndent();
					putIndent();
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

		private void insideVector(string tag)
		{
			printBeginEnvelope(tag);
			compoundStack.push("vector");
		}

		/// <exception cref="System.IO.IOException"/>
		private void outsideVector(string tag)
		{
			string s = compoundStack.pop();
			if (!"vector".Equals(s))
			{
				throw new System.IO.IOException("Error serializing vector.");
			}
			printEndEnvelope(tag);
		}

		private void insideMap(string tag)
		{
			printBeginEnvelope(tag);
			compoundStack.push("map");
		}

		/// <exception cref="System.IO.IOException"/>
		private void outsideMap(string tag)
		{
			string s = compoundStack.pop();
			if (!"map".Equals(s))
			{
				throw new System.IO.IOException("Error serializing map.");
			}
			printEndEnvelope(tag);
		}

		private void insideRecord(string tag)
		{
			printBeginEnvelope(tag);
			compoundStack.push("struct");
		}

		/// <exception cref="System.IO.IOException"/>
		private void outsideRecord(string tag)
		{
			string s = compoundStack.pop();
			if (!"struct".Equals(s))
			{
				throw new System.IO.IOException("Error serializing record.");
			}
			printEndEnvelope(tag);
		}

		/// <summary>Creates a new instance of XmlRecordOutput</summary>
		public XmlRecordOutput(java.io.OutputStream @out)
		{
			try
			{
				stream = new System.IO.TextWriter(@out, true, "UTF-8");
				compoundStack = new java.util.Stack<string>();
			}
			catch (java.io.UnsupportedEncodingException ex)
			{
				throw new System.Exception(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeByte(byte b, string tag)
		{
			printBeginEnvelope(tag);
			stream.Write("<ex:i1>");
			stream.Write(byte.toString(b));
			stream.Write("</ex:i1>");
			printEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeBool(bool b, string tag)
		{
			printBeginEnvelope(tag);
			stream.Write("<boolean>");
			stream.Write(b ? "1" : "0");
			stream.Write("</boolean>");
			printEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeInt(int i, string tag)
		{
			printBeginEnvelope(tag);
			stream.Write("<i4>");
			stream.Write(int.toString(i));
			stream.Write("</i4>");
			printEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeLong(long l, string tag)
		{
			printBeginEnvelope(tag);
			stream.Write("<ex:i8>");
			stream.Write(System.Convert.ToString(l));
			stream.Write("</ex:i8>");
			printEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeFloat(float f, string tag)
		{
			printBeginEnvelope(tag);
			stream.Write("<ex:float>");
			stream.Write(float.toString(f));
			stream.Write("</ex:float>");
			printEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeDouble(double d, string tag)
		{
			printBeginEnvelope(tag);
			stream.Write("<double>");
			stream.Write(double.toString(d));
			stream.Write("</double>");
			printEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeString(string s, string tag)
		{
			printBeginEnvelope(tag);
			stream.Write("<string>");
			stream.Write(org.apache.hadoop.record.Utils.toXMLString(s));
			stream.Write("</string>");
			printEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeBuffer(org.apache.hadoop.record.Buffer buf, string tag)
		{
			printBeginEnvelope(tag);
			stream.Write("<string>");
			stream.Write(org.apache.hadoop.record.Utils.toXMLBuffer(buf));
			stream.Write("</string>");
			printEndEnvelope(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startRecord(org.apache.hadoop.record.Record r, string tag)
		{
			insideRecord(tag);
			stream.Write("<struct>\n");
			addIndent();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endRecord(org.apache.hadoop.record.Record r, string tag)
		{
			closeIndent();
			putIndent();
			stream.Write("</struct>");
			outsideRecord(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startVector(System.Collections.ArrayList v, string tag)
		{
			insideVector(tag);
			stream.Write("<array>\n");
			addIndent();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endVector(System.Collections.ArrayList v, string tag)
		{
			closeIndent();
			putIndent();
			stream.Write("</array>");
			outsideVector(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startMap(System.Collections.SortedList v, string tag)
		{
			insideMap(tag);
			stream.Write("<array>\n");
			addIndent();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endMap(System.Collections.SortedList v, string tag)
		{
			closeIndent();
			putIndent();
			stream.Write("</array>");
			outsideMap(tag);
		}
	}
}
