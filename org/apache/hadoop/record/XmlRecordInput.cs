using Sharpen;

namespace org.apache.hadoop.record
{
	/// <summary>XML Deserializer.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class XmlRecordInput : org.apache.hadoop.record.RecordInput
	{
		private class Value
		{
			private string type;

			private System.Text.StringBuilder sb;

			public Value(string t)
			{
				type = t;
				sb = new System.Text.StringBuilder();
			}

			public virtual void addChars(char[] buf, int offset, int len)
			{
				sb.Append(buf, offset, len);
			}

			public virtual string getValue()
			{
				return sb.ToString();
			}

			public virtual string getType()
			{
				return type;
			}
		}

		private class XMLParser : org.xml.sax.helpers.DefaultHandler
		{
			private bool charsValid = false;

			private System.Collections.Generic.List<org.apache.hadoop.record.XmlRecordInput.Value
				> valList;

			private XMLParser(System.Collections.Generic.List<org.apache.hadoop.record.XmlRecordInput.Value
				> vlist)
			{
				valList = vlist;
			}

			/// <exception cref="org.xml.sax.SAXException"/>
			public override void startDocument()
			{
			}

			/// <exception cref="org.xml.sax.SAXException"/>
			public override void endDocument()
			{
			}

			/// <exception cref="org.xml.sax.SAXException"/>
			public override void startElement(string ns, string sname, string qname, org.xml.sax.Attributes
				 attrs)
			{
				charsValid = false;
				if ("boolean".Equals(qname) || "i4".Equals(qname) || "int".Equals(qname) || "string"
					.Equals(qname) || "double".Equals(qname) || "ex:i1".Equals(qname) || "ex:i8".Equals
					(qname) || "ex:float".Equals(qname))
				{
					charsValid = true;
					valList.add(new org.apache.hadoop.record.XmlRecordInput.Value(qname));
				}
				else
				{
					if ("struct".Equals(qname) || "array".Equals(qname))
					{
						valList.add(new org.apache.hadoop.record.XmlRecordInput.Value(qname));
					}
				}
			}

			/// <exception cref="org.xml.sax.SAXException"/>
			public override void endElement(string ns, string sname, string qname)
			{
				charsValid = false;
				if ("struct".Equals(qname) || "array".Equals(qname))
				{
					valList.add(new org.apache.hadoop.record.XmlRecordInput.Value("/" + qname));
				}
			}

			/// <exception cref="org.xml.sax.SAXException"/>
			public override void characters(char[] buf, int offset, int len)
			{
				if (charsValid)
				{
					org.apache.hadoop.record.XmlRecordInput.Value v = valList[valList.Count - 1];
					v.addChars(buf, offset, len);
				}
			}
		}

		private class XmlIndex : org.apache.hadoop.record.Index
		{
			public virtual bool done()
			{
				org.apache.hadoop.record.XmlRecordInput.Value v = this._enclosing.valList[this._enclosing
					.vIdx];
				if ("/array".Equals(v.getType()))
				{
					this._enclosing.valList.set(this._enclosing.vIdx, null);
					this._enclosing.vIdx++;
					return true;
				}
				else
				{
					return false;
				}
			}

			public virtual void incr()
			{
			}

			internal XmlIndex(XmlRecordInput _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly XmlRecordInput _enclosing;
		}

		private System.Collections.Generic.List<org.apache.hadoop.record.XmlRecordInput.Value
			> valList;

		private int vLen;

		private int vIdx;

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.record.XmlRecordInput.Value next()
		{
			if (vIdx < vLen)
			{
				org.apache.hadoop.record.XmlRecordInput.Value v = valList[vIdx];
				valList.set(vIdx, null);
				vIdx++;
				return v;
			}
			else
			{
				throw new System.IO.IOException("Error in deserialization.");
			}
		}

		/// <summary>Creates a new instance of XmlRecordInput</summary>
		public XmlRecordInput(java.io.InputStream @in)
		{
			try
			{
				valList = new System.Collections.Generic.List<org.apache.hadoop.record.XmlRecordInput.Value
					>();
				org.xml.sax.helpers.DefaultHandler handler = new org.apache.hadoop.record.XmlRecordInput.XMLParser
					(valList);
				javax.xml.parsers.SAXParserFactory factory = javax.xml.parsers.SAXParserFactory.newInstance
					();
				javax.xml.parsers.SAXParser parser = factory.newSAXParser();
				parser.parse(@in, handler);
				vLen = valList.Count;
				vIdx = 0;
			}
			catch (System.Exception ex)
			{
				throw new System.Exception(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte readByte(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"ex:i1".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return byte.parseByte(v.getValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool readBool(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"boolean".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return "1".Equals(v.getValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int readInt(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"i4".Equals(v.getType()) && !"int".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return System.Convert.ToInt32(v.getValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long readLong(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"ex:i8".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return long.Parse(v.getValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float readFloat(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"ex:float".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return float.parseFloat(v.getValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual double readDouble(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"double".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return double.parseDouble(v.getValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string readString(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"string".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return org.apache.hadoop.record.Utils.fromXMLString(v.getValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Buffer readBuffer(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"string".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return org.apache.hadoop.record.Utils.fromXMLBuffer(v.getValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void startRecord(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"struct".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endRecord(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"/struct".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Index startVector(string tag)
		{
			org.apache.hadoop.record.XmlRecordInput.Value v = next();
			if (!"array".Equals(v.getType()))
			{
				throw new System.IO.IOException("Error deserializing " + tag + ".");
			}
			return new org.apache.hadoop.record.XmlRecordInput.XmlIndex(this);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endVector(string tag)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.record.Index startMap(string tag)
		{
			return startVector(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void endMap(string tag)
		{
			endVector(tag);
		}
	}
}
