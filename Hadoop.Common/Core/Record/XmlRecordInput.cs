using System;
using System.IO;
using System.Text;
using Javax.Xml.Parsers;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;


namespace Org.Apache.Hadoop.Record
{
	/// <summary>XML Deserializer.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class XmlRecordInput : RecordInput
	{
		private class Value
		{
			private string type;

			private StringBuilder sb;

			public Value(string t)
			{
				type = t;
				sb = new StringBuilder();
			}

			public virtual void AddChars(char[] buf, int offset, int len)
			{
				sb.Append(buf, offset, len);
			}

			public virtual string GetValue()
			{
				return sb.ToString();
			}

			public virtual string GetType()
			{
				return type;
			}
		}

		private class XMLParser : DefaultHandler
		{
			private bool charsValid = false;

			private AList<XmlRecordInput.Value> valList;

			private XMLParser(AList<XmlRecordInput.Value> vlist)
			{
				valList = vlist;
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void StartDocument()
			{
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void EndDocument()
			{
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void StartElement(string ns, string sname, string qname, Attributes
				 attrs)
			{
				charsValid = false;
				if ("boolean".Equals(qname) || "i4".Equals(qname) || "int".Equals(qname) || "string"
					.Equals(qname) || "double".Equals(qname) || "ex:i1".Equals(qname) || "ex:i8".Equals
					(qname) || "ex:float".Equals(qname))
				{
					charsValid = true;
					valList.AddItem(new XmlRecordInput.Value(qname));
				}
				else
				{
					if ("struct".Equals(qname) || "array".Equals(qname))
					{
						valList.AddItem(new XmlRecordInput.Value(qname));
					}
				}
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void EndElement(string ns, string sname, string qname)
			{
				charsValid = false;
				if ("struct".Equals(qname) || "array".Equals(qname))
				{
					valList.AddItem(new XmlRecordInput.Value("/" + qname));
				}
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void Characters(char[] buf, int offset, int len)
			{
				if (charsValid)
				{
					XmlRecordInput.Value v = valList[valList.Count - 1];
					v.AddChars(buf, offset, len);
				}
			}
		}

		private class XmlIndex : Index
		{
			public virtual bool Done()
			{
				XmlRecordInput.Value v = this._enclosing.valList[this._enclosing.vIdx];
				if ("/array".Equals(v.GetType()))
				{
					this._enclosing.valList.Set(this._enclosing.vIdx, null);
					this._enclosing.vIdx++;
					return true;
				}
				else
				{
					return false;
				}
			}

			public virtual void Incr()
			{
			}

			internal XmlIndex(XmlRecordInput _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly XmlRecordInput _enclosing;
		}

		private AList<XmlRecordInput.Value> valList;

		private int vLen;

		private int vIdx;

		/// <exception cref="System.IO.IOException"/>
		private XmlRecordInput.Value Next()
		{
			if (vIdx < vLen)
			{
				XmlRecordInput.Value v = valList[vIdx];
				valList.Set(vIdx, null);
				vIdx++;
				return v;
			}
			else
			{
				throw new IOException("Error in deserialization.");
			}
		}

		/// <summary>Creates a new instance of XmlRecordInput</summary>
		public XmlRecordInput(InputStream @in)
		{
			try
			{
				valList = new AList<XmlRecordInput.Value>();
				DefaultHandler handler = new XmlRecordInput.XMLParser(valList);
				SAXParserFactory factory = SAXParserFactory.NewInstance();
				SAXParser parser = factory.NewSAXParser();
				parser.Parse(@in, handler);
				vLen = valList.Count;
				vIdx = 0;
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte ReadByte(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"ex:i1".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return byte.ParseByte(v.GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool ReadBool(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"boolean".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return "1".Equals(v.GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int ReadInt(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"i4".Equals(v.GetType()) && !"int".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return System.Convert.ToInt32(v.GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long ReadLong(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"ex:i8".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return long.Parse(v.GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual float ReadFloat(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"ex:float".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return float.ParseFloat(v.GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual double ReadDouble(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"double".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return double.ParseDouble(v.GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string ReadString(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"string".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return Utils.FromXMLString(v.GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Buffer ReadBuffer(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"string".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return Utils.FromXMLBuffer(v.GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartRecord(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"struct".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndRecord(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"/struct".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Index StartVector(string tag)
		{
			XmlRecordInput.Value v = Next();
			if (!"array".Equals(v.GetType()))
			{
				throw new IOException("Error deserializing " + tag + ".");
			}
			return new XmlRecordInput.XmlIndex(this);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndVector(string tag)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Index StartMap(string tag)
		{
			return StartVector(tag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndMap(string tag)
		{
			EndVector(tag);
		}
	}
}
