using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Xml.Serialize;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>
	/// An XmlEditsVisitor walks over an EditLog structure and writes out
	/// an equivalent XML document that contains the EditLog's components.
	/// </summary>
	public class XmlEditsVisitor : OfflineEditsVisitor
	{
		private readonly OutputStream @out;

		private ContentHandler contentHandler;

		/// <summary>
		/// Create a processor that writes to the file named and may or may not
		/// also output to the screen, as specified.
		/// </summary>
		/// <param name="filename">Name of file to write output to</param>
		/// <param name="printToScreen">Mirror output to screen?</param>
		/// <exception cref="System.IO.IOException"/>
		public XmlEditsVisitor(OutputStream @out)
		{
			this.@out = @out;
			OutputFormat outFormat = new OutputFormat("XML", "UTF-8", true);
			outFormat.SetIndenting(true);
			outFormat.SetIndent(2);
			outFormat.SetDoctype(null, null);
			XMLSerializer serializer = new XMLSerializer(@out, outFormat);
			contentHandler = serializer.AsContentHandler();
			try
			{
				contentHandler.StartDocument();
				contentHandler.StartElement(string.Empty, string.Empty, "EDITS", new AttributesImpl
					());
			}
			catch (SAXException e)
			{
				throw new IOException("SAX error: " + e.Message);
			}
		}

		/// <summary>Start visitor (initialization)</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Start(int version)
		{
			try
			{
				contentHandler.StartElement(string.Empty, string.Empty, "EDITS_VERSION", new AttributesImpl
					());
				StringBuilder bld = new StringBuilder();
				bld.Append(version);
				AddString(bld.ToString());
				contentHandler.EndElement(string.Empty, string.Empty, "EDITS_VERSION");
			}
			catch (SAXException e)
			{
				throw new IOException("SAX error: " + e.Message);
			}
		}

		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public virtual void AddString(string str)
		{
			int slen = str.Length;
			char[] arr = new char[slen];
			Sharpen.Runtime.GetCharsForString(str, 0, slen, arr, 0);
			contentHandler.Characters(arr, 0, slen);
		}

		/// <summary>Finish visitor</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close(Exception error)
		{
			try
			{
				contentHandler.EndElement(string.Empty, string.Empty, "EDITS");
				if (error != null)
				{
					string msg = error.Message;
					XMLUtils.AddSaxString(contentHandler, "ERROR", (msg == null) ? "null" : msg);
				}
				contentHandler.EndDocument();
			}
			catch (SAXException e)
			{
				throw new IOException("SAX error: " + e.Message);
			}
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void VisitOp(FSEditLogOp op)
		{
			try
			{
				op.OutputToXml(contentHandler);
			}
			catch (SAXException e)
			{
				throw new IOException("SAX error: " + e.Message);
			}
		}
	}
}
