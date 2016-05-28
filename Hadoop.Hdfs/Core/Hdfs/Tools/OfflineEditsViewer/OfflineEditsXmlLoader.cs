using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>OfflineEditsXmlLoader walks an EditsVisitor over an OEV XML file</summary>
	internal class OfflineEditsXmlLoader : DefaultHandler, OfflineEditsLoader
	{
		private readonly bool fixTxIds;

		private readonly OfflineEditsVisitor visitor;

		private readonly InputStreamReader fileReader;

		private OfflineEditsXmlLoader.ParseState state;

		private XMLUtils.Stanza stanza;

		private Stack<XMLUtils.Stanza> stanzaStack;

		private FSEditLogOpCodes opCode;

		private StringBuilder cbuf;

		private long nextTxId;

		private readonly FSEditLogOp.OpInstanceCache opCache = new FSEditLogOp.OpInstanceCache
			();

		internal enum ParseState
		{
			ExpectEditsTag,
			ExpectVersion,
			ExpectRecord,
			ExpectOpcode,
			ExpectData,
			HandleData,
			ExpectEnd
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		public OfflineEditsXmlLoader(OfflineEditsVisitor visitor, FilePath inputFile, OfflineEditsViewer.Flags
			 flags)
		{
			this.visitor = visitor;
			this.fileReader = new InputStreamReader(new FileInputStream(inputFile), Charsets.
				Utf8);
			this.fixTxIds = flags.GetFixTxIds();
		}

		/// <summary>Loads edits file, uses visitor to process all elements</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void LoadEdits()
		{
			try
			{
				XMLReader xr = XMLReaderFactory.CreateXMLReader();
				xr.SetContentHandler(this);
				xr.SetErrorHandler(this);
				xr.SetDTDHandler(null);
				xr.Parse(new InputSource(fileReader));
				visitor.Close(null);
			}
			catch (SAXParseException e)
			{
				System.Console.Out.WriteLine("XML parsing error: " + "\n" + "Line:    " + e.GetLineNumber
					() + "\n" + "URI:     " + e.GetSystemId() + "\n" + "Message: " + e.Message);
				visitor.Close(e);
				throw new IOException(e.ToString());
			}
			catch (SAXException e)
			{
				visitor.Close(e);
				throw new IOException(e.ToString());
			}
			catch (RuntimeException e)
			{
				visitor.Close(e);
				throw;
			}
			finally
			{
				fileReader.Close();
			}
		}

		public override void StartDocument()
		{
			state = OfflineEditsXmlLoader.ParseState.ExpectEditsTag;
			stanza = null;
			stanzaStack = new Stack<XMLUtils.Stanza>();
			opCode = null;
			cbuf = new StringBuilder();
			nextTxId = -1;
		}

		public override void EndDocument()
		{
			if (state != OfflineEditsXmlLoader.ParseState.ExpectEnd)
			{
				throw new XMLUtils.InvalidXmlException("expecting </EDITS>");
			}
		}

		public override void StartElement(string uri, string name, string qName, Attributes
			 atts)
		{
			switch (state)
			{
				case OfflineEditsXmlLoader.ParseState.ExpectEditsTag:
				{
					if (!name.Equals("EDITS"))
					{
						throw new XMLUtils.InvalidXmlException("you must put " + "<EDITS> at the top of the XML file! "
							 + "Got tag " + name + " instead");
					}
					state = OfflineEditsXmlLoader.ParseState.ExpectVersion;
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectVersion:
				{
					if (!name.Equals("EDITS_VERSION"))
					{
						throw new XMLUtils.InvalidXmlException("you must put " + "<EDITS_VERSION> at the top of the XML file! "
							 + "Got tag " + name + " instead");
					}
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectRecord:
				{
					if (!name.Equals("RECORD"))
					{
						throw new XMLUtils.InvalidXmlException("expected a <RECORD> tag");
					}
					state = OfflineEditsXmlLoader.ParseState.ExpectOpcode;
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectOpcode:
				{
					if (!name.Equals("OPCODE"))
					{
						throw new XMLUtils.InvalidXmlException("expected an <OPCODE> tag");
					}
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectData:
				{
					if (!name.Equals("DATA"))
					{
						throw new XMLUtils.InvalidXmlException("expected a <DATA> tag");
					}
					stanza = new XMLUtils.Stanza();
					state = OfflineEditsXmlLoader.ParseState.HandleData;
					break;
				}

				case OfflineEditsXmlLoader.ParseState.HandleData:
				{
					XMLUtils.Stanza parent = stanza;
					XMLUtils.Stanza child = new XMLUtils.Stanza();
					stanzaStack.Push(parent);
					stanza = child;
					parent.AddChild(name, child);
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectEnd:
				{
					throw new XMLUtils.InvalidXmlException("not expecting anything after </EDITS>");
				}
			}
		}

		public override void EndElement(string uri, string name, string qName)
		{
			string str = XMLUtils.UnmangleXmlString(cbuf.ToString(), false).Trim();
			cbuf = new StringBuilder();
			switch (state)
			{
				case OfflineEditsXmlLoader.ParseState.ExpectEditsTag:
				{
					throw new XMLUtils.InvalidXmlException("expected <EDITS/>");
				}

				case OfflineEditsXmlLoader.ParseState.ExpectVersion:
				{
					if (!name.Equals("EDITS_VERSION"))
					{
						throw new XMLUtils.InvalidXmlException("expected </EDITS_VERSION>");
					}
					try
					{
						int version = System.Convert.ToInt32(str);
						visitor.Start(version);
					}
					catch (IOException e)
					{
						// Can't throw IOException from a SAX method, sigh.
						throw new RuntimeException(e);
					}
					state = OfflineEditsXmlLoader.ParseState.ExpectRecord;
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectRecord:
				{
					if (name.Equals("EDITS"))
					{
						state = OfflineEditsXmlLoader.ParseState.ExpectEnd;
					}
					else
					{
						if (!name.Equals("RECORD"))
						{
							throw new XMLUtils.InvalidXmlException("expected </EDITS> or </RECORD>");
						}
					}
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectOpcode:
				{
					if (!name.Equals("OPCODE"))
					{
						throw new XMLUtils.InvalidXmlException("expected </OPCODE>");
					}
					opCode = FSEditLogOpCodes.ValueOf(str);
					state = OfflineEditsXmlLoader.ParseState.ExpectData;
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectData:
				{
					throw new XMLUtils.InvalidXmlException("expected <DATA/>");
				}

				case OfflineEditsXmlLoader.ParseState.HandleData:
				{
					stanza.SetValue(str);
					if (stanzaStack.Empty())
					{
						if (!name.Equals("DATA"))
						{
							throw new XMLUtils.InvalidXmlException("expected </DATA>");
						}
						state = OfflineEditsXmlLoader.ParseState.ExpectRecord;
						FSEditLogOp op = opCache.Get(opCode);
						opCode = null;
						try
						{
							op.DecodeXml(stanza);
							stanza = null;
						}
						finally
						{
							if (stanza != null)
							{
								System.Console.Error.WriteLine("fromXml error decoding opcode " + opCode + "\n" +
									 stanza.ToString());
								stanza = null;
							}
						}
						if (fixTxIds)
						{
							if (nextTxId <= 0)
							{
								nextTxId = op.GetTransactionId();
								if (nextTxId <= 0)
								{
									nextTxId = 1;
								}
							}
							op.SetTransactionId(nextTxId);
							nextTxId++;
						}
						try
						{
							visitor.VisitOp(op);
						}
						catch (IOException e)
						{
							// Can't throw IOException from a SAX method, sigh.
							throw new RuntimeException(e);
						}
						state = OfflineEditsXmlLoader.ParseState.ExpectRecord;
					}
					else
					{
						stanza = stanzaStack.Pop();
					}
					break;
				}

				case OfflineEditsXmlLoader.ParseState.ExpectEnd:
				{
					throw new XMLUtils.InvalidXmlException("not expecting anything after </EDITS>");
				}
			}
		}

		public override void Characters(char[] ch, int start, int length)
		{
			cbuf.Append(ch, start, length);
		}
	}
}
