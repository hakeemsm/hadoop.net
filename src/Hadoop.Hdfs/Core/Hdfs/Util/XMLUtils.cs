using System;
using System.Collections.Generic;
using System.Text;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>General xml utilities.</summary>
	public class XMLUtils
	{
		/// <summary>Exception that reflects an invalid XML document.</summary>
		[System.Serializable]
		public class InvalidXmlException : RuntimeException
		{
			private const long serialVersionUID = 1L;

			public InvalidXmlException(string s)
				: base(s)
			{
			}
		}

		/// <summary>Exception that reflects a string that cannot be unmangled.</summary>
		[System.Serializable]
		public class UnmanglingError : RuntimeException
		{
			private const long serialVersionUID = 1L;

			public UnmanglingError(string str, Exception e)
				: base(str, e)
			{
			}

			public UnmanglingError(string str)
				: base(str)
			{
			}
		}

		/// <summary>
		/// Given a code point, determine if it should be mangled before being
		/// represented in an XML document.
		/// </summary>
		/// <remarks>
		/// Given a code point, determine if it should be mangled before being
		/// represented in an XML document.
		/// Any code point that isn't valid in XML must be mangled.
		/// See http://en.wikipedia.org/wiki/Valid_characters_in_XML for a
		/// quick reference, or the w3 standard for the authoritative reference.
		/// </remarks>
		/// <param name="cp">The code point</param>
		/// <returns>True if the code point should be mangled</returns>
		private static bool CodePointMustBeMangled(int cp)
		{
			if (cp < unchecked((int)(0x20)))
			{
				return ((cp != unchecked((int)(0x9))) && (cp != unchecked((int)(0xa))) && (cp != 
					unchecked((int)(0xd))));
			}
			else
			{
				if ((unchecked((int)(0xd7ff)) < cp) && (cp < unchecked((int)(0xe000))))
				{
					return true;
				}
				else
				{
					if ((cp == unchecked((int)(0xfffe))) || (cp == unchecked((int)(0xffff))))
					{
						return true;
					}
					else
					{
						if (cp == unchecked((int)(0x5c)))
						{
							// we mangle backslash to simplify decoding... it's
							// easier if backslashes always begin mangled sequences. 
							return true;
						}
					}
				}
			}
			return false;
		}

		private const int NumSlashPositions = 4;

		private static string MangleCodePoint(int cp)
		{
			return string.Format("\\%0" + NumSlashPositions + "x;", cp);
		}

		private static string CodePointToEntityRef(int cp)
		{
			switch (cp)
			{
				case '&':
				{
					return "&amp;";
				}

				case '\"':
				{
					return "&quot;";
				}

				case '\'':
				{
					return "&apos;";
				}

				case '<':
				{
					return "&lt;";
				}

				case '>':
				{
					return "&gt;";
				}

				default:
				{
					return null;
				}
			}
		}

		/// <summary>Mangle a string so that it can be represented in an XML document.</summary>
		/// <remarks>
		/// Mangle a string so that it can be represented in an XML document.
		/// There are three kinds of code points in XML:
		/// - Those that can be represented normally,
		/// - Those that have to be escaped (for example, & must be represented
		/// as &amp;)
		/// - Those that cannot be represented at all in XML.
		/// The built-in SAX functions will handle the first two types for us just
		/// fine.  However, sometimes we come across a code point of the third type.
		/// In this case, we have to mangle the string in order to represent it at
		/// all.  We also mangle backslash to avoid confusing a backslash in the
		/// string with part our escape sequence.
		/// The encoding used here is as follows: an illegal code point is
		/// represented as '\ABCD;', where ABCD is the hexadecimal value of
		/// the code point.
		/// </remarks>
		/// <param name="str">The input string.</param>
		/// <returns>The mangled string.</returns>
		public static string MangleXmlString(string str, bool createEntityRefs)
		{
			StringBuilder bld = new StringBuilder();
			int length = str.Length;
			for (int offset = 0; offset < length; )
			{
				int cp = str.CodePointAt(offset);
				int len = char.CharCount(cp);
				if (CodePointMustBeMangled(cp))
				{
					bld.Append(MangleCodePoint(cp));
				}
				else
				{
					string entityRef = null;
					if (createEntityRefs)
					{
						entityRef = CodePointToEntityRef(cp);
					}
					if (entityRef != null)
					{
						bld.Append(entityRef);
					}
					else
					{
						for (int i = 0; i < len; i++)
						{
							bld.Append(str[offset + i]);
						}
					}
				}
				offset += len;
			}
			return bld.ToString();
		}

		/// <summary>Demangle a string from an XML document.</summary>
		/// <remarks>
		/// Demangle a string from an XML document.
		/// See
		/// <see cref="MangleXmlString(string, bool)"/>
		/// for a description of the
		/// mangling format.
		/// </remarks>
		/// <param name="str">The string to be demangled.</param>
		/// <returns>The unmangled string</returns>
		/// <exception cref="UnmanglingError">if the input is malformed.</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.UnmanglingError"/>
		public static string UnmangleXmlString(string str, bool decodeEntityRefs)
		{
			int slashPosition = -1;
			string escapedCp = string.Empty;
			StringBuilder bld = new StringBuilder();
			StringBuilder entityRef = null;
			for (int i = 0; i < str.Length; i++)
			{
				char ch = str[i];
				if (entityRef != null)
				{
					entityRef.Append(ch);
					if (ch == ';')
					{
						string e = entityRef.ToString();
						if (e.Equals("&quot;"))
						{
							bld.Append("\"");
						}
						else
						{
							if (e.Equals("&apos;"))
							{
								bld.Append("\'");
							}
							else
							{
								if (e.Equals("&amp;"))
								{
									bld.Append("&");
								}
								else
								{
									if (e.Equals("&lt;"))
									{
										bld.Append("<");
									}
									else
									{
										if (e.Equals("&gt;"))
										{
											bld.Append(">");
										}
										else
										{
											throw new XMLUtils.UnmanglingError("Unknown entity ref " + e);
										}
									}
								}
							}
						}
						entityRef = null;
					}
				}
				else
				{
					if ((slashPosition >= 0) && (slashPosition < NumSlashPositions))
					{
						escapedCp += ch;
						++slashPosition;
					}
					else
					{
						if (slashPosition == NumSlashPositions)
						{
							if (ch != ';')
							{
								throw new XMLUtils.UnmanglingError("unterminated code point escape: " + "expected semicolon at end."
									);
							}
							try
							{
								bld.AppendCodePoint(System.Convert.ToInt32(escapedCp, 16));
							}
							catch (FormatException e)
							{
								throw new XMLUtils.UnmanglingError("error parsing unmangling escape code", e);
							}
							escapedCp = string.Empty;
							slashPosition = -1;
						}
						else
						{
							if (ch == '\\')
							{
								slashPosition = 0;
							}
							else
							{
								bool startingEntityRef = false;
								if (decodeEntityRefs)
								{
									startingEntityRef = (ch == '&');
								}
								if (startingEntityRef)
								{
									entityRef = new StringBuilder();
									entityRef.Append("&");
								}
								else
								{
									bld.Append(ch);
								}
							}
						}
					}
				}
			}
			if (entityRef != null)
			{
				throw new XMLUtils.UnmanglingError("unterminated entity ref starting with " + entityRef
					.ToString());
			}
			else
			{
				if (slashPosition != -1)
				{
					throw new XMLUtils.UnmanglingError("unterminated code point escape: string " + "broke off in the middle"
						);
				}
			}
			return bld.ToString();
		}

		/// <summary>Add a SAX tag with a string inside.</summary>
		/// <param name="contentHandler">the SAX content handler</param>
		/// <param name="tag">the element tag to use</param>
		/// <param name="val">the string to put inside the tag</param>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		public static void AddSaxString(ContentHandler contentHandler, string tag, string
			 val)
		{
			contentHandler.StartElement(string.Empty, string.Empty, tag, new AttributesImpl()
				);
			char[] c = MangleXmlString(val, false).ToCharArray();
			contentHandler.Characters(c, 0, c.Length);
			contentHandler.EndElement(string.Empty, string.Empty, tag);
		}

		/// <summary>
		/// Represents a bag of key-value pairs encountered during parsing an XML
		/// file.
		/// </summary>
		public class Stanza
		{
			private readonly SortedDictionary<string, List<XMLUtils.Stanza>> subtrees;

			/// <summary>The unmangled value of this stanza.</summary>
			private string value;

			public Stanza()
			{
				subtrees = new SortedDictionary<string, List<XMLUtils.Stanza>>();
				value = string.Empty;
			}

			public virtual void SetValue(string value)
			{
				this.value = value;
			}

			public virtual string GetValue()
			{
				return this.value;
			}

			/// <summary>Discover if a stanza has a given entry.</summary>
			/// <param name="name">entry to look for</param>
			/// <returns>true if the entry was found</returns>
			public virtual bool HasChildren(string name)
			{
				return subtrees.Contains(name);
			}

			/// <summary>Pull an entry from a stanza.</summary>
			/// <param name="name">entry to look for</param>
			/// <returns>the entry</returns>
			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			public virtual IList<XMLUtils.Stanza> GetChildren(string name)
			{
				List<XMLUtils.Stanza> children = subtrees[name];
				if (children == null)
				{
					throw new XMLUtils.InvalidXmlException("no entry found for " + name);
				}
				return children;
			}

			/// <summary>Pull a string entry from a stanza.</summary>
			/// <param name="name">entry to look for</param>
			/// <returns>the entry</returns>
			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			public virtual string GetValue(string name)
			{
				string ret = GetValueOrNull(name);
				if (ret == null)
				{
					throw new XMLUtils.InvalidXmlException("no entry found for " + name);
				}
				return ret;
			}

			/// <summary>Pull a string entry from a stanza, or null.</summary>
			/// <param name="name">entry to look for</param>
			/// <returns>the entry, or null if it was not found.</returns>
			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			public virtual string GetValueOrNull(string name)
			{
				if (!subtrees.Contains(name))
				{
					return null;
				}
				List<XMLUtils.Stanza> l = subtrees[name];
				if (l.Count != 1)
				{
					throw new XMLUtils.InvalidXmlException("More than one value found for " + name);
				}
				return l[0].GetValue();
			}

			/// <summary>Add an entry to a stanza.</summary>
			/// <param name="name">name of the entry to add</param>
			/// <param name="child">the entry to add</param>
			public virtual void AddChild(string name, XMLUtils.Stanza child)
			{
				List<XMLUtils.Stanza> l;
				if (subtrees.Contains(name))
				{
					l = subtrees[name];
				}
				else
				{
					l = new List<XMLUtils.Stanza>();
					subtrees[name] = l;
				}
				l.AddItem(child);
			}

			/// <summary>Convert a stanza to a human-readable string.</summary>
			public override string ToString()
			{
				StringBuilder bld = new StringBuilder();
				bld.Append("{");
				if (!value.Equals(string.Empty))
				{
					bld.Append("\"").Append(value).Append("\"");
				}
				string prefix = string.Empty;
				foreach (KeyValuePair<string, List<XMLUtils.Stanza>> entry in subtrees)
				{
					string key = entry.Key;
					List<XMLUtils.Stanza> ll = entry.Value;
					foreach (XMLUtils.Stanza child in ll)
					{
						bld.Append(prefix);
						bld.Append("<").Append(key).Append(">");
						bld.Append(child.ToString());
						prefix = ", ";
					}
				}
				bld.Append("}");
				return bld.ToString();
			}
		}
	}
}
