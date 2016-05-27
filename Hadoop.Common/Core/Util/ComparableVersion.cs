// Code source of this file: 
//   http://grepcode.com/file/repo1.maven.org/maven2/
//     org.apache.maven/maven-artifact/3.1.1/
//       org/apache/maven/artifact/versioning/ComparableVersion.java/
//
// Modifications made on top of the source:
//   1. Changed
//        package org.apache.maven.artifact.versioning;
//      to
//        package org.apache.hadoop.util;
//   2. Removed author tags to clear hadoop author tag warning
//
using System.Collections.Generic;
using System.Text;
using Mono.Math;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>Generic implementation of version comparison.</summary>
	/// <remarks>
	/// Generic implementation of version comparison.
	/// <p>Features:
	/// <ul>
	/// <li>mixing of '<code>-</code>' (dash) and '<code>.</code>' (dot) separators,</li>
	/// <li>transition between characters and digits also constitutes a separator:
	/// <code>1.0alpha1 =&gt; [1, 0, alpha, 1]</code></li>
	/// <li>unlimited number of version components,</li>
	/// <li>version components in the text can be digits or strings,</li>
	/// <li>strings are checked for well-known qualifiers and the qualifier ordering is used for version ordering.
	/// Well-known qualifiers (case insensitive) are:<ul>
	/// <li><code>alpha</code> or <code>a</code></li>
	/// <li><code>beta</code> or <code>b</code></li>
	/// <li><code>milestone</code> or <code>m</code></li>
	/// <li><code>rc</code> or <code>cr</code></li>
	/// <li><code>snapshot</code></li>
	/// <li><code>(the empty string)</code> or <code>ga</code> or <code>final</code></li>
	/// <li><code>sp</code></li>
	/// </ul>
	/// Unknown qualifiers are considered after known qualifiers, with lexical order (always case insensitive),
	/// </li>
	/// <li>a dash usually precedes a qualifier, and is always less important than something preceded with a dot.</li>
	/// </ul></p>
	/// </remarks>
	/// <seealso><a href="https://cwiki.apache.org/confluence/display/MAVENOLD/Versioning">"Versioning" on Maven Wiki</a>
	/// 	</seealso>
	public class ComparableVersion : Comparable<Org.Apache.Hadoop.Util.ComparableVersion
		>
	{
		private string value;

		private string canonical;

		private ComparableVersion.ListItem items;

		private abstract class Item
		{
			public const int IntegerItem = 0;

			public const int StringItem = 1;

			public const int ListItem = 2;

			/*
			* Licensed to the Apache Software Foundation (ASF) under one
			* or more contributor license agreements.  See the NOTICE file
			* distributed with this work for additional information
			* regarding copyright ownership.  The ASF licenses this file
			* to you under the Apache License, Version 2.0 (the
			* "License"); you may not use this file except in compliance
			* with the License.  You may obtain a copy of the License at
			*
			*  http://www.apache.org/licenses/LICENSE-2.0
			*
			* Unless required by applicable law or agreed to in writing,
			* software distributed under the License is distributed on an
			* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
			* KIND, either express or implied.  See the License for the
			* specific language governing permissions and limitations
			* under the License.
			*/
			public abstract int CompareTo(ComparableVersion.Item item);

			public abstract int GetType();

			public abstract bool IsNull();
		}

		private static class ItemConstants
		{
		}

		/// <summary>Represents a numeric item in the version item list.</summary>
		private class IntegerItem : ComparableVersion.Item
		{
			private static readonly BigInteger BigIntegerZero = new BigInteger("0");

			private readonly BigInteger value;

			public static readonly ComparableVersion.IntegerItem Zero = new ComparableVersion.IntegerItem
				();

			private IntegerItem()
			{
				this.value = BigIntegerZero;
			}

			public IntegerItem(string str)
			{
				this.value = new BigInteger(str);
			}

			public override int GetType()
			{
				return IntegerItem;
			}

			public override bool IsNull()
			{
				return BigIntegerZero.Equals(value);
			}

			public override int CompareTo(ComparableVersion.Item item)
			{
				if (item == null)
				{
					return BigIntegerZero.Equals(value) ? 0 : 1;
				}
				switch (item.GetType())
				{
					case IntegerItem:
					{
						// 1.0 == 1, 1.1 > 1
						return value.CompareTo(((ComparableVersion.IntegerItem)item).value);
					}

					case StringItem:
					{
						return 1;
					}

					case ListItem:
					{
						// 1.1 > 1-sp
						return 1;
					}

					default:
					{
						// 1.1 > 1-1
						throw new RuntimeException("invalid item: " + item.GetType());
					}
				}
			}

			public override string ToString()
			{
				return value.ToString();
			}
		}

		/// <summary>Represents a string in the version item list, usually a qualifier.</summary>
		private class StringItem : ComparableVersion.Item
		{
			private static readonly string[] Qualifiers = new string[] { "alpha", "beta", "milestone"
				, "rc", "snapshot", string.Empty, "sp" };

			private static readonly IList<string> Qualifiers = Arrays.AsList(Qualifiers);

			private static readonly Properties Aliases = new Properties();

			static StringItem()
			{
				Aliases["ga"] = string.Empty;
				Aliases["final"] = string.Empty;
				Aliases["cr"] = "rc";
			}

			/// <summary>A comparable value for the empty-string qualifier.</summary>
			/// <remarks>
			/// A comparable value for the empty-string qualifier. This one is used to determine if a given qualifier makes
			/// the version older than one without a qualifier, or more recent.
			/// </remarks>
			private static readonly string ReleaseVersionIndex = Qualifiers.IndexOf(string.Empty
				).ToString();

			private string value;

			public StringItem(string value, bool followedByDigit)
			{
				if (followedByDigit && value.Length == 1)
				{
					switch (value[0])
					{
						case 'a':
						{
							// a1 = alpha-1, b1 = beta-1, m1 = milestone-1
							value = "alpha";
							break;
						}

						case 'b':
						{
							value = "beta";
							break;
						}

						case 'm':
						{
							value = "milestone";
							break;
						}

						default:
						{
							break;
						}
					}
				}
				this.value = Aliases.GetProperty(value, value);
			}

			public override int GetType()
			{
				return StringItem;
			}

			public override bool IsNull()
			{
				return (string.CompareOrdinal(ComparableQualifier(value), ReleaseVersionIndex) ==
					 0);
			}

			/// <summary>Returns a comparable value for a qualifier.</summary>
			/// <remarks>
			/// Returns a comparable value for a qualifier.
			/// This method takes into account the ordering of known qualifiers then unknown qualifiers with lexical ordering.
			/// just returning an Integer with the index here is faster, but requires a lot of if/then/else to check for -1
			/// or QUALIFIERS.size and then resort to lexical ordering. Most comparisons are decided by the first character,
			/// so this is still fast. If more characters are needed then it requires a lexical sort anyway.
			/// </remarks>
			/// <param name="qualifier"/>
			/// <returns>an equivalent value that can be used with lexical comparison</returns>
			public static string ComparableQualifier(string qualifier)
			{
				int i = Qualifiers.IndexOf(qualifier);
				return i == -1 ? (Qualifiers.Count + "-" + qualifier) : i.ToString();
			}

			public override int CompareTo(ComparableVersion.Item item)
			{
				if (item == null)
				{
					// 1-rc < 1, 1-ga > 1
					return string.CompareOrdinal(ComparableQualifier(value), ReleaseVersionIndex);
				}
				switch (item.GetType())
				{
					case IntegerItem:
					{
						return -1;
					}

					case StringItem:
					{
						// 1.any < 1.1 ?
						return string.CompareOrdinal(ComparableQualifier(value), ComparableQualifier(((ComparableVersion.StringItem
							)item).value));
					}

					case ListItem:
					{
						return -1;
					}

					default:
					{
						// 1.any < 1-1
						throw new RuntimeException("invalid item: " + item.GetType());
					}
				}
			}

			public override string ToString()
			{
				return value;
			}
		}

		/// <summary>Represents a version list item.</summary>
		/// <remarks>
		/// Represents a version list item. This class is used both for the global item list and for sub-lists (which start
		/// with '-(number)' in the version specification).
		/// </remarks>
		[System.Serializable]
		private class ListItem : AList<ComparableVersion.Item>, ComparableVersion.Item
		{
			public override int GetType()
			{
				return ListItem;
			}

			public override bool IsNull()
			{
				return (Count == 0);
			}

			internal virtual void Normalize()
			{
				for (ListIterator<ComparableVersion.Item> iterator = ListIterator(Count); iterator
					.HasPrevious(); )
				{
					ComparableVersion.Item item = iterator.Previous();
					if (item.IsNull())
					{
						iterator.Remove();
					}
					else
					{
						// remove null trailing items: 0, "", empty list
						break;
					}
				}
			}

			public override int CompareTo(ComparableVersion.Item item)
			{
				if (item == null)
				{
					if (Count == 0)
					{
						return 0;
					}
					// 1-0 = 1- (normalize) = 1
					ComparableVersion.Item first = this[0];
					return first.CompareTo(null);
				}
				switch (item.GetType())
				{
					case IntegerItem:
					{
						return -1;
					}

					case StringItem:
					{
						// 1-1 < 1.0.x
						return 1;
					}

					case ListItem:
					{
						// 1-1 > 1-sp
						IEnumerator<ComparableVersion.Item> left = GetEnumerator();
						IEnumerator<ComparableVersion.Item> right = ((ComparableVersion.ListItem)item).GetEnumerator
							();
						while (left.HasNext() || right.HasNext())
						{
							ComparableVersion.Item l = left.HasNext() ? left.Next() : null;
							ComparableVersion.Item r = right.HasNext() ? right.Next() : null;
							// if this is shorter, then invert the compare and mul with -1
							int result = l == null ? -1 * r.CompareTo(l) : l.CompareTo(r);
							if (result != 0)
							{
								return result;
							}
						}
						return 0;
					}

					default:
					{
						throw new RuntimeException("invalid item: " + item.GetType());
					}
				}
			}

			public override string ToString()
			{
				StringBuilder buffer = new StringBuilder("(");
				for (IEnumerator<ComparableVersion.Item> iter = GetEnumerator(); iter.HasNext(); )
				{
					buffer.Append(iter.Next());
					if (iter.HasNext())
					{
						buffer.Append(',');
					}
				}
				buffer.Append(')');
				return buffer.ToString();
			}
		}

		public ComparableVersion(string version)
		{
			ParseVersion(version);
		}

		public void ParseVersion(string version)
		{
			this.value = version;
			items = new ComparableVersion.ListItem();
			version = StringUtils.ToLowerCase(version);
			ComparableVersion.ListItem list = items;
			Stack<ComparableVersion.Item> stack = new Stack<ComparableVersion.Item>();
			stack.Push(list);
			bool isDigit = false;
			int startIndex = 0;
			for (int i = 0; i < version.Length; i++)
			{
				char c = version[i];
				if (c == '.')
				{
					if (i == startIndex)
					{
						list.AddItem(ComparableVersion.IntegerItem.Zero);
					}
					else
					{
						list.AddItem(ParseItem(isDigit, Sharpen.Runtime.Substring(version, startIndex, i)
							));
					}
					startIndex = i + 1;
				}
				else
				{
					if (c == '-')
					{
						if (i == startIndex)
						{
							list.AddItem(ComparableVersion.IntegerItem.Zero);
						}
						else
						{
							list.AddItem(ParseItem(isDigit, Sharpen.Runtime.Substring(version, startIndex, i)
								));
						}
						startIndex = i + 1;
						if (isDigit)
						{
							list.Normalize();
							// 1.0-* = 1-*
							if ((i + 1 < version.Length) && char.IsDigit(version[i + 1]))
							{
								// new ListItem only if previous were digits and new char is a digit,
								// ie need to differentiate only 1.1 from 1-1
								list.AddItem(list = new ComparableVersion.ListItem());
								stack.Push(list);
							}
						}
					}
					else
					{
						if (char.IsDigit(c))
						{
							if (!isDigit && i > startIndex)
							{
								list.AddItem(new ComparableVersion.StringItem(Sharpen.Runtime.Substring(version, 
									startIndex, i), true));
								startIndex = i;
							}
							isDigit = true;
						}
						else
						{
							if (isDigit && i > startIndex)
							{
								list.AddItem(ParseItem(true, Sharpen.Runtime.Substring(version, startIndex, i)));
								startIndex = i;
							}
							isDigit = false;
						}
					}
				}
			}
			if (version.Length > startIndex)
			{
				list.AddItem(ParseItem(isDigit, Sharpen.Runtime.Substring(version, startIndex)));
			}
			while (!stack.IsEmpty())
			{
				list = (ComparableVersion.ListItem)stack.Pop();
				list.Normalize();
			}
			canonical = items.ToString();
		}

		private static ComparableVersion.Item ParseItem(bool isDigit, string buf)
		{
			return isDigit ? new ComparableVersion.IntegerItem(buf) : new ComparableVersion.StringItem
				(buf, false);
		}

		public virtual int CompareTo(ComparableVersion o)
		{
			return items.CompareTo(o.items);
		}

		public override string ToString()
		{
			return value;
		}

		public override bool Equals(object o)
		{
			return (o is ComparableVersion) && canonical.Equals(((ComparableVersion)o).canonical
				);
		}

		public override int GetHashCode()
		{
			return canonical.GetHashCode();
		}
	}
}
