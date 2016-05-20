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
using Sharpen;

namespace org.apache.hadoop.util
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
	public class ComparableVersion : java.lang.Comparable<org.apache.hadoop.util.ComparableVersion
		>
	{
		private string value;

		private string canonical;

		private org.apache.hadoop.util.ComparableVersion.ListItem items;

		private abstract class Item
		{
			public const int INTEGER_ITEM = 0;

			public const int STRING_ITEM = 1;

			public const int LIST_ITEM = 2;

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
			public abstract int compareTo(org.apache.hadoop.util.ComparableVersion.Item item);

			public abstract int getType();

			public abstract bool isNull();
		}

		private static class ItemConstants
		{
		}

		/// <summary>Represents a numeric item in the version item list.</summary>
		private class IntegerItem : org.apache.hadoop.util.ComparableVersion.Item
		{
			private static readonly java.math.BigInteger BIG_INTEGER_ZERO = new java.math.BigInteger
				("0");

			private readonly java.math.BigInteger value;

			public static readonly org.apache.hadoop.util.ComparableVersion.IntegerItem ZERO = 
				new org.apache.hadoop.util.ComparableVersion.IntegerItem();

			private IntegerItem()
			{
				this.value = BIG_INTEGER_ZERO;
			}

			public IntegerItem(string str)
			{
				this.value = new java.math.BigInteger(str);
			}

			public override int getType()
			{
				return INTEGER_ITEM;
			}

			public override bool isNull()
			{
				return BIG_INTEGER_ZERO.Equals(value);
			}

			public override int compareTo(org.apache.hadoop.util.ComparableVersion.Item item)
			{
				if (item == null)
				{
					return BIG_INTEGER_ZERO.Equals(value) ? 0 : 1;
				}
				switch (item.getType())
				{
					case INTEGER_ITEM:
					{
						// 1.0 == 1, 1.1 > 1
						return value.compareTo(((org.apache.hadoop.util.ComparableVersion.IntegerItem)item
							).value);
					}

					case STRING_ITEM:
					{
						return 1;
					}

					case LIST_ITEM:
					{
						// 1.1 > 1-sp
						return 1;
					}

					default:
					{
						// 1.1 > 1-1
						throw new System.Exception("invalid item: " + Sharpen.Runtime.getClassForObject(item
							));
					}
				}
			}

			public override string ToString()
			{
				return value.ToString();
			}
		}

		/// <summary>Represents a string in the version item list, usually a qualifier.</summary>
		private class StringItem : org.apache.hadoop.util.ComparableVersion.Item
		{
			private static readonly string[] QUALIFIERS = new string[] { "alpha", "beta", "milestone"
				, "rc", "snapshot", string.Empty, "sp" };

			private static readonly System.Collections.Generic.IList<string> _QUALIFIERS = java.util.Arrays
				.asList(QUALIFIERS);

			private static readonly java.util.Properties ALIASES = new java.util.Properties();

			static StringItem()
			{
				ALIASES["ga"] = string.Empty;
				ALIASES["final"] = string.Empty;
				ALIASES["cr"] = "rc";
			}

			/// <summary>A comparable value for the empty-string qualifier.</summary>
			/// <remarks>
			/// A comparable value for the empty-string qualifier. This one is used to determine if a given qualifier makes
			/// the version older than one without a qualifier, or more recent.
			/// </remarks>
			private static readonly string RELEASE_VERSION_INDEX = Sharpen.Runtime.getStringValueOf
				(_QUALIFIERS.indexOf(string.Empty));

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
				this.value = ALIASES.getProperty(value, value);
			}

			public override int getType()
			{
				return STRING_ITEM;
			}

			public override bool isNull()
			{
				return (string.CompareOrdinal(comparableQualifier(value), RELEASE_VERSION_INDEX) 
					== 0);
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
			public static string comparableQualifier(string qualifier)
			{
				int i = _QUALIFIERS.indexOf(qualifier);
				return i == -1 ? (_QUALIFIERS.Count + "-" + qualifier) : Sharpen.Runtime.getStringValueOf
					(i);
			}

			public override int compareTo(org.apache.hadoop.util.ComparableVersion.Item item)
			{
				if (item == null)
				{
					// 1-rc < 1, 1-ga > 1
					return string.CompareOrdinal(comparableQualifier(value), RELEASE_VERSION_INDEX);
				}
				switch (item.getType())
				{
					case INTEGER_ITEM:
					{
						return -1;
					}

					case STRING_ITEM:
					{
						// 1.any < 1.1 ?
						return string.CompareOrdinal(comparableQualifier(value), comparableQualifier(((org.apache.hadoop.util.ComparableVersion.StringItem
							)item).value));
					}

					case LIST_ITEM:
					{
						return -1;
					}

					default:
					{
						// 1.any < 1-1
						throw new System.Exception("invalid item: " + Sharpen.Runtime.getClassForObject(item
							));
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
		private class ListItem : System.Collections.Generic.List<org.apache.hadoop.util.ComparableVersion.Item
			>, org.apache.hadoop.util.ComparableVersion.Item
		{
			public override int getType()
			{
				return LIST_ITEM;
			}

			public override bool isNull()
			{
				return (Count == 0);
			}

			internal virtual void normalize()
			{
				for (java.util.ListIterator<org.apache.hadoop.util.ComparableVersion.Item> iterator
					 = listIterator(Count); iterator.hasPrevious(); )
				{
					org.apache.hadoop.util.ComparableVersion.Item item = iterator.previous();
					if (item.isNull())
					{
						iterator.remove();
					}
					else
					{
						// remove null trailing items: 0, "", empty list
						break;
					}
				}
			}

			public override int compareTo(org.apache.hadoop.util.ComparableVersion.Item item)
			{
				if (item == null)
				{
					if (Count == 0)
					{
						return 0;
					}
					// 1-0 = 1- (normalize) = 1
					org.apache.hadoop.util.ComparableVersion.Item first = this[0];
					return first.compareTo(null);
				}
				switch (item.getType())
				{
					case INTEGER_ITEM:
					{
						return -1;
					}

					case STRING_ITEM:
					{
						// 1-1 < 1.0.x
						return 1;
					}

					case LIST_ITEM:
					{
						// 1-1 > 1-sp
						System.Collections.Generic.IEnumerator<org.apache.hadoop.util.ComparableVersion.Item
							> left = GetEnumerator();
						System.Collections.Generic.IEnumerator<org.apache.hadoop.util.ComparableVersion.Item
							> right = ((org.apache.hadoop.util.ComparableVersion.ListItem)item).GetEnumerator
							();
						while (left.MoveNext() || right.MoveNext())
						{
							org.apache.hadoop.util.ComparableVersion.Item l = left.MoveNext() ? left.Current : 
								null;
							org.apache.hadoop.util.ComparableVersion.Item r = right.MoveNext() ? right.Current
								 : null;
							// if this is shorter, then invert the compare and mul with -1
							int result = l == null ? -1 * r.compareTo(l) : l.compareTo(r);
							if (result != 0)
							{
								return result;
							}
						}
						return 0;
					}

					default:
					{
						throw new System.Exception("invalid item: " + Sharpen.Runtime.getClassForObject(item
							));
					}
				}
			}

			public override string ToString()
			{
				java.lang.StringBuilder buffer = new java.lang.StringBuilder("(");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.util.ComparableVersion.Item
					> iter = GetEnumerator(); iter.MoveNext(); )
				{
					buffer.Append(iter.Current);
					if (iter.MoveNext())
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
			parseVersion(version);
		}

		public void parseVersion(string version)
		{
			this.value = version;
			items = new org.apache.hadoop.util.ComparableVersion.ListItem();
			version = org.apache.hadoop.util.StringUtils.toLowerCase(version);
			org.apache.hadoop.util.ComparableVersion.ListItem list = items;
			java.util.Stack<org.apache.hadoop.util.ComparableVersion.Item> stack = new java.util.Stack
				<org.apache.hadoop.util.ComparableVersion.Item>();
			stack.push(list);
			bool isDigit = false;
			int startIndex = 0;
			for (int i = 0; i < version.Length; i++)
			{
				char c = version[i];
				if (c == '.')
				{
					if (i == startIndex)
					{
						list.add(org.apache.hadoop.util.ComparableVersion.IntegerItem.ZERO);
					}
					else
					{
						list.add(parseItem(isDigit, Sharpen.Runtime.substring(version, startIndex, i)));
					}
					startIndex = i + 1;
				}
				else
				{
					if (c == '-')
					{
						if (i == startIndex)
						{
							list.add(org.apache.hadoop.util.ComparableVersion.IntegerItem.ZERO);
						}
						else
						{
							list.add(parseItem(isDigit, Sharpen.Runtime.substring(version, startIndex, i)));
						}
						startIndex = i + 1;
						if (isDigit)
						{
							list.normalize();
							// 1.0-* = 1-*
							if ((i + 1 < version.Length) && char.isDigit(version[i + 1]))
							{
								// new ListItem only if previous were digits and new char is a digit,
								// ie need to differentiate only 1.1 from 1-1
								list.add(list = new org.apache.hadoop.util.ComparableVersion.ListItem());
								stack.push(list);
							}
						}
					}
					else
					{
						if (char.isDigit(c))
						{
							if (!isDigit && i > startIndex)
							{
								list.add(new org.apache.hadoop.util.ComparableVersion.StringItem(Sharpen.Runtime.substring
									(version, startIndex, i), true));
								startIndex = i;
							}
							isDigit = true;
						}
						else
						{
							if (isDigit && i > startIndex)
							{
								list.add(parseItem(true, Sharpen.Runtime.substring(version, startIndex, i)));
								startIndex = i;
							}
							isDigit = false;
						}
					}
				}
			}
			if (version.Length > startIndex)
			{
				list.add(parseItem(isDigit, Sharpen.Runtime.substring(version, startIndex)));
			}
			while (!stack.isEmpty())
			{
				list = (org.apache.hadoop.util.ComparableVersion.ListItem)stack.pop();
				list.normalize();
			}
			canonical = items.ToString();
		}

		private static org.apache.hadoop.util.ComparableVersion.Item parseItem(bool isDigit
			, string buf)
		{
			return isDigit ? new org.apache.hadoop.util.ComparableVersion.IntegerItem(buf) : 
				new org.apache.hadoop.util.ComparableVersion.StringItem(buf, false);
		}

		public virtual int compareTo(org.apache.hadoop.util.ComparableVersion o)
		{
			return items.compareTo(o.items);
		}

		public override string ToString()
		{
			return value;
		}

		public override bool Equals(object o)
		{
			return (o is org.apache.hadoop.util.ComparableVersion) && canonical.Equals(((org.apache.hadoop.util.ComparableVersion
				)o).canonical);
		}

		public override int GetHashCode()
		{
			return canonical.GetHashCode();
		}
	}
}
