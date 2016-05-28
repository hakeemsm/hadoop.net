/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Counters;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Util
{
	/// <summary>String conversion utilities for counters.</summary>
	/// <remarks>
	/// String conversion utilities for counters.
	/// Candidate for deprecation since we start to use JSON in 0.21+
	/// </remarks>
	public class CountersStrings
	{
		private const char GroupOpen = '{';

		private const char GroupClose = '}';

		private const char CounterOpen = '[';

		private const char CounterClose = ']';

		private const char UnitOpen = '(';

		private const char UnitClose = ')';

		private static char[] charsToEscape = new char[] { GroupOpen, GroupClose, CounterOpen
			, CounterClose, UnitOpen, UnitClose };

		/// <summary>Make the pre 0.21 counter string (for e.g.</summary>
		/// <remarks>
		/// Make the pre 0.21 counter string (for e.g. old job history files)
		/// [(actual-name)(display-name)(value)]
		/// </remarks>
		/// <param name="counter">to stringify</param>
		/// <returns>the stringified result</returns>
		public static string ToEscapedCompactString(Counter counter)
		{
			// First up, obtain the strings that need escaping. This will help us
			// determine the buffer length apriori.
			string escapedName;
			string escapedDispName;
			long currentValue;
			lock (counter)
			{
				escapedName = Escape(counter.GetName());
				escapedDispName = Escape(counter.GetDisplayName());
				currentValue = counter.GetValue();
			}
			int length = escapedName.Length + escapedDispName.Length + 4;
			length += 8;
			// For the following delimiting characters
			StringBuilder builder = new StringBuilder(length);
			builder.Append(CounterOpen);
			// Add the counter name
			builder.Append(UnitOpen);
			builder.Append(escapedName);
			builder.Append(UnitClose);
			// Add the display name
			builder.Append(UnitOpen);
			builder.Append(escapedDispName);
			builder.Append(UnitClose);
			// Add the value
			builder.Append(UnitOpen);
			builder.Append(currentValue);
			builder.Append(UnitClose);
			builder.Append(CounterClose);
			return builder.ToString();
		}

		/// <summary>Make the 0.21 counter group string.</summary>
		/// <remarks>
		/// Make the 0.21 counter group string.
		/// format: {(actual-name)(display-name)(value)[][][]}
		/// where [] are compact strings for the counters within.
		/// </remarks>
		/// <?/>
		/// <param name="group">to stringify</param>
		/// <returns>the stringified result</returns>
		public static string ToEscapedCompactString<G>(G group)
			where G : CounterGroupBase<object>
		{
			IList<string> escapedStrs = Lists.NewArrayList();
			int length;
			string escapedName;
			string escapedDispName;
			lock (group)
			{
				// First up, obtain the strings that need escaping. This will help us
				// determine the buffer length apriori.
				escapedName = Escape(group.GetName());
				escapedDispName = Escape(group.GetDisplayName());
				int i = 0;
				length = escapedName.Length + escapedDispName.Length;
				foreach (Counter counter in group)
				{
					string escapedStr = ToEscapedCompactString(counter);
					escapedStrs.AddItem(escapedStr);
					length += escapedStr.Length;
				}
			}
			length += 6;
			// for all the delimiting characters below
			StringBuilder builder = new StringBuilder(length);
			builder.Append(GroupOpen);
			// group start
			// Add the group name
			builder.Append(UnitOpen);
			builder.Append(escapedName);
			builder.Append(UnitClose);
			// Add the display name
			builder.Append(UnitOpen);
			builder.Append(escapedDispName);
			builder.Append(UnitClose);
			// write the value
			foreach (string escaped in escapedStrs)
			{
				builder.Append(escaped);
			}
			builder.Append(GroupClose);
			// group end
			return builder.ToString();
		}

		/// <summary>Make the pre 0.21 counters string</summary>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <param name="counters">the object to stringify</param>
		/// <returns>
		/// the string in the following format
		/// {(groupName)(group-displayName)[(counterName)(displayName)(value)]*}
		/// </returns>
		public static string ToEscapedCompactString<C, G, T>(T counters)
			where C : Counter
			where G : CounterGroupBase<C>
			where T : AbstractCounters<C, G>
		{
			string[] groupsArray;
			int length = 0;
			lock (counters)
			{
				groupsArray = new string[counters.CountCounters()];
				int i = 0;
				// First up, obtain the escaped string for each group so that we can
				// determine the buffer length apriori.
				foreach (G group in counters)
				{
					string escapedString = ToEscapedCompactString(group);
					groupsArray[i++] = escapedString;
					length += escapedString.Length;
				}
			}
			// Now construct the buffer
			StringBuilder builder = new StringBuilder(length);
			foreach (string group_1 in groupsArray)
			{
				builder.Append(group_1);
			}
			return builder.ToString();
		}

		// Escapes all the delimiters for counters i.e {,[,(,),],}
		private static string Escape(string @string)
		{
			return StringUtils.EscapeString(@string, StringUtils.EscapeChar, charsToEscape);
		}

		// Unescapes all the delimiters for counters i.e {,[,(,),],}
		private static string Unescape(string @string)
		{
			return StringUtils.UnEscapeString(@string, StringUtils.EscapeChar, charsToEscape);
		}

		// Extracts a block (data enclosed within delimeters) ignoring escape
		// sequences. Throws ParseException if an incomplete block is found else
		// returns null.
		/// <exception cref="Sharpen.ParseException"/>
		private static string GetBlock(string str, char open, char close, IntWritable index
			)
		{
			StringBuilder split = new StringBuilder();
			int next = StringUtils.FindNext(str, open, StringUtils.EscapeChar, index.Get(), split
				);
			split.Length = 0;
			// clear the buffer
			if (next >= 0)
			{
				++next;
				// move over '('
				next = StringUtils.FindNext(str, close, StringUtils.EscapeChar, next, split);
				if (next >= 0)
				{
					++next;
					// move over ')'
					index.Set(next);
					return split.ToString();
				}
				else
				{
					// found a block
					throw new ParseException("Unexpected end of block", next);
				}
			}
			return null;
		}

		// found nothing
		/// <summary>Parse a pre 0.21 counters string into a counter object.</summary>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <param name="compactString">to parse</param>
		/// <param name="counters">an empty counters object to hold the result</param>
		/// <returns>the counters object holding the result</returns>
		/// <exception cref="Sharpen.ParseException"/>
		public static T ParseEscapedCompactString<C, G, T>(string compactString, T counters
			)
			where C : Counter
			where G : CounterGroupBase<C>
			where T : AbstractCounters<C, G>
		{
			IntWritable index = new IntWritable(0);
			// Get the group to work on
			string groupString = GetBlock(compactString, GroupOpen, GroupClose, index);
			while (groupString != null)
			{
				IntWritable groupIndex = new IntWritable(0);
				// Get the actual name
				string groupName = StringInterner.WeakIntern(GetBlock(groupString, UnitOpen, UnitClose
					, groupIndex));
				groupName = StringInterner.WeakIntern(Unescape(groupName));
				// Get the display name
				string groupDisplayName = StringInterner.WeakIntern(GetBlock(groupString, UnitOpen
					, UnitClose, groupIndex));
				groupDisplayName = StringInterner.WeakIntern(Unescape(groupDisplayName));
				// Get the counters
				G group = counters.GetGroup(groupName);
				group.SetDisplayName(groupDisplayName);
				string counterString = GetBlock(groupString, CounterOpen, CounterClose, groupIndex
					);
				while (counterString != null)
				{
					IntWritable counterIndex = new IntWritable(0);
					// Get the actual name
					string counterName = StringInterner.WeakIntern(GetBlock(counterString, UnitOpen, 
						UnitClose, counterIndex));
					counterName = StringInterner.WeakIntern(Unescape(counterName));
					// Get the display name
					string counterDisplayName = StringInterner.WeakIntern(GetBlock(counterString, UnitOpen
						, UnitClose, counterIndex));
					counterDisplayName = StringInterner.WeakIntern(Unescape(counterDisplayName));
					// Get the value
					long value = long.Parse(GetBlock(counterString, UnitOpen, UnitClose, counterIndex
						));
					// Add the counter
					Counter counter = group.FindCounter(counterName);
					counter.SetDisplayName(counterDisplayName);
					counter.Increment(value);
					// Get the next counter
					counterString = GetBlock(groupString, CounterOpen, CounterClose, groupIndex);
				}
				groupString = GetBlock(compactString, GroupOpen, GroupClose, index);
			}
			return counters;
		}
	}
}
