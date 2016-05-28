using System;
using System.Collections.Generic;
using System.Text;
using Java.Math;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Implement DBSplitter over text strings.</summary>
	public class TextSplitter : BigDecimalSplitter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TextSplitter));

		/// <summary>This method needs to determine the splits between two user-provided strings.
		/// 	</summary>
		/// <remarks>
		/// This method needs to determine the splits between two user-provided strings.
		/// In the case where the user's strings are 'A' and 'Z', this is not hard; we
		/// could create two splits from ['A', 'M') and ['M', 'Z'], 26 splits for strings
		/// beginning with each letter, etc.
		/// If a user has provided us with the strings "Ham" and "Haze", however, we need
		/// to create splits that differ in the third letter.
		/// The algorithm used is as follows:
		/// Since there are 2**16 unicode characters, we interpret characters as digits in
		/// base 65536. Given a string 's' containing characters s_0, s_1 .. s_n, we interpret
		/// the string as the number: 0.s_0 s_1 s_2.. s_n in base 65536. Having mapped the
		/// low and high strings into floating-point values, we then use the BigDecimalSplitter
		/// to establish the even split points, then map the resulting floating point values
		/// back into strings.
		/// </remarks>
		/// <exception cref="Java.Sql.SQLException"/>
		public override IList<InputSplit> Split(Configuration conf, ResultSet results, string
			 colName)
		{
			Log.Warn("Generating splits for a textual index column.");
			Log.Warn("If your database sorts in a case-insensitive order, " + "this may result in a partial import or duplicate records."
				);
			Log.Warn("You are strongly encouraged to choose an integral split column.");
			string minString = results.GetString(1);
			string maxString = results.GetString(2);
			bool minIsNull = false;
			// If the min value is null, switch it to an empty string instead for purposes
			// of interpolation. Then add [null, null] as a special case split.
			if (null == minString)
			{
				minString = string.Empty;
				minIsNull = true;
			}
			if (null == maxString)
			{
				// If the max string is null, then the min string has to be null too.
				// Just return a special split for this case.
				IList<InputSplit> splits = new AList<InputSplit>();
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
				return splits;
			}
			// Use this as a hint. May need an extra task if the size doesn't
			// divide cleanly.
			int numSplits = conf.GetInt(MRJobConfig.NumMaps, 1);
			string lowClausePrefix = colName + " >= '";
			string highClausePrefix = colName + " < '";
			// If there is a common prefix between minString and maxString, establish it
			// and pull it out of minString and maxString.
			int maxPrefixLen = Math.Min(minString.Length, maxString.Length);
			int sharedLen;
			for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++)
			{
				char c1 = minString[sharedLen];
				char c2 = maxString[sharedLen];
				if (c1 != c2)
				{
					break;
				}
			}
			// The common prefix has length 'sharedLen'. Extract it from both.
			string commonPrefix = Sharpen.Runtime.Substring(minString, 0, sharedLen);
			minString = Sharpen.Runtime.Substring(minString, sharedLen);
			maxString = Sharpen.Runtime.Substring(maxString, sharedLen);
			IList<string> splitStrings = Split(numSplits, minString, maxString, commonPrefix);
			IList<InputSplit> splits_1 = new AList<InputSplit>();
			// Convert the list of split point strings into an actual set of InputSplits.
			string start = splitStrings[0];
			for (int i = 1; i < splitStrings.Count; i++)
			{
				string end = splitStrings[i];
				if (i == splitStrings.Count - 1)
				{
					// This is the last one; use a closed interval.
					splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
						 + start + "'", colName + " <= '" + end + "'"));
				}
				else
				{
					// Normal open-interval case.
					splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
						 + start + "'", highClausePrefix + end + "'"));
				}
			}
			if (minIsNull)
			{
				// Add the special null split at the end.
				splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
			}
			return splits_1;
		}

		/// <exception cref="Java.Sql.SQLException"/>
		internal virtual IList<string> Split(int numSplits, string minString, string maxString
			, string commonPrefix)
		{
			BigDecimal minVal = StringToBigDecimal(minString);
			BigDecimal maxVal = StringToBigDecimal(maxString);
			IList<BigDecimal> splitPoints = Split(new BigDecimal(numSplits), minVal, maxVal);
			IList<string> splitStrings = new AList<string>();
			// Convert the BigDecimal splitPoints into their string representations.
			foreach (BigDecimal bd in splitPoints)
			{
				splitStrings.AddItem(commonPrefix + BigDecimalToString(bd));
			}
			// Make sure that our user-specified boundaries are the first and last entries
			// in the array.
			if (splitStrings.Count == 0 || !splitStrings[0].Equals(commonPrefix + minString))
			{
				splitStrings.Add(0, commonPrefix + minString);
			}
			if (splitStrings.Count == 1 || !splitStrings[splitStrings.Count - 1].Equals(commonPrefix
				 + maxString))
			{
				splitStrings.AddItem(commonPrefix + maxString);
			}
			return splitStrings;
		}

		private static readonly BigDecimal OnePlace = new BigDecimal(65536);

		private const int MaxChars = 8;

		// Maximum number of characters to convert. This is to prevent rounding errors
		// or repeating fractions near the very bottom from getting out of control. Note
		// that this still gives us a huge number of possible splits.
		/// <summary>
		/// Return a BigDecimal representation of string 'str' suitable for use
		/// in a numerically-sorting order.
		/// </summary>
		internal virtual BigDecimal StringToBigDecimal(string str)
		{
			BigDecimal result = BigDecimal.Zero;
			BigDecimal curPlace = OnePlace;
			// start with 1/65536 to compute the first digit.
			int len = System.Math.Min(str.Length, MaxChars);
			for (int i = 0; i < len; i++)
			{
				int codePoint = str.CodePointAt(i);
				result = result.Add(TryDivide(new BigDecimal(codePoint), curPlace));
				// advance to the next less significant place. e.g., 1/(65536^2) for the second char.
				curPlace = curPlace.Multiply(OnePlace);
			}
			return result;
		}

		/// <summary>Return the string encoded in a BigDecimal.</summary>
		/// <remarks>
		/// Return the string encoded in a BigDecimal.
		/// Repeatedly multiply the input value by 65536; the integer portion after such a multiplication
		/// represents a single character in base 65536. Convert that back into a char and create a
		/// string out of these until we have no data left.
		/// </remarks>
		internal virtual string BigDecimalToString(BigDecimal bd)
		{
			BigDecimal cur = bd.StripTrailingZeros();
			StringBuilder sb = new StringBuilder();
			for (int numConverted = 0; numConverted < MaxChars; numConverted++)
			{
				cur = cur.Multiply(OnePlace);
				int curCodePoint = cur;
				if (0 == curCodePoint)
				{
					break;
				}
				cur = cur.Subtract(new BigDecimal(curCodePoint));
				sb.Append(char.ToChars(curCodePoint));
			}
			return sb.ToString();
		}
	}
}
