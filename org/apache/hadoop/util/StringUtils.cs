using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>General string utils</summary>
	public class StringUtils
	{
		/// <summary>Priority of the StringUtils shutdown hook.</summary>
		public const int SHUTDOWN_HOOK_PRIORITY = 0;

		/// <summary>
		/// Shell environment variables: $ followed by one letter or _ followed by
		/// multiple letters, numbers, or underscores.
		/// </summary>
		/// <remarks>
		/// Shell environment variables: $ followed by one letter or _ followed by
		/// multiple letters, numbers, or underscores.  The group captures the
		/// environment variable name without the leading $.
		/// </remarks>
		public static readonly java.util.regex.Pattern SHELL_ENV_VAR_PATTERN = java.util.regex.Pattern
			.compile("\\$([A-Za-z_]{1}[A-Za-z0-9_]*)");

		/// <summary>Windows environment variables: surrounded by %.</summary>
		/// <remarks>
		/// Windows environment variables: surrounded by %.  The group captures the
		/// environment variable name without the leading and trailing %.
		/// </remarks>
		public static readonly java.util.regex.Pattern WIN_ENV_VAR_PATTERN = java.util.regex.Pattern
			.compile("%(.*?)%");

		/// <summary>
		/// Regular expression that matches and captures environment variable names
		/// according to platform-specific rules.
		/// </summary>
		public static readonly java.util.regex.Pattern ENV_VAR_PATTERN = org.apache.hadoop.util.Shell
			.WINDOWS ? WIN_ENV_VAR_PATTERN : SHELL_ENV_VAR_PATTERN;

		/// <summary>Make a string representation of the exception.</summary>
		/// <param name="e">The exception to stringify</param>
		/// <returns>A string with exception name and call stack.</returns>
		public static string stringifyException(System.Exception e)
		{
			System.IO.StringWriter stm = new System.IO.StringWriter();
			java.io.PrintWriter wrt = new java.io.PrintWriter(stm);
			Sharpen.Runtime.printStackTrace(e, wrt);
			wrt.close();
			return stm.ToString();
		}

		/// <summary>Given a full hostname, return the word upto the first dot.</summary>
		/// <param name="fullHostname">the full hostname</param>
		/// <returns>the hostname to the first dot</returns>
		public static string simpleHostname(string fullHostname)
		{
			if (com.google.common.net.InetAddresses.isInetAddress(fullHostname))
			{
				return fullHostname;
			}
			int offset = fullHostname.IndexOf('.');
			if (offset != -1)
			{
				return Sharpen.Runtime.substring(fullHostname, 0, offset);
			}
			return fullHostname;
		}

		/// <summary>
		/// Given an integer, return a string that is in an approximate, but human
		/// readable format.
		/// </summary>
		/// <param name="number">the number to format</param>
		/// <returns>a human readable form of the integer</returns>
		[System.ObsoleteAttribute(@"use TraditionalBinaryPrefix.long2String(long, string, int) ."
			)]
		public static string humanReadableInt(long number)
		{
			return org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.long2String(number
				, string.Empty, 1);
		}

		/// <summary>The same as String.format(Locale.ENGLISH, format, objects).</summary>
		public static string format(string format, params object[] objects)
		{
			return string.format(java.util.Locale.ENGLISH, format, objects);
		}

		/// <summary>Format a percentage for presentation to the user.</summary>
		/// <param name="fraction">the percentage as a fraction, e.g. 0.1 = 10%</param>
		/// <param name="decimalPlaces">the number of decimal places</param>
		/// <returns>a string representation of the percentage</returns>
		public static string formatPercent(double fraction, int decimalPlaces)
		{
			return format("%." + decimalPlaces + "f%%", fraction * 100);
		}

		/// <summary>Given an array of strings, return a comma-separated list of its elements.
		/// 	</summary>
		/// <param name="strs">Array of strings</param>
		/// <returns>
		/// Empty string if strs.length is 0, comma separated list of strings
		/// otherwise
		/// </returns>
		public static string arrayToString(string[] strs)
		{
			if (strs.Length == 0)
			{
				return string.Empty;
			}
			java.lang.StringBuilder sbuf = new java.lang.StringBuilder();
			sbuf.Append(strs[0]);
			for (int idx = 1; idx < strs.Length; idx++)
			{
				sbuf.Append(",");
				sbuf.Append(strs[idx]);
			}
			return sbuf.ToString();
		}

		/// <summary>
		/// Given an array of bytes it will convert the bytes to a hex string
		/// representation of the bytes
		/// </summary>
		/// <param name="bytes"/>
		/// <param name="start">start index, inclusively</param>
		/// <param name="end">end index, exclusively</param>
		/// <returns>hex string representation of the byte array</returns>
		public static string byteToHexString(byte[] bytes, int start, int end)
		{
			if (bytes == null)
			{
				throw new System.ArgumentException("bytes == null");
			}
			java.lang.StringBuilder s = new java.lang.StringBuilder();
			for (int i = start; i < end; i++)
			{
				s.Append(format("%02x", bytes[i]));
			}
			return s.ToString();
		}

		/// <summary>Same as byteToHexString(bytes, 0, bytes.length).</summary>
		public static string byteToHexString(byte[] bytes)
		{
			return byteToHexString(bytes, 0, bytes.Length);
		}

		/// <summary>
		/// Given a hexstring this will return the byte array corresponding to the
		/// string
		/// </summary>
		/// <param name="hex">the hex String array</param>
		/// <returns>
		/// a byte array that is a hex string representation of the given
		/// string. The size of the byte array is therefore hex.length/2
		/// </returns>
		public static byte[] hexStringToByte(string hex)
		{
			byte[] bts = new byte[hex.Length / 2];
			for (int i = 0; i < bts.Length; i++)
			{
				bts[i] = unchecked((byte)System.Convert.ToInt32(Sharpen.Runtime.substring(hex, 2 
					* i, 2 * i + 2), 16));
			}
			return bts;
		}

		/// <param name="uris"/>
		public static string uriToString(java.net.URI[] uris)
		{
			if (uris == null)
			{
				return null;
			}
			java.lang.StringBuilder ret = new java.lang.StringBuilder(uris[0].ToString());
			for (int i = 1; i < uris.Length; i++)
			{
				ret.Append(",");
				ret.Append(uris[i].ToString());
			}
			return ret.ToString();
		}

		/// <param name="str">The string array to be parsed into an URI array.</param>
		/// <returns>
		/// <tt>null</tt> if str is <tt>null</tt>, else the URI array
		/// equivalent to str.
		/// </returns>
		/// <exception cref="System.ArgumentException">If any string in str violates RFC&nbsp;2396.
		/// 	</exception>
		public static java.net.URI[] stringToURI(string[] str)
		{
			if (str == null)
			{
				return null;
			}
			java.net.URI[] uris = new java.net.URI[str.Length];
			for (int i = 0; i < str.Length; i++)
			{
				try
				{
					uris[i] = new java.net.URI(str[i]);
				}
				catch (java.net.URISyntaxException ur)
				{
					throw new System.ArgumentException("Failed to create uri for " + str[i], ur);
				}
			}
			return uris;
		}

		/// <param name="str"/>
		public static org.apache.hadoop.fs.Path[] stringToPath(string[] str)
		{
			if (str == null)
			{
				return null;
			}
			org.apache.hadoop.fs.Path[] p = new org.apache.hadoop.fs.Path[str.Length];
			for (int i = 0; i < str.Length; i++)
			{
				p[i] = new org.apache.hadoop.fs.Path(str[i]);
			}
			return p;
		}

		/// <summary>
		/// Given a finish and start time in long milliseconds, returns a
		/// String in the format Xhrs, Ymins, Z sec, for the time difference between two times.
		/// </summary>
		/// <remarks>
		/// Given a finish and start time in long milliseconds, returns a
		/// String in the format Xhrs, Ymins, Z sec, for the time difference between two times.
		/// If finish time comes before start time then negative valeus of X, Y and Z wil return.
		/// </remarks>
		/// <param name="finishTime">finish time</param>
		/// <param name="startTime">start time</param>
		public static string formatTimeDiff(long finishTime, long startTime)
		{
			long timeDiff = finishTime - startTime;
			return formatTime(timeDiff);
		}

		/// <summary>
		/// Given the time in long milliseconds, returns a
		/// String in the format Xhrs, Ymins, Z sec.
		/// </summary>
		/// <param name="timeDiff">The time difference to format</param>
		public static string formatTime(long timeDiff)
		{
			java.lang.StringBuilder buf = new java.lang.StringBuilder();
			long hours = timeDiff / (60 * 60 * 1000);
			long rem = (timeDiff % (60 * 60 * 1000));
			long minutes = rem / (60 * 1000);
			rem = rem % (60 * 1000);
			long seconds = rem / 1000;
			if (hours != 0)
			{
				buf.Append(hours);
				buf.Append("hrs, ");
			}
			if (minutes != 0)
			{
				buf.Append(minutes);
				buf.Append("mins, ");
			}
			// return "0sec if no difference
			buf.Append(seconds);
			buf.Append("sec");
			return buf.ToString();
		}

		/// <summary>
		/// Formats time in ms and appends difference (finishTime - startTime)
		/// as returned by formatTimeDiff().
		/// </summary>
		/// <remarks>
		/// Formats time in ms and appends difference (finishTime - startTime)
		/// as returned by formatTimeDiff().
		/// If finish time is 0, empty string is returned, if start time is 0
		/// then difference is not appended to return value.
		/// </remarks>
		/// <param name="dateFormat">date format to use</param>
		/// <param name="finishTime">fnish time</param>
		/// <param name="startTime">start time</param>
		/// <returns>formatted value.</returns>
		public static string getFormattedTimeWithDiff(java.text.DateFormat dateFormat, long
			 finishTime, long startTime)
		{
			java.lang.StringBuilder buf = new java.lang.StringBuilder();
			if (0 != finishTime)
			{
				buf.Append(dateFormat.format(new System.DateTime(finishTime)));
				if (0 != startTime)
				{
					buf.Append(" (" + formatTimeDiff(finishTime, startTime) + ")");
				}
			}
			return buf.ToString();
		}

		/// <summary>Returns an arraylist of strings.</summary>
		/// <param name="str">the comma seperated string values</param>
		/// <returns>the arraylist of the comma seperated string values</returns>
		public static string[] getStrings(string str)
		{
			System.Collections.Generic.ICollection<string> values = getStringCollection(str);
			if (values.Count == 0)
			{
				return null;
			}
			return Sharpen.Collections.ToArray(values, new string[values.Count]);
		}

		/// <summary>Returns a collection of strings.</summary>
		/// <param name="str">comma seperated string values</param>
		/// <returns>an <code>ArrayList</code> of string values</returns>
		public static System.Collections.Generic.ICollection<string> getStringCollection(
			string str)
		{
			string delim = ",";
			return getStringCollection(str, delim);
		}

		/// <summary>Returns a collection of strings.</summary>
		/// <param name="str">String to parse</param>
		/// <param name="delim">delimiter to separate the values</param>
		/// <returns>Collection of parsed elements.</returns>
		public static System.Collections.Generic.ICollection<string> getStringCollection(
			string str, string delim)
		{
			System.Collections.Generic.IList<string> values = new System.Collections.Generic.List
				<string>();
			if (str == null)
			{
				return values;
			}
			java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(str, delim);
			while (tokenizer.hasMoreTokens())
			{
				values.add(tokenizer.nextToken());
			}
			return values;
		}

		/// <summary>Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
		/// 	</summary>
		/// <remarks>
		/// Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
		/// Duplicate and empty values are removed.
		/// </remarks>
		/// <param name="str">a comma separated <String> with values</param>
		/// <returns>a <code>Collection</code> of <code>String</code> values</returns>
		public static System.Collections.Generic.ICollection<string> getTrimmedStringCollection
			(string str)
		{
			System.Collections.Generic.ICollection<string> set = new java.util.LinkedHashSet<
				string>(java.util.Arrays.asList(getTrimmedStrings(str)));
			set.remove(string.Empty);
			return set;
		}

		/// <summary>Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
		/// 	</summary>
		/// <param name="str">a comma separated <String> with values</param>
		/// <returns>an array of <code>String</code> values</returns>
		public static string[] getTrimmedStrings(string str)
		{
			if (null == str || str.Trim().isEmpty())
			{
				return emptyStringArray;
			}
			return str.Trim().split("\\s*,\\s*");
		}

		/// <summary>Trims all the strings in a Collection<String> and returns a Set<String>.
		/// 	</summary>
		/// <param name="strings"/>
		/// <returns/>
		public static System.Collections.Generic.ICollection<string> getTrimmedStrings(System.Collections.Generic.ICollection
			<string> strings)
		{
			System.Collections.Generic.ICollection<string> trimmedStrings = new java.util.HashSet
				<string>();
			foreach (string @string in strings)
			{
				trimmedStrings.add(@string.Trim());
			}
			return trimmedStrings;
		}

		public static readonly string[] emptyStringArray = new string[] {  };

		public const char COMMA = ',';

		public const string COMMA_STR = ",";

		public const char ESCAPE_CHAR = '\\';

		/// <summary>Split a string using the default separator</summary>
		/// <param name="str">a string that may have escaped separator</param>
		/// <returns>an array of strings</returns>
		public static string[] split(string str)
		{
			return split(str, ESCAPE_CHAR, COMMA);
		}

		/// <summary>Split a string using the given separator</summary>
		/// <param name="str">a string that may have escaped separator</param>
		/// <param name="escapeChar">a char that be used to escape the separator</param>
		/// <param name="separator">a separator char</param>
		/// <returns>an array of strings</returns>
		public static string[] split(string str, char escapeChar, char separator)
		{
			if (str == null)
			{
				return null;
			}
			System.Collections.Generic.List<string> strList = new System.Collections.Generic.List
				<string>();
			java.lang.StringBuilder split = new java.lang.StringBuilder();
			int index = 0;
			while ((index = findNext(str, separator, escapeChar, index, split)) >= 0)
			{
				++index;
				// move over the separator for next search
				strList.add(split.ToString());
				split.Length = 0;
			}
			// reset the buffer 
			strList.add(split.ToString());
			// remove trailing empty split(s)
			int last = strList.Count;
			// last split
			while (--last >= 0 && string.Empty.Equals(strList[last]))
			{
				strList.remove(last);
			}
			return Sharpen.Collections.ToArray(strList, new string[strList.Count]);
		}

		/// <summary>Split a string using the given separator, with no escaping performed.</summary>
		/// <param name="str">a string to be split. Note that this may not be null.</param>
		/// <param name="separator">a separator char</param>
		/// <returns>an array of strings</returns>
		public static string[] split(string str, char separator)
		{
			// String.split returns a single empty result for splitting the empty
			// string.
			if (str.isEmpty())
			{
				return new string[] { string.Empty };
			}
			System.Collections.Generic.List<string> strList = new System.Collections.Generic.List
				<string>();
			int startIndex = 0;
			int nextIndex = 0;
			while ((nextIndex = str.IndexOf(separator, startIndex)) != -1)
			{
				strList.add(Sharpen.Runtime.substring(str, startIndex, nextIndex));
				startIndex = nextIndex + 1;
			}
			strList.add(Sharpen.Runtime.substring(str, startIndex));
			// remove trailing empty split(s)
			int last = strList.Count;
			// last split
			while (--last >= 0 && string.Empty.Equals(strList[last]))
			{
				strList.remove(last);
			}
			return Sharpen.Collections.ToArray(strList, new string[strList.Count]);
		}

		/// <summary>
		/// Finds the first occurrence of the separator character ignoring the escaped
		/// separators starting from the index.
		/// </summary>
		/// <remarks>
		/// Finds the first occurrence of the separator character ignoring the escaped
		/// separators starting from the index. Note the substring between the index
		/// and the position of the separator is passed.
		/// </remarks>
		/// <param name="str">the source string</param>
		/// <param name="separator">the character to find</param>
		/// <param name="escapeChar">character used to escape</param>
		/// <param name="start">from where to search</param>
		/// <param name="split">used to pass back the extracted string</param>
		public static int findNext(string str, char separator, char escapeChar, int start
			, java.lang.StringBuilder split)
		{
			int numPreEscapes = 0;
			for (int i = start; i < str.Length; i++)
			{
				char curChar = str[i];
				if (numPreEscapes == 0 && curChar == separator)
				{
					// separator 
					return i;
				}
				else
				{
					split.Append(curChar);
					numPreEscapes = (curChar == escapeChar) ? (++numPreEscapes) % 2 : 0;
				}
			}
			return -1;
		}

		/// <summary>Escape commas in the string using the default escape char</summary>
		/// <param name="str">a string</param>
		/// <returns>an escaped string</returns>
		public static string escapeString(string str)
		{
			return escapeString(str, ESCAPE_CHAR, COMMA);
		}

		/// <summary>
		/// Escape <code>charToEscape</code> in the string
		/// with the escape char <code>escapeChar</code>
		/// </summary>
		/// <param name="str">string</param>
		/// <param name="escapeChar">escape char</param>
		/// <param name="charToEscape">the char to be escaped</param>
		/// <returns>an escaped string</returns>
		public static string escapeString(string str, char escapeChar, char charToEscape)
		{
			return escapeString(str, escapeChar, new char[] { charToEscape });
		}

		// check if the character array has the character 
		private static bool hasChar(char[] chars, char character)
		{
			foreach (char target in chars)
			{
				if (character == target)
				{
					return true;
				}
			}
			return false;
		}

		/// <param name="charsToEscape">array of characters to be escaped</param>
		public static string escapeString(string str, char escapeChar, char[] charsToEscape
			)
		{
			if (str == null)
			{
				return null;
			}
			java.lang.StringBuilder result = new java.lang.StringBuilder();
			for (int i = 0; i < str.Length; i++)
			{
				char curChar = str[i];
				if (curChar == escapeChar || hasChar(charsToEscape, curChar))
				{
					// special char
					result.Append(escapeChar);
				}
				result.Append(curChar);
			}
			return result.ToString();
		}

		/// <summary>Unescape commas in the string using the default escape char</summary>
		/// <param name="str">a string</param>
		/// <returns>an unescaped string</returns>
		public static string unEscapeString(string str)
		{
			return unEscapeString(str, ESCAPE_CHAR, COMMA);
		}

		/// <summary>
		/// Unescape <code>charToEscape</code> in the string
		/// with the escape char <code>escapeChar</code>
		/// </summary>
		/// <param name="str">string</param>
		/// <param name="escapeChar">escape char</param>
		/// <param name="charToEscape">the escaped char</param>
		/// <returns>an unescaped string</returns>
		public static string unEscapeString(string str, char escapeChar, char charToEscape
			)
		{
			return unEscapeString(str, escapeChar, new char[] { charToEscape });
		}

		/// <param name="charsToEscape">array of characters to unescape</param>
		public static string unEscapeString(string str, char escapeChar, char[] charsToEscape
			)
		{
			if (str == null)
			{
				return null;
			}
			java.lang.StringBuilder result = new java.lang.StringBuilder(str.Length);
			bool hasPreEscape = false;
			for (int i = 0; i < str.Length; i++)
			{
				char curChar = str[i];
				if (hasPreEscape)
				{
					if (curChar != escapeChar && !hasChar(charsToEscape, curChar))
					{
						// no special char
						throw new System.ArgumentException("Illegal escaped string " + str + " unescaped "
							 + escapeChar + " at " + (i - 1));
					}
					// otherwise discard the escape char
					result.Append(curChar);
					hasPreEscape = false;
				}
				else
				{
					if (hasChar(charsToEscape, curChar))
					{
						throw new System.ArgumentException("Illegal escaped string " + str + " unescaped "
							 + curChar + " at " + i);
					}
					else
					{
						if (curChar == escapeChar)
						{
							hasPreEscape = true;
						}
						else
						{
							result.Append(curChar);
						}
					}
				}
			}
			if (hasPreEscape)
			{
				throw new System.ArgumentException("Illegal escaped string " + str + ", not expecting "
					 + escapeChar + " in the end.");
			}
			return result.ToString();
		}

		/// <summary>Return a message for logging.</summary>
		/// <param name="prefix">prefix keyword for the message</param>
		/// <param name="msg">content of the message</param>
		/// <returns>a message for logging</returns>
		private static string toStartupShutdownString(string prefix, string[] msg)
		{
			java.lang.StringBuilder b = new java.lang.StringBuilder(prefix);
			b.Append("\n/************************************************************");
			foreach (string s in msg)
			{
				b.Append("\n" + prefix + s);
			}
			b.Append("\n************************************************************/");
			return b.ToString();
		}

		/// <summary>Print a log message for starting up and shutting down</summary>
		/// <param name="clazz">the class of the server</param>
		/// <param name="args">arguments</param>
		/// <param name="LOG">the target log object</param>
		public static void startupShutdownMessage(java.lang.Class clazz, string[] args, org.apache.commons.logging.Log
			 LOG)
		{
			startupShutdownMessage(clazz, args, org.apache.hadoop.util.LogAdapter.create(LOG)
				);
		}

		/// <summary>Print a log message for starting up and shutting down</summary>
		/// <param name="clazz">the class of the server</param>
		/// <param name="args">arguments</param>
		/// <param name="LOG">the target log object</param>
		public static void startupShutdownMessage(java.lang.Class clazz, string[] args, org.slf4j.Logger
			 LOG)
		{
			startupShutdownMessage(clazz, args, org.apache.hadoop.util.LogAdapter.create(LOG)
				);
		}

		internal static void startupShutdownMessage(java.lang.Class clazz, string[] args, 
			org.apache.hadoop.util.LogAdapter LOG)
		{
			string hostname = org.apache.hadoop.net.NetUtils.getHostname();
			string classname = clazz.getSimpleName();
			LOG.info(toStartupShutdownString("STARTUP_MSG: ", new string[] { "Starting " + classname
				, "  host = " + hostname, "  args = " + java.util.Arrays.asList(args), "  version = "
				 + org.apache.hadoop.util.VersionInfo.getVersion(), "  classpath = " + Sharpen.Runtime
				.getProperty("java.class.path"), "  build = " + org.apache.hadoop.util.VersionInfo
				.getUrl() + " -r " + org.apache.hadoop.util.VersionInfo.getRevision() + "; compiled by '"
				 + org.apache.hadoop.util.VersionInfo.getUser() + "' on " + org.apache.hadoop.util.VersionInfo
				.getDate(), "  java = " + Sharpen.Runtime.getProperty("java.version") }));
			if (org.apache.commons.lang.SystemUtils.IS_OS_UNIX)
			{
				try
				{
					org.apache.hadoop.util.SignalLogger.INSTANCE.register(LOG);
				}
				catch (System.Exception t)
				{
					LOG.warn("failed to register any UNIX signal loggers: ", t);
				}
			}
			org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(new _Runnable_673
				(LOG, classname, hostname), SHUTDOWN_HOOK_PRIORITY);
		}

		private sealed class _Runnable_673 : java.lang.Runnable
		{
			public _Runnable_673(org.apache.hadoop.util.LogAdapter LOG, string classname, string
				 hostname)
			{
				this.LOG = LOG;
				this.classname = classname;
				this.hostname = hostname;
			}

			public void run()
			{
				LOG.info(org.apache.hadoop.util.StringUtils.toStartupShutdownString("SHUTDOWN_MSG: "
					, new string[] { "Shutting down " + classname + " at " + hostname }));
			}

			private readonly org.apache.hadoop.util.LogAdapter LOG;

			private readonly string classname;

			private readonly string hostname;
		}

		/// <summary>
		/// The traditional binary prefixes, kilo, mega, ..., exa,
		/// which can be represented by a 64-bit integer.
		/// </summary>
		/// <remarks>
		/// The traditional binary prefixes, kilo, mega, ..., exa,
		/// which can be represented by a 64-bit integer.
		/// TraditionalBinaryPrefix symbol are case insensitive.
		/// </remarks>
		[System.Serializable]
		public sealed class TraditionalBinaryPrefix
		{
			public static readonly org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				 KILO = new org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix(10);

			public static readonly org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				 MEGA = new org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix(org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.KILO.bitShift + 10);

			public static readonly org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				 GIGA = new org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix(org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.MEGA.bitShift + 10);

			public static readonly org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				 TERA = new org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix(org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.GIGA.bitShift + 10);

			public static readonly org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				 PETA = new org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix(org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.TERA.bitShift + 10);

			public static readonly org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				 EXA = new org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix(org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.PETA.bitShift + 10);

			public readonly long value;

			public readonly char symbol;

			public readonly int bitShift;

			public readonly long bitMask;

			private TraditionalBinaryPrefix(int bitShift)
			{
				this.bitShift = bitShift;
				this.value = 1L << bitShift;
				this.bitMask = this.value - 1L;
				this.symbol = ToString()[0];
			}

			/// <returns>The TraditionalBinaryPrefix object corresponding to the symbol.</returns>
			public static org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix valueOf(
				char symbol)
			{
				symbol = char.toUpperCase(symbol);
				foreach (org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix prefix in org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
					.values())
				{
					if (symbol == prefix.symbol)
					{
						return prefix;
					}
				}
				throw new System.ArgumentException("Unknown symbol '" + symbol + "'");
			}

			/// <summary>Convert a string to long.</summary>
			/// <remarks>
			/// Convert a string to long.
			/// The input string is first be trimmed
			/// and then it is parsed with traditional binary prefix.
			/// For example,
			/// "-1230k" will be converted to -1230 * 1024 = -1259520;
			/// "891g" will be converted to 891 * 1024^3 = 956703965184;
			/// </remarks>
			/// <param name="s">input string</param>
			/// <returns>a long value represented by the input string.</returns>
			public static long string2long(string s)
			{
				s = s.Trim();
				int lastpos = s.Length - 1;
				char lastchar = s[lastpos];
				if (char.isDigit(lastchar))
				{
					return long.Parse(s);
				}
				else
				{
					long prefix;
					try
					{
						prefix = org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.valueOf(lastchar
							).value;
					}
					catch (System.ArgumentException)
					{
						throw new System.ArgumentException("Invalid size prefix '" + lastchar + "' in '" 
							+ s + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)");
					}
					long num = long.Parse(Sharpen.Runtime.substring(s, 0, lastpos));
					if (num > (long.MaxValue / prefix) || num < (long.MinValue / prefix))
					{
						throw new System.ArgumentException(s + " does not fit in a Long");
					}
					return num * prefix;
				}
			}

			/// <summary>Convert a long integer to a string with traditional binary prefix.</summary>
			/// <param name="n">the value to be converted</param>
			/// <param name="unit">The unit, e.g. "B" for bytes.</param>
			/// <param name="decimalPlaces">The number of decimal places.</param>
			/// <returns>a string with traditional binary prefix.</returns>
			public static string long2String(long n, string unit, int decimalPlaces)
			{
				if (unit == null)
				{
					unit = string.Empty;
				}
				//take care a special case
				if (n == long.MinValue)
				{
					return "-8 " + org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.EXA.symbol
						 + unit;
				}
				java.lang.StringBuilder b = new java.lang.StringBuilder();
				//take care negative numbers
				if (n < 0)
				{
					b.Append('-');
					n = -n;
				}
				if (n < org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.KILO.value)
				{
					//no prefix
					b.Append(n);
					return (unit.isEmpty() ? b : b.Append(" ").Append(unit)).ToString();
				}
				else
				{
					//find traditional binary prefix
					int i = 0;
					for (; i < values().Length && n >= values()[i].value; i++)
					{
					}
					org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix prefix = values()[i - 
						1];
					if ((n & prefix.bitMask) == 0)
					{
						//exact division
						b.Append(n >> prefix.bitShift);
					}
					else
					{
						string format = "%." + decimalPlaces + "f";
						string s = format(format, n / (double)prefix.value);
						//check a special rounding up case
						if (s.StartsWith("1024"))
						{
							prefix = values()[i];
							s = format(format, n / (double)prefix.value);
						}
						b.Append(s);
					}
					return b.Append(' ').Append(prefix.symbol).Append(unit).ToString();
				}
			}
		}

		/// <summary>Escapes HTML Special characters present in the string.</summary>
		/// <param name="string"/>
		/// <returns>HTML Escaped String representation</returns>
		public static string escapeHTML(string @string)
		{
			if (@string == null)
			{
				return null;
			}
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			bool lastCharacterWasSpace = false;
			char[] chars = @string.ToCharArray();
			foreach (char c in chars)
			{
				if (c == ' ')
				{
					if (lastCharacterWasSpace)
					{
						lastCharacterWasSpace = false;
						sb.Append("&nbsp;");
					}
					else
					{
						lastCharacterWasSpace = true;
						sb.Append(" ");
					}
				}
				else
				{
					lastCharacterWasSpace = false;
					switch (c)
					{
						case '<':
						{
							sb.Append("&lt;");
							break;
						}

						case '>':
						{
							sb.Append("&gt;");
							break;
						}

						case '&':
						{
							sb.Append("&amp;");
							break;
						}

						case '"':
						{
							sb.Append("&quot;");
							break;
						}

						default:
						{
							sb.Append(c);
							break;
						}
					}
				}
			}
			return sb.ToString();
		}

		/// <returns>a byte description of the given long interger value.</returns>
		public static string byteDesc(long len)
		{
			return org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.long2String(len
				, "B", 2);
		}

		[System.ObsoleteAttribute(@"use StringUtils.format(""%.2f"", d).")]
		public static string limitDecimalTo2(double d)
		{
			return format("%.2f", d);
		}

		/// <summary>Concatenates strings, using a separator.</summary>
		/// <param name="separator">Separator to join with.</param>
		/// <param name="strings">Strings to join.</param>
		public static string join<_T0>(java.lang.CharSequence separator, System.Collections.Generic.IEnumerable
			<_T0> strings)
		{
			System.Collections.Generic.IEnumerator<object> i = strings.GetEnumerator();
			if (!i.MoveNext())
			{
				return string.Empty;
			}
			java.lang.StringBuilder sb = new java.lang.StringBuilder(i.Current.ToString());
			while (i.MoveNext())
			{
				sb.Append(separator);
				sb.Append(i.Current.ToString());
			}
			return sb.ToString();
		}

		/// <summary>Concatenates strings, using a separator.</summary>
		/// <param name="separator">to join with</param>
		/// <param name="strings">to join</param>
		/// <returns>the joined string</returns>
		public static string join(java.lang.CharSequence separator, string[] strings)
		{
			// Ideally we don't have to duplicate the code here if array is iterable.
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			bool first = true;
			foreach (string s in strings)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					sb.Append(separator);
				}
				sb.Append(s);
			}
			return sb.ToString();
		}

		/// <summary>Convert SOME_STUFF to SomeStuff</summary>
		/// <param name="s">input string</param>
		/// <returns>camelized string</returns>
		public static string camelize(string s)
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			string[] words = split(org.apache.hadoop.util.StringUtils.toLowerCase(s), ESCAPE_CHAR
				, '_');
			foreach (string word in words)
			{
				sb.Append(org.apache.commons.lang.StringUtils.capitalize(word));
			}
			return sb.ToString();
		}

		/// <summary>
		/// Matches a template string against a pattern, replaces matched tokens with
		/// the supplied replacements, and returns the result.
		/// </summary>
		/// <remarks>
		/// Matches a template string against a pattern, replaces matched tokens with
		/// the supplied replacements, and returns the result.  The regular expression
		/// must use a capturing group.  The value of the first capturing group is used
		/// to look up the replacement.  If no replacement is found for the token, then
		/// it is replaced with the empty string.
		/// For example, assume template is "%foo%_%bar%_%baz%", pattern is "%(.*?)%",
		/// and replacements contains 2 entries, mapping "foo" to "zoo" and "baz" to
		/// "zaz".  The result returned would be "zoo__zaz".
		/// </remarks>
		/// <param name="template">String template to receive replacements</param>
		/// <param name="pattern">
		/// Pattern to match for identifying tokens, must use a capturing
		/// group
		/// </param>
		/// <param name="replacements">
		/// Map<String, String> mapping tokens identified by the
		/// capturing group to their replacement values
		/// </param>
		/// <returns>String template with replacements</returns>
		public static string replaceTokens(string template, java.util.regex.Pattern pattern
			, System.Collections.Generic.IDictionary<string, string> replacements)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			java.util.regex.Matcher matcher = pattern.matcher(template);
			while (matcher.find())
			{
				string replacement = replacements[matcher.group(1)];
				if (replacement == null)
				{
					replacement = string.Empty;
				}
				matcher.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(replacement
					));
			}
			matcher.appendTail(sb);
			return sb.ToString();
		}

		/// <summary>Get stack trace for a given thread.</summary>
		public static string getStackTrace(java.lang.Thread t)
		{
			java.lang.StackTraceElement[] stackTrace = t.getStackTrace();
			java.lang.StringBuilder str = new java.lang.StringBuilder();
			foreach (java.lang.StackTraceElement e in stackTrace)
			{
				str.Append(e.ToString() + "\n");
			}
			return str.ToString();
		}

		/// <summary>
		/// From a list of command-line arguments, remove both an option and the
		/// next argument.
		/// </summary>
		/// <param name="name">Name of the option to remove.  Example: -foo.</param>
		/// <param name="args">List of arguments.</param>
		/// <returns>
		/// null if the option was not found; the value of the
		/// option otherwise.
		/// </returns>
		/// <exception cref="System.ArgumentException">if the option's argument is not present
		/// 	</exception>
		public static string popOptionWithArgument(string name, System.Collections.Generic.IList
			<string> args)
		{
			string val = null;
			for (System.Collections.Generic.IEnumerator<string> iter = args.GetEnumerator(); 
				iter.MoveNext(); )
			{
				string cur = iter.Current;
				if (cur.Equals("--"))
				{
					// stop parsing arguments when you see --
					break;
				}
				else
				{
					if (cur.Equals(name))
					{
						iter.remove();
						if (!iter.MoveNext())
						{
							throw new System.ArgumentException("option " + name + " requires 1 " + "argument."
								);
						}
						val = iter.Current;
						iter.remove();
						break;
					}
				}
			}
			return val;
		}

		/// <summary>From a list of command-line arguments, remove an option.</summary>
		/// <param name="name">Name of the option to remove.  Example: -foo.</param>
		/// <param name="args">List of arguments.</param>
		/// <returns>true if the option was found and removed; false otherwise.</returns>
		public static bool popOption(string name, System.Collections.Generic.IList<string
			> args)
		{
			for (System.Collections.Generic.IEnumerator<string> iter = args.GetEnumerator(); 
				iter.MoveNext(); )
			{
				string cur = iter.Current;
				if (cur.Equals("--"))
				{
					// stop parsing arguments when you see --
					break;
				}
				else
				{
					if (cur.Equals(name))
					{
						iter.remove();
						return true;
					}
				}
			}
			return false;
		}

		/// <summary>
		/// From a list of command-line arguments, return the first non-option
		/// argument.
		/// </summary>
		/// <remarks>
		/// From a list of command-line arguments, return the first non-option
		/// argument.  Non-option arguments are those which either come after
		/// a double dash (--) or do not start with a dash.
		/// </remarks>
		/// <param name="args">List of arguments.</param>
		/// <returns>The first non-option argument, or null if there were none.</returns>
		public static string popFirstNonOption(System.Collections.Generic.IList<string> args
			)
		{
			for (System.Collections.Generic.IEnumerator<string> iter = args.GetEnumerator(); 
				iter.MoveNext(); )
			{
				string cur = iter.Current;
				if (cur.Equals("--"))
				{
					if (!iter.MoveNext())
					{
						return null;
					}
					cur = iter.Current;
					iter.remove();
					return cur;
				}
				else
				{
					if (!cur.StartsWith("-"))
					{
						iter.remove();
						return cur;
					}
				}
			}
			return null;
		}

		/// <summary>
		/// Converts all of the characters in this String to lower case with
		/// Locale.ENGLISH.
		/// </summary>
		/// <param name="str">string to be converted</param>
		/// <returns>the str, converted to lowercase.</returns>
		public static string toLowerCase(string str)
		{
			return str.ToLower(java.util.Locale.ENGLISH);
		}

		/// <summary>
		/// Converts all of the characters in this String to upper case with
		/// Locale.ENGLISH.
		/// </summary>
		/// <param name="str">string to be converted</param>
		/// <returns>the str, converted to uppercase.</returns>
		public static string toUpperCase(string str)
		{
			return str.ToUpper(java.util.Locale.ENGLISH);
		}

		/// <summary>Compare strings locale-freely by using String#equalsIgnoreCase.</summary>
		/// <param name="s1">Non-null string to be converted</param>
		/// <param name="s2">string to be converted</param>
		/// <returns>the str, converted to uppercase.</returns>
		public static bool equalsIgnoreCase(string s1, string s2)
		{
			com.google.common.@base.Preconditions.checkNotNull(s1);
			// don't check non-null against s2 to make the semantics same as
			// s1.equals(s2)
			return Sharpen.Runtime.equalsIgnoreCase(s1, s2);
		}
	}
}
