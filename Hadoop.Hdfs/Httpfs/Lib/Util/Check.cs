using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Util
{
	/// <summary>Utility methods to check preconditions.</summary>
	/// <remarks>
	/// Utility methods to check preconditions.
	/// <p>
	/// Commonly used for method arguments preconditions.
	/// </remarks>
	public class Check
	{
		/// <summary>Verifies a variable is not NULL.</summary>
		/// <param name="obj">the variable to check.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the variable.</returns>
		/// <exception cref="System.ArgumentException">if the variable is NULL.</exception>
		public static T NotNull<T>(T obj, string name)
		{
			if (obj == null)
			{
				throw new ArgumentException(name + " cannot be null");
			}
			return obj;
		}

		/// <summary>Verifies a list does not have any NULL elements.</summary>
		/// <param name="list">the list to check.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the list.</returns>
		/// <exception cref="System.ArgumentException">if the list has NULL elements.</exception>
		public static IList<T> NotNullElements<T>(IList<T> list, string name)
		{
			NotNull(list, name);
			for (int i = 0; i < list.Count; i++)
			{
				NotNull(list[i], MessageFormat.Format("list [{0}] element [{1}]", name, i));
			}
			return list;
		}

		/// <summary>Verifies a string is not NULL and not emtpy</summary>
		/// <param name="str">the variable to check.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the variable.</returns>
		/// <exception cref="System.ArgumentException">if the variable is NULL or empty.</exception>
		public static string NotEmpty(string str, string name)
		{
			if (str == null)
			{
				throw new ArgumentException(name + " cannot be null");
			}
			if (str.Length == 0)
			{
				throw new ArgumentException(name + " cannot be empty");
			}
			return str;
		}

		/// <summary>Verifies a string list is not NULL and not emtpy</summary>
		/// <param name="list">the list to check.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the variable.</returns>
		/// <exception cref="System.ArgumentException">
		/// if the string list has NULL or empty
		/// elements.
		/// </exception>
		public static IList<string> NotEmptyElements(IList<string> list, string name)
		{
			NotNull(list, name);
			for (int i = 0; i < list.Count; i++)
			{
				NotEmpty(list[i], MessageFormat.Format("list [{0}] element [{1}]", name, i));
			}
			return list;
		}

		private const string IdentifierPatternStr = "[a-zA-z_][a-zA-Z0-9_\\-]*";

		private static readonly Sharpen.Pattern IdentifierPattern = Sharpen.Pattern.Compile
			("^" + IdentifierPatternStr + "$");

		/// <summary>
		/// Verifies a value is a valid identifier,
		/// <code>[a-zA-z_][a-zA-Z0-9_\-]*</code>, up to a maximum length.
		/// </summary>
		/// <param name="value">string to check if it is a valid identifier.</param>
		/// <param name="maxLen">maximun length.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the value.</returns>
		/// <exception cref="System.ArgumentException">if the string is not a valid identifier.
		/// 	</exception>
		public static string ValidIdentifier(string value, int maxLen, string name)
		{
			Check.NotEmpty(value, name);
			if (value.Length > maxLen)
			{
				throw new ArgumentException(MessageFormat.Format("[{0}] = [{1}] exceeds max len [{2}]"
					, name, value, maxLen));
			}
			if (!IdentifierPattern.Matcher(value).Find())
			{
				throw new ArgumentException(MessageFormat.Format("[{0}] = [{1}] must be '{2}'", name
					, value, IdentifierPatternStr));
			}
			return value;
		}

		/// <summary>Verifies an integer is greater than zero.</summary>
		/// <param name="value">integer value.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the value.</returns>
		/// <exception cref="System.ArgumentException">if the integer is zero or less.</exception>
		public static int Gt0(int value, string name)
		{
			return (int)Gt0((long)value, name);
		}

		/// <summary>Verifies an long is greater than zero.</summary>
		/// <param name="value">long value.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the value.</returns>
		/// <exception cref="System.ArgumentException">if the long is zero or less.</exception>
		public static long Gt0(long value, string name)
		{
			if (value <= 0)
			{
				throw new ArgumentException(MessageFormat.Format("parameter [{0}] = [{1}] must be greater than zero"
					, name, value));
			}
			return value;
		}

		/// <summary>Verifies an integer is greater or equal to zero.</summary>
		/// <param name="value">integer value.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the value.</returns>
		/// <exception cref="System.ArgumentException">if the integer is greater or equal to zero.
		/// 	</exception>
		public static int Ge0(int value, string name)
		{
			return (int)Ge0((long)value, name);
		}

		/// <summary>Verifies an long is greater or equal to zero.</summary>
		/// <param name="value">integer value.</param>
		/// <param name="name">the name to use in the exception message.</param>
		/// <returns>the value.</returns>
		/// <exception cref="System.ArgumentException">if the long is greater or equal to zero.
		/// 	</exception>
		public static long Ge0(long value, string name)
		{
			if (value < 0)
			{
				throw new ArgumentException(MessageFormat.Format("parameter [{0}] = [{1}] must be greater than or equals zero"
					, name, value));
			}
			return value;
		}
	}
}
