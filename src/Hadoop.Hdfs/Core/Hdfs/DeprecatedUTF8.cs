using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// A simple wrapper around
	/// <see cref="Org.Apache.Hadoop.IO.UTF8"/>
	/// .
	/// This class should be used only when it is absolutely necessary
	/// to use
	/// <see cref="Org.Apache.Hadoop.IO.UTF8"/>
	/// . The only difference is that
	/// using this class does not require "@SuppressWarning" annotation to avoid
	/// javac warning. Instead the deprecation is implied in the class name.
	/// This should be treated as package private class to HDFS.
	/// </summary>
	public class DeprecatedUTF8 : UTF8
	{
		public DeprecatedUTF8()
			: base()
		{
		}

		/// <summary>Construct from a given string.</summary>
		public DeprecatedUTF8(string @string)
			: base(@string)
		{
		}

		/// <summary>Construct from a given string.</summary>
		public DeprecatedUTF8(Org.Apache.Hadoop.Hdfs.DeprecatedUTF8 utf8)
			: base(utf8)
		{
		}

		/* The following two are the mostly commonly used methods.
		* wrapping them so that editors do not complain about the deprecation.
		*/
		/// <exception cref="System.IO.IOException"/>
		public static string ReadString(DataInput @in)
		{
			return UTF8.ReadString(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public static int WriteString(DataOutput @out, string s)
		{
			return UTF8.WriteString(@out, s);
		}
	}
}
