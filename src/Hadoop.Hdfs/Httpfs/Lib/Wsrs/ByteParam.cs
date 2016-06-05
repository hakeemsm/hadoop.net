using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public abstract class ByteParam : Param<byte>
	{
		public ByteParam(string name, byte defaultValue)
			: base(name, defaultValue)
		{
		}

		/// <exception cref="System.Exception"/>
		protected internal override byte Parse(string str)
		{
			return byte.ParseByte(str);
		}

		protected internal override string GetDomain()
		{
			return "a byte";
		}
	}
}
