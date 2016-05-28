using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Operation</summary>
	[System.Serializable]
	public sealed class OP
	{
		public static readonly OP WriteBlock = new OP(unchecked((byte)80));

		public static readonly OP ReadBlock = new OP(unchecked((byte)81));

		public static readonly OP ReadMetadata = new OP(unchecked((byte)82));

		public static readonly OP ReplaceBlock = new OP(unchecked((byte)83));

		public static readonly OP CopyBlock = new OP(unchecked((byte)84));

		public static readonly OP BlockChecksum = new OP(unchecked((byte)85));

		public static readonly OP TransferBlock = new OP(unchecked((byte)86));

		public static readonly OP RequestShortCircuitFds = new OP(unchecked((byte)87));

		public static readonly OP ReleaseShortCircuitFds = new OP(unchecked((byte)88));

		public static readonly OP RequestShortCircuitShm = new OP(unchecked((byte)89));

		/// <summary>The code for this operation.</summary>
		public readonly byte code;

		private OP(byte code)
		{
			this.code = code;
		}

		private static readonly int FirstCode = Values()[0].code;

		/// <summary>Return the object represented by the code.</summary>
		private static OP ValueOf(byte code)
		{
			int i = (code & unchecked((int)(0xff))) - OP.FirstCode;
			return i < 0 || i >= Values().Length ? null : Values()[i];
		}

		/// <summary>Read from in</summary>
		/// <exception cref="System.IO.IOException"/>
		public static OP Read(DataInput @in)
		{
			return ValueOf(@in.ReadByte());
		}

		/// <summary>Write to out</summary>
		/// <exception cref="System.IO.IOException"/>
		public void Write(DataOutput @out)
		{
			@out.Write(OP.code);
		}
	}
}
