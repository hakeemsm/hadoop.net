using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// Thrown by
	/// <see cref="VersionedWritable#readFields(DataInput)"/>
	/// when the
	/// version of an object being read does not match the current implementation
	/// version as returned by
	/// <see cref="VersionedWritable.getVersion()"/>
	/// .
	/// </summary>
	[System.Serializable]
	public class VersionMismatchException : System.IO.IOException
	{
		private byte expectedVersion;

		private byte foundVersion;

		public VersionMismatchException(byte expectedVersionIn, byte foundVersionIn)
		{
			expectedVersion = expectedVersionIn;
			foundVersion = foundVersionIn;
		}

		/// <summary>Returns a string representation of this object.</summary>
		public override string ToString()
		{
			return "A record version mismatch occurred. Expecting v" + expectedVersion + ", found v"
				 + foundVersion;
		}
	}
}
