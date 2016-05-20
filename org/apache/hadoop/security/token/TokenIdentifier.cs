using Sharpen;

namespace org.apache.hadoop.security.token
{
	/// <summary>
	/// An identifier that identifies a token, may contain public information
	/// about a token, including its kind (or type).
	/// </summary>
	public abstract class TokenIdentifier : org.apache.hadoop.io.Writable
	{
		private string trackingId = null;

		/// <summary>Get the token kind</summary>
		/// <returns>the kind of the token</returns>
		public abstract org.apache.hadoop.io.Text getKind();

		/// <summary>Get the Ugi with the username encoded in the token identifier</summary>
		/// <returns>
		/// the username. null is returned if username in the identifier is
		/// empty or null.
		/// </returns>
		public abstract org.apache.hadoop.security.UserGroupInformation getUser();

		/// <summary>Get the bytes for the token identifier</summary>
		/// <returns>the bytes of the identifier</returns>
		public virtual byte[] getBytes()
		{
			org.apache.hadoop.io.DataOutputBuffer buf = new org.apache.hadoop.io.DataOutputBuffer
				(4096);
			try
			{
				this.write(buf);
			}
			catch (System.IO.IOException ie)
			{
				throw new System.Exception("i/o error in getBytes", ie);
			}
			return java.util.Arrays.copyOf(buf.getData(), buf.getLength());
		}

		/// <summary>
		/// Returns a tracking identifier that can be used to associate usages of a
		/// token across multiple client sessions.
		/// </summary>
		/// <remarks>
		/// Returns a tracking identifier that can be used to associate usages of a
		/// token across multiple client sessions.
		/// Currently, this function just returns an MD5 of {
		/// <see cref="getBytes()"/>
		/// .
		/// </remarks>
		/// <returns>tracking identifier</returns>
		public virtual string getTrackingId()
		{
			if (trackingId == null)
			{
				trackingId = org.apache.commons.codec.digest.DigestUtils.md5Hex(getBytes());
			}
			return trackingId;
		}

		public abstract void readFields(java.io.DataInput arg1);

		public abstract void write(java.io.DataOutput arg1);
	}
}
