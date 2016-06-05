using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Commons.Codec.Digest;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token
{
	/// <summary>
	/// An identifier that identifies a token, may contain public information
	/// about a token, including its kind (or type).
	/// </summary>
	public abstract class TokenIdentifier : IWritable
	{
		private string trackingId = null;

		/// <summary>Get the token kind</summary>
		/// <returns>the kind of the token</returns>
		public abstract Text GetKind();

		/// <summary>Get the Ugi with the username encoded in the token identifier</summary>
		/// <returns>
		/// the username. null is returned if username in the identifier is
		/// empty or null.
		/// </returns>
		public abstract UserGroupInformation GetUser();

		/// <summary>Get the bytes for the token identifier</summary>
		/// <returns>the bytes of the identifier</returns>
		public virtual byte[] GetBytes()
		{
			DataOutputBuffer buf = new DataOutputBuffer(4096);
			try
			{
				this.Write(buf);
			}
			catch (IOException ie)
			{
				throw new RuntimeException("i/o error in getBytes", ie);
			}
			return Arrays.CopyOf(buf.GetData(), buf.GetLength());
		}

		/// <summary>
		/// Returns a tracking identifier that can be used to associate usages of a
		/// token across multiple client sessions.
		/// </summary>
		/// <remarks>
		/// Returns a tracking identifier that can be used to associate usages of a
		/// token across multiple client sessions.
		/// Currently, this function just returns an MD5 of {
		/// <see cref="GetBytes()"/>
		/// .
		/// </remarks>
		/// <returns>tracking identifier</returns>
		public virtual string GetTrackingId()
		{
			if (trackingId == null)
			{
				trackingId = DigestUtils.Md5Hex(GetBytes());
			}
			return trackingId;
		}

		public abstract void ReadFields(BinaryReader arg1);

		public abstract void Write(BinaryWriter arg1);
	}
}
