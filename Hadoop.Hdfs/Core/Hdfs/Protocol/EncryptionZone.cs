using Org.Apache.Commons.Lang.Builder;
using Org.Apache.Hadoop.Crypto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>A simple class for representing an encryption zone.</summary>
	/// <remarks>
	/// A simple class for representing an encryption zone. Presently an encryption
	/// zone only has a path (the root of the encryption zone), a key name, and a
	/// unique id. The id is used to implement batched listing of encryption zones.
	/// </remarks>
	public class EncryptionZone
	{
		private readonly long id;

		private readonly string path;

		private readonly CipherSuite suite;

		private readonly CryptoProtocolVersion version;

		private readonly string keyName;

		public EncryptionZone(long id, string path, CipherSuite suite, CryptoProtocolVersion
			 version, string keyName)
		{
			this.id = id;
			this.path = path;
			this.suite = suite;
			this.version = version;
			this.keyName = keyName;
		}

		public virtual long GetId()
		{
			return id;
		}

		public virtual string GetPath()
		{
			return path;
		}

		public virtual CipherSuite GetSuite()
		{
			return suite;
		}

		public virtual CryptoProtocolVersion GetVersion()
		{
			return version;
		}

		public virtual string GetKeyName()
		{
			return keyName;
		}

		public override int GetHashCode()
		{
			return new HashCodeBuilder(13, 31).Append(id).Append(path).Append(suite).Append(version
				).Append(keyName).ToHashCode();
		}

		public override bool Equals(object obj)
		{
			if (obj == null)
			{
				return false;
			}
			if (obj == this)
			{
				return true;
			}
			if (obj.GetType() != GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Protocol.EncryptionZone rhs = (Org.Apache.Hadoop.Hdfs.Protocol.EncryptionZone
				)obj;
			return new EqualsBuilder().Append(id, rhs.id).Append(path, rhs.path).Append(suite
				, rhs.suite).Append(version, rhs.version).Append(keyName, rhs.keyName).IsEquals(
				);
		}

		public override string ToString()
		{
			return "EncryptionZone [id=" + id + ", path=" + path + ", suite=" + suite + ", version="
				 + version + ", keyName=" + keyName + "]";
		}
	}
}
