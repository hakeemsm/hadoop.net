using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>
	/// Information sent by a subordinate name-node to the active name-node
	/// during the registration process.
	/// </summary>
	public class NamenodeRegistration : StorageInfo, NodeRegistration
	{
		internal readonly string rpcAddress;

		internal readonly string httpAddress;

		internal readonly HdfsServerConstants.NamenodeRole role;

		public NamenodeRegistration(string address, string httpAddress, StorageInfo storageInfo
			, HdfsServerConstants.NamenodeRole role)
			: base(storageInfo)
		{
			// RPC address of the node
			// HTTP address of the node
			// node role
			this.rpcAddress = address;
			this.httpAddress = httpAddress;
			this.role = role;
		}

		public virtual string GetAddress()
		{
			// NodeRegistration
			return rpcAddress;
		}

		public virtual string GetHttpAddress()
		{
			return httpAddress;
		}

		public virtual string GetRegistrationID()
		{
			// NodeRegistration
			return Storage.GetRegistrationID(this);
		}

		public virtual int GetVersion()
		{
			// NodeRegistration
			return base.GetLayoutVersion();
		}

		public override string ToString()
		{
			// NodeRegistration
			return GetType().Name + "(" + rpcAddress + ", role=" + GetRole() + ")";
		}

		/// <summary>Get name-node role.</summary>
		public virtual HdfsServerConstants.NamenodeRole GetRole()
		{
			return role;
		}

		public virtual bool IsRole(HdfsServerConstants.NamenodeRole that)
		{
			return role.Equals(that);
		}
	}
}
