using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records
{
	/// <summary>
	/// Contains all the state data that needs to be stored persistently
	/// for
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.AMRMTokenSecretManager
	/// 	"/>
	/// </summary>
	public abstract class AMRMTokenSecretManagerState
	{
		public static AMRMTokenSecretManagerState NewInstance(MasterKey currentMasterKey, 
			MasterKey nextMasterKey)
		{
			AMRMTokenSecretManagerState data = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				AMRMTokenSecretManagerState>();
			data.SetCurrentMasterKey(currentMasterKey);
			data.SetNextMasterKey(nextMasterKey);
			return data;
		}

		public static AMRMTokenSecretManagerState NewInstance(AMRMTokenSecretManagerState
			 state)
		{
			AMRMTokenSecretManagerState data = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				AMRMTokenSecretManagerState>();
			data.SetCurrentMasterKey(state.GetCurrentMasterKey());
			data.SetNextMasterKey(state.GetNextMasterKey());
			return data;
		}

		/// <summary>
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.AMRMTokenSecretManager
		/// 	"/>
		/// current Master key
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract MasterKey GetCurrentMasterKey();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetCurrentMasterKey(MasterKey currentMasterKey);

		/// <summary>
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.AMRMTokenSecretManager
		/// 	"/>
		/// next Master key
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract MasterKey GetNextMasterKey();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetNextMasterKey(MasterKey nextMasterKey);

		public abstract YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto
			 GetProto();
	}
}
