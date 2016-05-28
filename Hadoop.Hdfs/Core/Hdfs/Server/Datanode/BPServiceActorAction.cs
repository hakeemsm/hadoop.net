using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Base class for BPServiceActor class
	/// Issued by BPOfferSerivce class to tell BPServiceActor
	/// to take several actions.
	/// </summary>
	public interface BPServiceActorAction
	{
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.BPServiceActorActionException
		/// 	"/>
		void ReportTo(DatanodeProtocolClientSideTranslatorPB bpNamenode, DatanodeRegistration
			 bpRegistration);
	}
}
