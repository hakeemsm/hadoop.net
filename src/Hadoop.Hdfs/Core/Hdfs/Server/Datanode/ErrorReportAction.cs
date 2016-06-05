using System.IO;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// A ErrorReportAction is an instruction issued by BPOfferService to
	/// BPServiceActor about a particular block encapsulated in errorMessage.
	/// </summary>
	public class ErrorReportAction : BPServiceActorAction
	{
		internal readonly int errorCode;

		internal readonly string errorMessage;

		public ErrorReportAction(int errorCode, string errorMessage)
		{
			this.errorCode = errorCode;
			this.errorMessage = errorMessage;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.BPServiceActorActionException
		/// 	"/>
		public virtual void ReportTo(DatanodeProtocolClientSideTranslatorPB bpNamenode, DatanodeRegistration
			 bpRegistration)
		{
			try
			{
				bpNamenode.ErrorReport(bpRegistration, errorCode, errorMessage);
			}
			catch (RemoteException re)
			{
				DataNode.Log.Info("trySendErrorReport encountered RemoteException  " + "errorMessage: "
					 + errorMessage + "  errorCode: " + errorCode, re);
			}
			catch (IOException)
			{
				throw new BPServiceActorActionException("Error reporting " + "an error to namenode: "
					);
			}
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + errorCode;
			result = prime * result + ((errorMessage == null) ? 0 : errorMessage.GetHashCode(
				));
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null || !(obj is Org.Apache.Hadoop.Hdfs.Server.Datanode.ErrorReportAction
				))
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Server.Datanode.ErrorReportAction other = (Org.Apache.Hadoop.Hdfs.Server.Datanode.ErrorReportAction
				)obj;
			if (errorCode != other.errorCode)
			{
				return false;
			}
			if (errorMessage == null)
			{
				if (other.errorMessage != null)
				{
					return false;
				}
			}
			else
			{
				if (!errorMessage.Equals(other.errorMessage))
				{
					return false;
				}
			}
			return true;
		}
	}
}
